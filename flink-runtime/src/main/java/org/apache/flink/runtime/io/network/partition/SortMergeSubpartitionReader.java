/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.io.network.partition;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.BufferRecycler;
import org.apache.flink.runtime.io.network.buffer.CustomBuffer;
import org.apache.flink.runtime.io.network.partition.ResultSubpartition.BufferAndBacklog;

import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/** Subpartition data reader for {@link SortMergeResultPartition}. */
class SortMergeSubpartitionReader
        implements ResultSubpartitionView, Comparable<SortMergeSubpartitionReader> {

    private final Object lock = new Object();

    /** A {@link CompletableFuture} to be completed when this subpartition reader is released. */
    private final CompletableFuture<?> releaseFuture = new CompletableFuture<>();

    /** Listener to notify when data is available. */
    private final BufferAvailabilityListener availabilityListener;

    /** Buffers already read which can be consumed by netty thread. */
    @GuardedBy("lock")
    private final Queue<Buffer> buffersRead = new ArrayDeque<>();

    /** File reader used to read buffer from. */
    private final PartitionedFileReader fileReader;

    /** Number of remaining non-event buffers in the buffer queue. */
    @GuardedBy("lock")
    private int dataBufferBacklog;

    /** Whether this reader is released or not. */
    @GuardedBy("lock")
    private boolean isReleased;

    /** Cause of failure which should be propagated to the consumer. */
    @GuardedBy("lock")
    private Throwable failureCause;

    /** Sequence number of the next buffer to be sent to the consumer. */
    private int sequenceNumber;

    @GuardedBy("lock")
    private final Map<String, List<CustomBuffer>> compressedBuffers = new HashMap<>();

    @GuardedBy("lock")
    private final Map<String, List<CustomBuffer>> unCompressedBuffers = new HashMap<>();

    private final ResultSubpartitionIndexSet indexSet;

    private int pageSize;

    SortMergeSubpartitionReader(
            int pageSize,
            ResultSubpartitionIndexSet indexSet,
            BufferAvailabilityListener listener,
            PartitionedFileReader fileReader) {
        this.availabilityListener = checkNotNull(listener);
        this.fileReader = checkNotNull(fileReader);
        this.indexSet = checkNotNull(indexSet);
        this.pageSize = pageSize;
    }

    @VisibleForTesting
    SortMergeSubpartitionReader(
            BufferAvailabilityListener listener, PartitionedFileReader fileReader) {
        this.availabilityListener = checkNotNull(listener);
        this.fileReader = checkNotNull(fileReader);
        this.indexSet = null;
    }

    @Nullable
    @Override
    public BufferAndBacklog getNextBuffer() {
        synchronized (lock) {
            Buffer buffer = buffersRead.poll();
            if (buffer == null) {
                return null;
            }

            if (buffer.isBuffer()) {
                if (buffer instanceof CustomBuffer) {
                    dataBufferBacklog -= ((CustomBuffer) buffer).getPartialBuffers().size();
                } else {
                    --dataBufferBacklog;
                }
            }

            Buffer lookAhead = buffersRead.peek();
            BufferAndBacklog bufferAndBacklog =
                    BufferAndBacklog.fromBufferAndLookahead(
                            buffer,
                            lookAhead == null ? Buffer.DataType.NONE : lookAhead.getDataType(),
                            dataBufferBacklog,
                            sequenceNumber);
            if (buffer instanceof CustomBuffer) {
                sequenceNumber += ((CustomBuffer) buffer).getPartialBuffers().size();
            } else {
                sequenceNumber++;
            }
            return bufferAndBacklog;
        }
    }

    private void addBuffer(Buffer buffer) {
        boolean notifyAvailable = false;
        boolean needRecycleBuffer = false;

        synchronized (lock) {
            if (isReleased) {
                needRecycleBuffer = true;
            } else {
                notifyAvailable = buffersRead.isEmpty();

                buffersRead.add(buffer);
                if (buffer.isBuffer()) {
                    ++dataBufferBacklog;
                }
            }
        }

        if (needRecycleBuffer) {
            buffer.recycleBuffer();
            throw new IllegalStateException("Subpartition reader has been already released.");
        }

        if (notifyAvailable) {
            notifyDataAvailable();
        }
    }

    private void addBufferForSet(Buffer buffer) throws IOException {
        boolean needRecycleBuffer = false;

        synchronized (lock) {
            if (isReleased) {
                needRecycleBuffer = true;
            } else {
                addBufferToCustomBuffer(buffer);
            }
        }

        if (needRecycleBuffer) {
            buffer.recycleBuffer();
            throw new IllegalStateException("Subpartition reader has been already released.");
        }
    }

    private void addBufferToCustomBuffer(Buffer buffer) {
        Map<String, List<CustomBuffer>> map;
        if (buffer.isCompressed()) {
            map = compressedBuffers;
        } else {
            map = unCompressedBuffers;
        }

        List<CustomBuffer> list =
                map.computeIfAbsent(buffer.getDataType().name(), k -> new ArrayList<>());

        if (list.isEmpty() || buffer.readableBytes() > list.get(list.size() - 1).missingLength()) {
            CustomBuffer bb =
                    new CustomBuffer(buffer.getDataType(), pageSize, buffer.isCompressed());
            list.add(bb);
        }

        CustomBuffer target = list.get(list.size() - 1);

        if (target.isBuffer()) {
            ++dataBufferBacklog;
        }
        target.addPartialBuffer(buffer);
    }

    /** This method is called by the IO thread of {@link SortMergeResultPartitionReadScheduler}. */
    boolean readBuffers(Queue<MemorySegment> buffers, BufferRecycler recycler) throws IOException {

        boolean hasRemaining =
                fileReader.readCurrentRegion(
                        buffers,
                        recycler,
                        (buffer) -> {
                            if (indexSet.isHashConvertToBroadcast()) {
                                try {
                                    addBufferForSet(buffer);
                                } catch (IOException e) {
                                    throw new RuntimeException(e);
                                }
                            } else {
                                addBuffer(buffer);
                            }
                        });

        if (indexSet.isHashConvertToBroadcast()) {
            boolean canNotify;
            synchronized (lock) {
                boolean emptyBefore = buffersRead.isEmpty();
                for (List<CustomBuffer> compositeBuffers : compressedBuffers.values()) {
                    buffersRead.addAll(compositeBuffers);
                }
                for (List<CustomBuffer> compositeBuffers : unCompressedBuffers.values()) {
                    buffersRead.addAll(compositeBuffers);
                }
                compressedBuffers.clear();
                unCompressedBuffers.clear();

                boolean notEmptyAfter = !buffersRead.isEmpty();
                canNotify = notEmptyAfter && emptyBefore;
            }

            // can not be locked!
            if (canNotify) {
                notifyDataAvailable();
            }
        }

        return hasRemaining;
    }

    CompletableFuture<?> getReleaseFuture() {
        return releaseFuture;
    }

    void fail(Throwable throwable) {
        checkArgument(throwable != null, "Must be not null.");

        releaseInternal(throwable);
        // notify the netty thread which will propagate the error to the consumer task
        notifyDataAvailable();
    }

    @Override
    public void notifyDataAvailable() {
        availabilityListener.notifyDataAvailable(this);
    }

    @Override
    public int compareTo(SortMergeSubpartitionReader that) {
        int thisQueuedBuffers = unsynchronizedGetNumberOfQueuedBuffers();
        int thatQueuedBuffers = that.unsynchronizedGetNumberOfQueuedBuffers();
        if (thisQueuedBuffers != thatQueuedBuffers
                && (thisQueuedBuffers == 0 || thatQueuedBuffers == 0)) {
            return thisQueuedBuffers > thatQueuedBuffers ? 1 : -1;
        }

        long thisPriority = fileReader.getPriority();
        long thatPriority = that.fileReader.getPriority();

        if (thisPriority == thatPriority) {
            return 0;
        }
        return thisPriority > thatPriority ? 1 : -1;
    }

    @Override
    public void releaseAllResources() {
        releaseInternal(null);
    }

    private void releaseInternal(@Nullable Throwable throwable) {
        List<Buffer> buffersToRecycle;
        synchronized (lock) {
            if (isReleased) {
                return;
            }

            isReleased = true;
            if (failureCause == null) {
                failureCause = throwable;
            }
            for (List<CustomBuffer> compositeBuffers : compressedBuffers.values()) {
                buffersRead.addAll(compositeBuffers);
            }
            for (List<CustomBuffer> compositeBuffers : unCompressedBuffers.values()) {
                buffersRead.addAll(compositeBuffers);
            }
            compressedBuffers.clear();
            unCompressedBuffers.clear();
            buffersToRecycle = new ArrayList<>(buffersRead);
            buffersRead.clear();
            dataBufferBacklog = 0;
        }
        buffersToRecycle.forEach(Buffer::recycleBuffer);
        buffersToRecycle.clear();

        releaseFuture.complete(null);
    }

    @Override
    public boolean isReleased() {
        synchronized (lock) {
            return isReleased;
        }
    }

    @Override
    public void resumeConsumption() {
        throw new UnsupportedOperationException("Method should never be called.");
    }

    @Override
    public void acknowledgeAllDataProcessed() {
        // in case of bounded partitions there is no upstream to acknowledge, we simply ignore
        // the ack, as there are no checkpoints
    }

    @Override
    public Throwable getFailureCause() {
        synchronized (lock) {
            return failureCause;
        }
    }

    @Override
    public AvailabilityWithBacklog getAvailabilityAndBacklog(boolean isCreditAvailable) {
        synchronized (lock) {
            boolean isAvailable;
            if (isReleased) {
                isAvailable = true;
            } else if (buffersRead.isEmpty()) {
                isAvailable = false;
            } else {
                isAvailable = isCreditAvailable || !buffersRead.peek().isBuffer();
            }
            return new AvailabilityWithBacklog(isAvailable, dataBufferBacklog);
        }
    }

    // suppress warning as this method is only for unsafe purpose.
    @SuppressWarnings("FieldAccessNotGuarded")
    @Override
    public int unsynchronizedGetNumberOfQueuedBuffers() {
        return buffersRead.size();
    }

    @Override
    public int getNumberOfQueuedBuffers() {
        synchronized (lock) {
            return buffersRead.size();
        }
    }

    @Override
    public void notifyNewBufferSize(int newBufferSize) {}

    @Override
    public int peekNextBufferSubpartitionId() {
        return 0;
    }
}
