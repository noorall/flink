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

import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.BufferHeader;
import org.apache.flink.runtime.io.network.buffer.BufferRecycler;
import org.apache.flink.runtime.io.network.buffer.CompositeBuffer;
import org.apache.flink.runtime.io.network.buffer.NetworkBuffer;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Queue;
import java.util.function.Consumer;

import static org.apache.flink.runtime.io.network.partition.BufferReaderWriterUtil.HEADER_LENGTH;
import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/** Reader which can read all data of the target subpartition from a {@link PartitionedFile}. */
class CustomPartitionedFileReader extends PartitionedFileReader {

    /** Used to read buffer headers from file channel. */
    private final ByteBuffer headerBuf;

    /** Used to read index entry from index file. */
    private final ByteBuffer indexEntryBuf;

    /** Target {@link PartitionedFile} to read. */
    private final PartitionedFile partitionedFile;

    /** Target subpartition to read. */
    private final ResultSubpartitionIndexSet indexSet;

    /** Data file channel of the target {@link PartitionedFile}. */
    private final FileChannel dataFileChannel;

    /** Index file channel of the target {@link PartitionedFile}. */
    private final FileChannel indexFileChannel;

    CustomPartitionedFileReader(
            PartitionedFile partitionedFile,
            ResultSubpartitionIndexSet indexSet,
            FileChannel dataFileChannel,
            FileChannel indexFileChannel,
            ByteBuffer headerBuffer,
            ByteBuffer indexEntryBuffer) {
        super(
                partitionedFile,
                indexSet,
                dataFileChannel,
                indexFileChannel,
                headerBuffer,
                indexEntryBuffer);
        checkArgument(checkNotNull(dataFileChannel).isOpen(), "Data file channel must be opened.");
        checkArgument(
                checkNotNull(indexFileChannel).isOpen(), "Index file channel must be opened.");

        this.partitionedFile = checkNotNull(partitionedFile);
        this.indexSet = checkNotNull(indexSet);
        this.dataFileChannel = dataFileChannel;
        this.indexFileChannel = indexFileChannel;
        this.headerBuf = headerBuffer;
        this.indexEntryBuf = indexEntryBuffer;
    }

    List<long[]> offsetAndSizes = new ArrayList<>();

    int currentIndex = 0;

    long currentRegionRemainingBytes = 0;

    long nextOffsetToRead = 0;

    private void init(ByteBuffer indexEntryBuf) throws IOException {
        List<long[]> list = new ArrayList<>();
        List<long[]> endOfPartitions = new ArrayList<>(indexSet.size());
        for (int i = 0; i < partitionedFile.getNumRegions(); i++) {
            for (int target = indexSet.getStartIndex();
                    target <= indexSet.getEndIndex();
                    target++) {
                partitionedFile.getIndexEntry(indexFileChannel, indexEntryBuf, i, target);
                long start = indexEntryBuf.getLong();
                long size = indexEntryBuf.getLong();
                if (i < partitionedFile.getNumRegions() - 1) {
                    list.add(new long[] {start, start + size});
                } else {
                    endOfPartitions.add(new long[] {start, start + size});
                }
            }
        }

        offsetAndSizes = merge(list);
        offsetAndSizes.addAll(endOfPartitions);
    }

    private List<long[]> merge(List<long[]> intervals) {
        if (intervals == null || intervals.isEmpty()) {
            return Collections.emptyList();
        }

        // 根据区间的起始值对区间进行排序
        intervals.sort(Comparator.comparingLong(a -> a[0]));

        List<long[]> mergedIntervals = new ArrayList<>();

        long[] currentInterval = intervals.get(0);
        mergedIntervals.add(currentInterval);

        for (long[] interval : intervals) {
            if (currentInterval[1] >= interval[0]) { // 当前区间与下一个区间有重叠
                currentInterval[1] = Math.max(currentInterval[1], interval[1]); // 合并区间
            } else {
                // 没有重叠，将当前区间加入结果列表，并更新当前区间为下一个区间
                currentInterval = interval;
                mergedIntervals.add(currentInterval);
            }
        }

        return mergedIntervals; // 返回合并后的区间
    }

    /**
     * Reads a buffer from the current region of the target {@link PartitionedFile} and moves the
     * read position forward.
     *
     * <p>Note: The caller is responsible for recycling the target buffer if any exception occurs.
     *
     * @param freeSegments The free {@link MemorySegment}s to read data to.
     * @param recycler The {@link BufferRecycler} which is responsible to recycle the target buffer.
     * @param consumer The target {@link Buffer} stores the data read from file channel.
     * @return Whether the file reader has remaining data to read.
     */
    boolean readCurrentRegion(
            Queue<MemorySegment> freeSegments, BufferRecycler recycler, Consumer<Buffer> consumer)
            throws IOException {
        if (currentRegionRemainingBytes == 0) {
            if (currentIndex < offsetAndSizes.size()) {
                currentRegionRemainingBytes =
                        offsetAndSizes.get(currentIndex)[1] - offsetAndSizes.get(currentIndex)[0];
                nextOffsetToRead = offsetAndSizes.get(currentIndex)[0];
                currentIndex++;
            } else {
                return false;
            }
        }

        checkArgument(!freeSegments.isEmpty(), "No buffer available for data reading.");
        dataFileChannel.position(nextOffsetToRead);

        BufferAndHeader partialBuffer = new BufferAndHeader(null, null);
        try {
            while (!freeSegments.isEmpty() && currentRegionRemainingBytes > 0) {
                MemorySegment segment = freeSegments.poll();
                int numBytes = (int) Math.min(segment.size(), currentRegionRemainingBytes);
                ByteBuffer byteBuffer = segment.wrap(0, numBytes);

                try {
                    BufferReaderWriterUtil.readByteBufferFully(dataFileChannel, byteBuffer);
                    byteBuffer.flip();
                    currentRegionRemainingBytes -= byteBuffer.remaining();
                    nextOffsetToRead += byteBuffer.remaining();
                } catch (Throwable throwable) {
                    freeSegments.add(segment);
                    throw throwable;
                }

                NetworkBuffer buffer = new NetworkBuffer(segment, recycler);
                buffer.setSize(byteBuffer.remaining());
                try {
                    partialBuffer = processBuffer(byteBuffer, buffer, partialBuffer, consumer);
                } catch (Throwable throwable) {
                    partialBuffer = new BufferAndHeader(null, null);
                    throw throwable;
                } finally {
                    buffer.recycleBuffer();
                }
            }
        } finally {
            if (headerBuf.position() > 0) {
                nextOffsetToRead -= headerBuf.position();
                currentRegionRemainingBytes += headerBuf.position();
                headerBuf.clear();
            }
            if (partialBuffer.header != null) {
                nextOffsetToRead -= HEADER_LENGTH;
                currentRegionRemainingBytes += HEADER_LENGTH;
            }
            if (partialBuffer.buffer != null) {
                nextOffsetToRead -= partialBuffer.buffer.readableBytes();
                currentRegionRemainingBytes += partialBuffer.buffer.readableBytes();
                partialBuffer.buffer.recycleBuffer();
            }
        }

        boolean remaining = hasRemaining();
        if (remaining && indexSet.isHashConvertToBroadcast() && !freeSegments.isEmpty()) {
            return readCurrentRegion(freeSegments, recycler, consumer);
        }
        return remaining;
    }

    boolean hasRemaining() throws IOException {
        return currentRegionRemainingBytes > 0 || currentIndex < offsetAndSizes.size();
    }

    void initRegionIndex(ByteBuffer initIndexEntryBuffer) throws IOException {
        init(initIndexEntryBuffer);
    }

    /** Gets read priority of this file reader. Smaller value indicates higher priority. */
    long getPriority() {
        return nextOffsetToRead;
    }

    private BufferAndHeader processBuffer(
            ByteBuffer byteBuffer,
            Buffer buffer,
            BufferAndHeader partialBuffer,
            Consumer<Buffer> consumer) {
        BufferHeader header = partialBuffer.header;
        CompositeBuffer targetBuffer = partialBuffer.buffer;
        while (byteBuffer.hasRemaining()) {
            if (header == null && (header = parseBufferHeader(byteBuffer)) == null) {
                break;
            }

            if (targetBuffer != null) {
                buffer.retainBuffer();
                int position = byteBuffer.position() + targetBuffer.missingLength();
                targetBuffer.addPartialBuffer(
                        buffer.readOnlySlice(byteBuffer.position(), targetBuffer.missingLength()));
                byteBuffer.position(position);
            } else if (byteBuffer.remaining() < header.getLength()) {
                if (byteBuffer.hasRemaining()) {
                    buffer.retainBuffer();
                    targetBuffer = new CompositeBuffer(header);
                    targetBuffer.addPartialBuffer(
                            buffer.readOnlySlice(byteBuffer.position(), byteBuffer.remaining()));
                }
                break;
            } else {
                buffer.retainBuffer();
                targetBuffer = new CompositeBuffer(header);
                targetBuffer.addPartialBuffer(
                        buffer.readOnlySlice(byteBuffer.position(), header.getLength()));
                byteBuffer.position(byteBuffer.position() + header.getLength());
            }

            header = null;
            consumer.accept(targetBuffer);
            targetBuffer = null;
        }
        return new BufferAndHeader(targetBuffer, header);
    }

    private BufferHeader parseBufferHeader(ByteBuffer buffer) {
        BufferHeader header = null;
        if (headerBuf.position() > 0) {
            while (headerBuf.hasRemaining()) {
                headerBuf.put(buffer.get());
            }
            headerBuf.flip();
            header = BufferReaderWriterUtil.parseBufferHeader(headerBuf);
            headerBuf.clear();
        }

        if (header == null && buffer.remaining() < HEADER_LENGTH) {
            headerBuf.put(buffer);
        } else if (header == null) {
            header = BufferReaderWriterUtil.parseBufferHeader(buffer);
        }
        return header;
    }

    private static class BufferAndHeader {

        private final CompositeBuffer buffer;
        private final BufferHeader header;

        BufferAndHeader(CompositeBuffer buffer, BufferHeader header) {
            this.buffer = buffer;
            this.header = header;
        }
    }
}
