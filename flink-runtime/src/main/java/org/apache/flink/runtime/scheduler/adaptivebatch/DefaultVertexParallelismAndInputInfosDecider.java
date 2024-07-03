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

package org.apache.flink.runtime.scheduler.adaptivebatch;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.configuration.BatchExecutionOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.runtime.executiongraph.ExecutionVertexInputInfo;
import org.apache.flink.runtime.executiongraph.IndexRange;
import org.apache.flink.runtime.executiongraph.JobVertexInputInfo;
import org.apache.flink.runtime.executiongraph.ParallelismAndInputInfos;
import org.apache.flink.runtime.executiongraph.VertexInputInfoComputationUtils;
import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;
import org.apache.flink.runtime.jobgraph.JobVertexID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * Default implementation of {@link VertexParallelismAndInputInfosDecider}. This implementation will
 * decide parallelism and {@link JobVertexInputInfo}s as follows:
 *
 * <p>1. For job vertices whose inputs are all ALL_TO_ALL edges, evenly distribute data to
 * downstream subtasks, make different downstream subtasks consume roughly the same amount of data.
 *
 * <p>2. For other cases, evenly distribute subpartitions to downstream subtasks, make different
 * downstream subtasks consume roughly the same number of subpartitions.
 */
public class DefaultVertexParallelismAndInputInfosDecider
        implements VertexParallelismAndInputInfosDecider {

    private static final Logger LOG =
            LoggerFactory.getLogger(DefaultVertexParallelismAndInputInfosDecider.class);

    /**
     * The maximum number of subpartitions belonging to the same result that each task can consume.
     * We currently need this limitation to avoid too many channels in a downstream task leading to
     * poor performance.
     *
     * <p>TODO: Once we support one channel to consume multiple upstream subpartitions in the
     * future, we can remove this limitation
     */
    private static final int MAX_NUM_SUBPARTITIONS_PER_TASK_CONSUME = 32768;

    private final int globalMaxParallelism;
    private final int globalMinParallelism;
    private final long dataVolumePerTask;
    private final int globalDefaultSourceParallelism;
    private final double skewedPartitionFactor;
    private final long skewedPartitionThreshold;
    private final double skewedPartitionMergeFactor;

    private DefaultVertexParallelismAndInputInfosDecider(
            int globalMaxParallelism,
            int globalMinParallelism,
            MemorySize dataVolumePerTask,
            int globalDefaultSourceParallelism,
            double skewedPartitionFactor,
            long skewedPartitionThreshold,
            double skewedPartitionMergeFactor) {

        checkArgument(globalMinParallelism > 0, "The minimum parallelism must be larger than 0.");
        checkArgument(
                globalMaxParallelism >= globalMinParallelism,
                "Maximum parallelism should be greater than or equal to the minimum parallelism.");
        checkArgument(
                globalDefaultSourceParallelism > 0,
                "The default source parallelism must be larger than 0.");
        checkNotNull(dataVolumePerTask);
        checkArgument(
                skewedPartitionFactor > 0,
                "The default skewed partition factor must be larger than 0.");
        checkArgument(
                skewedPartitionThreshold > 0,
                "The default skewed threshold must be larger than 0.");
        checkArgument(
                skewedPartitionMergeFactor > 0,
                "The default skewedPartitionMergeFactor must be larger than 0.");

        this.globalMaxParallelism = globalMaxParallelism;
        this.globalMinParallelism = globalMinParallelism;
        this.dataVolumePerTask = dataVolumePerTask.getBytes();
        this.globalDefaultSourceParallelism = globalDefaultSourceParallelism;
        this.skewedPartitionFactor = skewedPartitionFactor;
        this.skewedPartitionThreshold = skewedPartitionThreshold;
        this.skewedPartitionMergeFactor = skewedPartitionMergeFactor;
    }

    @Override
    public ParallelismAndInputInfos decideParallelismAndInputInfosForVertex(
            JobVertexID jobVertexId,
            List<BlockingResultInfo> consumedResults,
            int vertexInitialParallelism,
            int vertexMinParallelism,
            int vertexMaxParallelism) {
        checkArgument(
                vertexInitialParallelism == ExecutionConfig.PARALLELISM_DEFAULT
                        || vertexInitialParallelism > 0);
        checkArgument(
                vertexMinParallelism == ExecutionConfig.PARALLELISM_DEFAULT
                        || vertexMinParallelism > 0);
        checkArgument(
                vertexMaxParallelism > 0
                        && vertexMaxParallelism >= vertexInitialParallelism
                        && vertexMaxParallelism >= vertexMinParallelism);

        if (consumedResults.isEmpty()) {
            // source job vertex
            int parallelism =
                    vertexInitialParallelism > 0
                            ? vertexInitialParallelism
                            : computeSourceParallelismUpperBound(jobVertexId, vertexMaxParallelism);
            return new ParallelismAndInputInfos(parallelism, Collections.emptyMap());
        } else {
            int minParallelism = Math.max(globalMinParallelism, vertexMinParallelism);
            int maxParallelism = globalMaxParallelism;

            if (vertexInitialParallelism == ExecutionConfig.PARALLELISM_DEFAULT
                    && vertexMaxParallelism < minParallelism) {
                LOG.info(
                        "The vertex maximum parallelism {} is smaller than the minimum parallelism {}. "
                                + "Use {} as the lower bound to decide parallelism of job vertex {}.",
                        vertexMaxParallelism,
                        minParallelism,
                        vertexMaxParallelism,
                        jobVertexId);
                minParallelism = vertexMaxParallelism;
            }
            if (vertexInitialParallelism == ExecutionConfig.PARALLELISM_DEFAULT
                    && vertexMaxParallelism < maxParallelism) {
                LOG.info(
                        "The vertex maximum parallelism {} is smaller than the global maximum parallelism {}. "
                                + "Use {} as the upper bound to decide parallelism of job vertex {}.",
                        vertexMaxParallelism,
                        maxParallelism,
                        vertexMaxParallelism,
                        jobVertexId);
                maxParallelism = vertexMaxParallelism;
            }
            checkState(maxParallelism >= minParallelism);

            if (vertexInitialParallelism == ExecutionConfig.PARALLELISM_DEFAULT
                    && areAllInputsAllToAll(consumedResults)
                    && !areAllInputsBroadcast(consumedResults)) {
                LOG.info(
                        "### DEBUG ### try decideParallelismAndEvenlyDistributeSkewedData for {}, the consumed results size is {}({})",
                        jobVertexId,
                        consumedResults.size(),
                        consumedResults);
                return decideParallelismAndEvenlyDistributeSkewedData(
                        jobVertexId,
                        consumedResults,
                        vertexInitialParallelism,
                        minParallelism,
                        maxParallelism);
            } else {
                LOG.info(
                        "### DEBUG ### try decideParallelismAndEvenlyDistributeSubpartitions for {}, the consumed results size is {}({})",
                        jobVertexId,
                        consumedResults.size(),
                        consumedResults);
                return decideParallelismAndEvenlyDistributeSubpartitions(
                        jobVertexId,
                        consumedResults,
                        vertexInitialParallelism,
                        minParallelism,
                        maxParallelism);
            }
        }
    }

    @Override
    public int computeSourceParallelismUpperBound(JobVertexID jobVertexId, int maxParallelism) {
        if (globalDefaultSourceParallelism > maxParallelism) {
            LOG.info(
                    "The global default source parallelism {} is larger than the maximum parallelism {}. "
                            + "Use {} as the upper bound parallelism of source job vertex {}.",
                    globalDefaultSourceParallelism,
                    maxParallelism,
                    maxParallelism,
                    jobVertexId);
            return maxParallelism;
        } else {
            return globalDefaultSourceParallelism;
        }
    }

    @Override
    public long getDataVolumePerTask() {
        return dataVolumePerTask;
    }

    private static boolean areAllInputsAllToAll(List<BlockingResultInfo> consumedResults) {
        return consumedResults.stream().noneMatch(BlockingResultInfo::isPointwise);
    }

    private static boolean areAllInputsBroadcast(List<BlockingResultInfo> consumedResults) {
        return consumedResults.stream().allMatch(BlockingResultInfo::isBroadcast);
    }

    /**
     * Decide parallelism and input infos, which will make the subpartitions be evenly distributed
     * to downstream subtasks, such that different downstream subtasks consume roughly the same
     * number of subpartitions.
     *
     * @param jobVertexId The job vertex id
     * @param consumedResults The information of consumed blocking results
     * @param initialParallelism The initial parallelism of the job vertex
     * @param minParallelism the min parallelism
     * @param maxParallelism the max parallelism
     * @return the parallelism and vertex input infos
     */
    private ParallelismAndInputInfos decideParallelismAndEvenlyDistributeSubpartitions(
            JobVertexID jobVertexId,
            List<BlockingResultInfo> consumedResults,
            int initialParallelism,
            int minParallelism,
            int maxParallelism) {
        checkArgument(!consumedResults.isEmpty());
        int parallelism =
                initialParallelism > 0
                        ? initialParallelism
                        : decideParallelism(
                                jobVertexId, consumedResults, minParallelism, maxParallelism);
        return new ParallelismAndInputInfos(
                parallelism,
                VertexInputInfoComputationUtils.computeVertexInputInfos(
                        parallelism, consumedResults, true));
    }

    int decideParallelism(
            JobVertexID jobVertexId,
            List<BlockingResultInfo> consumedResults,
            int minParallelism,
            int maxParallelism) {
        checkArgument(!consumedResults.isEmpty());

        // Considering that the sizes of broadcast results are usually very small, we compute the
        // parallelism only based on sizes of non-broadcast results
        final List<BlockingResultInfo> nonBroadcastResults =
                getNonBroadcastResultInfos(consumedResults);
        if (nonBroadcastResults.isEmpty()) {
            return minParallelism;
        }

        long totalBytes =
                nonBroadcastResults.stream()
                        .mapToLong(BlockingResultInfo::getNumBytesProduced)
                        .sum();
        int parallelism = (int) Math.ceil((double) totalBytes / dataVolumePerTask);
        int minParallelismLimitedByMaxSubpartitions =
                (int)
                        Math.ceil(
                                (double) getMaxNumSubpartitions(nonBroadcastResults)
                                        / MAX_NUM_SUBPARTITIONS_PER_TASK_CONSUME);
        parallelism = Math.max(parallelism, minParallelismLimitedByMaxSubpartitions);

        LOG.debug(
                "The total size of non-broadcast data is {}, the initially decided parallelism of job vertex {} is {}.",
                new MemorySize(totalBytes),
                jobVertexId,
                parallelism);

        if (parallelism < minParallelism) {
            LOG.info(
                    "The initially decided parallelism {} is smaller than the minimum parallelism {}. "
                            + "Use {} as the finally decided parallelism of job vertex {}.",
                    parallelism,
                    minParallelism,
                    minParallelism,
                    jobVertexId);
            parallelism = minParallelism;
        } else if (parallelism > maxParallelism) {
            LOG.info(
                    "The initially decided parallelism {} is larger than the maximum parallelism {}. "
                            + "Use {} as the finally decided parallelism of job vertex {}.",
                    parallelism,
                    maxParallelism,
                    maxParallelism,
                    jobVertexId);
            parallelism = maxParallelism;
        }

        return parallelism;
    }

    /**
     * Decide parallelism and input infos, which will make the data be evenly distributed to
     * downstream subtasks, such that different downstream subtasks consume roughly the same amount
     * of data.
     *
     * @param jobVertexId The job vertex id
     * @param consumedResults The information of consumed blocking results
     * @param initialParallelism The initial parallelism of the job vertex
     * @param minParallelism the min parallelism
     * @param maxParallelism the max parallelism
     * @return the parallelism and vertex input infos
     */
    private ParallelismAndInputInfos decideParallelismAndEvenlyDistributeData(
            JobVertexID jobVertexId,
            List<BlockingResultInfo> consumedResults,
            int initialParallelism,
            int minParallelism,
            int maxParallelism) {
        checkArgument(initialParallelism == ExecutionConfig.PARALLELISM_DEFAULT);
        checkArgument(!consumedResults.isEmpty());
        consumedResults.forEach(resultInfo -> checkState(!resultInfo.isPointwise()));

        // Considering that the sizes of broadcast results are usually very small, we compute the
        // parallelism and input infos only based on sizes of non-broadcast results
        final List<BlockingResultInfo> nonBroadcastResults =
                getNonBroadcastResultInfos(consumedResults);
        int subpartitionNum = checkAndGetSubpartitionNum(nonBroadcastResults);

        long[] bytesBySubpartition = new long[subpartitionNum];
        Arrays.fill(bytesBySubpartition, 0L);
        for (BlockingResultInfo resultInfo : nonBroadcastResults) {
            List<Long> subpartitionBytes =
                    ((AllToAllBlockingResultInfo) resultInfo).getAggregatedSubpartitionBytes();
            for (int i = 0; i < subpartitionNum; ++i) {
                bytesBySubpartition[i] += subpartitionBytes.get(i);
            }
        }

        int maxNumPartitions = getMaxNumPartitions(nonBroadcastResults);
        int maxRangeSize = MAX_NUM_SUBPARTITIONS_PER_TASK_CONSUME / maxNumPartitions;
        // compute subpartition ranges
        List<IndexRange> subpartitionRanges =
                computeSubpartitionRanges(bytesBySubpartition, dataVolumePerTask, maxRangeSize);

        // if the parallelism is not legal, adjust to a legal parallelism
        if (!isLegalParallelism(subpartitionRanges.size(), minParallelism, maxParallelism)) {
            Optional<List<IndexRange>> adjustedSubpartitionRanges =
                    adjustToClosestLegalParallelism(
                            dataVolumePerTask,
                            subpartitionRanges.size(),
                            minParallelism,
                            maxParallelism,
                            Arrays.stream(bytesBySubpartition).min().getAsLong(),
                            Arrays.stream(bytesBySubpartition).sum(),
                            limit -> computeParallelism(bytesBySubpartition, limit, maxRangeSize),
                            limit ->
                                    computeSubpartitionRanges(
                                            bytesBySubpartition, limit, maxRangeSize));
            if (!adjustedSubpartitionRanges.isPresent()) {
                // can't find any legal parallelism, fall back to evenly distribute subpartitions
                LOG.info(
                        "Cannot find a legal parallelism to evenly distribute data for job vertex {}. "
                                + "Fall back to compute a parallelism that can evenly distribute subpartitions.",
                        jobVertexId);
                return decideParallelismAndEvenlyDistributeSubpartitions(
                        jobVertexId,
                        consumedResults,
                        initialParallelism,
                        minParallelism,
                        maxParallelism);
            }
            subpartitionRanges = adjustedSubpartitionRanges.get();
        }

        checkState(isLegalParallelism(subpartitionRanges.size(), minParallelism, maxParallelism));
        return createParallelismAndInputInfos(consumedResults, subpartitionRanges);
    }

    public ParallelismAndInputInfos decideParallelismAndEvenlyDistributeSkewedData(
            JobVertexID jobVertexId,
            List<BlockingResultInfo> consumedResults,
            int initialParallelism,
            int minParallelism,
            int maxParallelism) {
        checkArgument(initialParallelism == ExecutionConfig.PARALLELISM_DEFAULT);
        checkArgument(!consumedResults.isEmpty());
        consumedResults.forEach(resultInfo -> checkState(!resultInfo.isPointwise()));

        if (consumedResults.size() != 2) {
            LOG.info(
                    " The size of consumedResults of Job vertex {} is not equal to 2. "
                            + "Fall back to compute a parallelism that can evenly distribute data.",
                    jobVertexId);
            return decideParallelismAndEvenlyDistributeData(
                    jobVertexId,
                    consumedResults,
                    initialParallelism,
                    minParallelism,
                    maxParallelism);
        }
        BlockingResultInfo leftResultInfo = consumedResults.get(0);
        BlockingResultInfo rightResultInfo = consumedResults.get(1);
        if (!leftResultInfo.isSkewed() && !rightResultInfo.isSkewed()) {
            LOG.info(
                    " Job vertex {} no data skew occurs. "
                            + "Fall back to compute a parallelism that can evenly distribute data.",
                    jobVertexId);
            return decideParallelismAndEvenlyDistributeData(
                    jobVertexId,
                    consumedResults,
                    initialParallelism,
                    minParallelism,
                    maxParallelism);
        }
        int subPartitionNum = checkAndGetSubpartitionNum(consumedResults);
        List<Long> leftSubpartitionBytes =
                ((AllToAllBlockingResultInfo) leftResultInfo).getAggregatedSubpartitionBytes();
        List<Long> rightSubpartitionBytes =
                ((AllToAllBlockingResultInfo) rightResultInfo).getAggregatedSubpartitionBytes();

        long leftMedSize = median(leftSubpartitionBytes);
        long rightMedSize = median(rightSubpartitionBytes);

        boolean canSplitLeft = leftResultInfo.isSplittable() & !leftResultInfo.isBroadcast();
        boolean canSplitRight = rightResultInfo.isSplittable() & !rightResultInfo.isBroadcast();

        long leftSkewThreshold =
                getSkewThreshold(leftMedSize, skewedPartitionThreshold, skewedPartitionFactor);
        long rightSkewThreshold =
                getSkewThreshold(rightMedSize, skewedPartitionThreshold, skewedPartitionFactor);
        long leftTargetSize = getTargetSize(leftSubpartitionBytes, leftSkewThreshold);
        long rightTargetSize = getTargetSize(rightSubpartitionBytes, rightSkewThreshold);

        LOG.info("The dataVolumePerTask is {}", dataVolumePerTask);

        LOG.info(
                "The left skewed threshold is {} and the target size is {}.",
                leftSkewThreshold,
                leftTargetSize);

        LOG.info(
                "The right skewed threshold is {} and the target size is {}.",
                rightSkewThreshold,
                rightTargetSize);

        List<IndexRange> leftSplitPartitionRanges = new ArrayList<>();
        List<IndexRange> rightSplitPartitionRanges = new ArrayList<>();
        // Create a Map between the spilt subPartitions and the original Partitions
        Map<Integer, Integer> mapToSubpartitionIdx = new HashMap<>();

        int subPartitionNumAfterSplit = 0;

        for (int i = 0; i < subPartitionNum; ++i) {
            boolean isLeftSkew = canSplitLeft && leftSubpartitionBytes.get(i) > leftSkewThreshold;
            boolean isRightSkew =
                    canSplitRight && rightSubpartitionBytes.get(i) > rightSkewThreshold;

            List<IndexRange> leftPartitionRange;
            if (isLeftSkew) {
                leftPartitionRange =
                        splitSkewPartition(
                                leftResultInfo.getSubpartitionBytesByPartitionIndex(),
                                i,
                                leftTargetSize);
                LOG.info(
                        "Left side partition {} is skewed, split it into {} parts: {}",
                        i,
                        leftPartitionRange.size(),
                        leftPartitionRange);
            } else {
                leftPartitionRange =
                        Collections.singletonList(
                                new IndexRange(0, leftResultInfo.getNumPartitions() - 1));
            }
            List<IndexRange> rightPartitionRange;
            if (isRightSkew) {
                rightPartitionRange =
                        splitSkewPartition(
                                rightResultInfo.getSubpartitionBytesByPartitionIndex(),
                                i,
                                rightTargetSize);
                LOG.info(
                        "Right side partition {} is skewed, split it into {} parts: {}",
                        i,
                        rightPartitionRange.size(),
                        rightPartitionRange);
            } else {
                rightPartitionRange =
                        Collections.singletonList(
                                new IndexRange(0, rightResultInfo.getNumPartitions() - 1));
            }
            for (IndexRange left : leftPartitionRange) {
                for (IndexRange right : rightPartitionRange) {
                    leftSplitPartitionRanges.add(left);
                    rightSplitPartitionRanges.add(right);
                    mapToSubpartitionIdx.put(subPartitionNumAfterSplit, i);
                    ++subPartitionNumAfterSplit;
                }
            }
        }

        final List<BlockingResultInfo> nonBroadcastResults =
                getNonBroadcastResultInfos(consumedResults);
        int maxNumPartitions = getMaxNumPartitions(nonBroadcastResults);
        int maxRangeSize = MAX_NUM_SUBPARTITIONS_PER_TASK_CONSUME / maxNumPartitions;

        long[] bytesBySplitSubPartitions = new long[subPartitionNumAfterSplit];
        for (int i = 0; i < subPartitionNumAfterSplit; ++i) {
            int subPartitionIdx = mapToSubpartitionIdx.get(i);
            IndexRange subPartitionRange = new IndexRange(subPartitionIdx, subPartitionIdx);
            if (!leftResultInfo.isBroadcast()) {
                bytesBySplitSubPartitions[i] +=
                        getNumBytesByIndexRange(
                                leftResultInfo, leftSplitPartitionRanges.get(i), subPartitionRange);
            }
            if (!rightResultInfo.isBroadcast()) {
                bytesBySplitSubPartitions[i] +=
                        getNumBytesByIndexRange(
                                rightResultInfo,
                                rightSplitPartitionRanges.get(i),
                                subPartitionRange);
            }
        }

        // compute subpartition ranges
        List<IndexRange> splitSubpartitionRanges =
                computeSubpartitionRangesForSkewed(
                        bytesBySplitSubPartitions,
                        dataVolumePerTask,
                        maxRangeSize,
                        mapToSubpartitionIdx,
                        skewedPartitionMergeFactor);

        // if the parallelism is not legal, adjust to a legal parallelism
        if (!isLegalParallelism(splitSubpartitionRanges.size(), minParallelism, maxParallelism)) {
            LOG.info(
                    "The maximum parallelism limit is exceeded and the parallelism is recalculate, {}",
                    jobVertexId);
            Optional<List<IndexRange>> adjustedSubpartitionRanges =
                    adjustToClosestLegalParallelism(
                            dataVolumePerTask,
                            splitSubpartitionRanges.size(),
                            minParallelism,
                            maxParallelism,
                            Arrays.stream(bytesBySplitSubPartitions).min().getAsLong(),
                            Arrays.stream(bytesBySplitSubPartitions).sum(),
                            limit ->
                                    computeParallelismForSkewed(
                                            bytesBySplitSubPartitions,
                                            limit,
                                            maxRangeSize,
                                            mapToSubpartitionIdx,
                                            skewedPartitionMergeFactor),
                            limit ->
                                    computeSubpartitionRangesForSkewed(
                                            bytesBySplitSubPartitions,
                                            limit,
                                            maxRangeSize,
                                            mapToSubpartitionIdx,
                                            skewedPartitionMergeFactor));
            if (!adjustedSubpartitionRanges.isPresent()) {
                // can't find any legal parallelism, fall back to evenly distribute subpartitions
                LOG.info(
                        "Cannot find a legal parallelism to evenly distribute skewed data for job vertex {}. "
                                + "Fall back to compute a parallelism that can evenly distribute data.",
                        jobVertexId);
                return decideParallelismAndEvenlyDistributeData(
                        jobVertexId,
                        consumedResults,
                        initialParallelism,
                        minParallelism,
                        maxParallelism);
            }
            splitSubpartitionRanges = adjustedSubpartitionRanges.get();
        }

        checkState(
                isLegalParallelism(splitSubpartitionRanges.size(), minParallelism, maxParallelism));
        return createParallelismAndInputInfosForSplitPartition(
                consumedResults,
                splitSubpartitionRanges,
                leftSplitPartitionRanges,
                rightSplitPartitionRanges,
                mapToSubpartitionIdx);
    }

    private static long getNumBytesByIndexRange(
            BlockingResultInfo resultInfo,
            IndexRange partitionIndexRange,
            IndexRange subpartitionIndexRange) {
        int numSubpartitions = checkAndGetSubpartitionNum(Collections.singletonList(resultInfo));
        Map<Integer, long[]> subpartitionBytesByPartitionIndex =
                resultInfo.getSubpartitionBytesByPartitionIndex();
        checkState(
                partitionIndexRange.getEndIndex() < resultInfo.getNumPartitions(),
                "Partition index %s is out of range.",
                partitionIndexRange.getEndIndex());
        checkState(
                subpartitionIndexRange.getEndIndex() < numSubpartitions,
                "Subpartition index %s is out of range.",
                subpartitionIndexRange.getEndIndex());
        long numBytes = 0L;
        for (int i = partitionIndexRange.getStartIndex();
                i <= partitionIndexRange.getEndIndex();
                ++i) {
            numBytes +=
                    Arrays.stream(
                                    subpartitionBytesByPartitionIndex.get(i),
                                    subpartitionIndexRange.getStartIndex(),
                                    subpartitionIndexRange.getEndIndex() + 1)
                            .sum();
        }
        return numBytes;
    }

    private static ParallelismAndInputInfos createParallelismAndInputInfosForSplitPartition(
            List<BlockingResultInfo> consumedResults,
            List<IndexRange> splitSubPartitionRanges,
            List<IndexRange> leftSplitSubPartitions,
            List<IndexRange> rightSplitSubPartitions,
            Map<Integer, Integer> mapToSubpartitionIdx) {
        checkArgument(consumedResults.size() == 2);
        final Map<IntermediateDataSetID, JobVertexInputInfo> vertexInputInfos = new HashMap<>();
        BlockingResultInfo leftResultInfo = consumedResults.get(0);
        BlockingResultInfo rightResultInfo = consumedResults.get(1);
        List<ExecutionVertexInputInfo> leftExecutionVertexInputInfos =
                createdExecutionVertexInputInfos(
                        leftResultInfo,
                        splitSubPartitionRanges,
                        leftSplitSubPartitions,
                        mapToSubpartitionIdx);
        List<ExecutionVertexInputInfo> rightExecutionVertexInputInfos =
                createdExecutionVertexInputInfos(
                        rightResultInfo,
                        splitSubPartitionRanges,
                        rightSplitSubPartitions,
                        mapToSubpartitionIdx);
        vertexInputInfos.put(
                leftResultInfo.getResultId(),
                new JobVertexInputInfo(leftExecutionVertexInputInfos));
        vertexInputInfos.put(
                rightResultInfo.getResultId(),
                new JobVertexInputInfo(rightExecutionVertexInputInfos));
        return new ParallelismAndInputInfos(splitSubPartitionRanges.size(), vertexInputInfos);
    }

    public static List<ExecutionVertexInputInfo> createdExecutionVertexInputInfos(
            BlockingResultInfo resultInfo,
            List<IndexRange> combinedPartitionRanges,
            List<IndexRange> splitPartitions,
            Map<Integer, Integer> mapToSubpartitionIdx) {
        int sourceParallelism = resultInfo.getNumPartitions();
        List<ExecutionVertexInputInfo> executionVertexInputInfos = new ArrayList<>();
        for (int i = 0; i < combinedPartitionRanges.size(); ++i) {
            ExecutionVertexInputInfo executionVertexInputInfo;
            if (resultInfo.isBroadcast()) {
                executionVertexInputInfo =
                        new ExecutionVertexInputInfo(
                                i, new IndexRange(0, sourceParallelism - 1), new IndexRange(0, 0));
            } else {
                IndexRange splitSubpartitionRange = combinedPartitionRanges.get(i);
                Map<IndexRange, IndexRange> mergedPartitionRanges =
                        mergePartitionRanges(
                                splitSubpartitionRange, splitPartitions, mapToSubpartitionIdx);
                if (mergedPartitionRanges.size() > 1) {
                    LOG.info("Input info for Task {} is {}", i, mergedPartitionRanges);
                }
                executionVertexInputInfo = new ExecutionVertexInputInfo(i, mergedPartitionRanges);
            }
            executionVertexInputInfos.add(executionVertexInputInfo);
        }
        return executionVertexInputInfos;
    }

    private static Map<IndexRange, IndexRange> mergePartitionRanges(
            IndexRange combinedPartitionRange,
            List<IndexRange> splitPartitions,
            Map<Integer, Integer> mapToSubpartitionIdx) {
        int startSubpartitionIdx = mapToSubpartitionIdx.get(combinedPartitionRange.getStartIndex());
        int endSubpartitionIdx = mapToSubpartitionIdx.get(combinedPartitionRange.getEndIndex());
        Map<Integer, IndexRange> subPartitionToPartitionIdx = new TreeMap<>();
        for (int i = combinedPartitionRange.getStartIndex();
                i <= combinedPartitionRange.getEndIndex();
                i++) {
            IndexRange partitionRange = splitPartitions.get(i);
            int subPartitionIdx = mapToSubpartitionIdx.get(i);
            subPartitionToPartitionIdx.merge(
                    subPartitionIdx,
                    partitionRange,
                    (oldRange, newRange) -> {
                        if (oldRange.equals(newRange)) {
                            return oldRange;
                        }
                        checkArgument(oldRange.getEndIndex() + 1 == newRange.getStartIndex());
                        return new IndexRange(oldRange.getStartIndex(), newRange.getEndIndex());
                    });
        }
        int startIdx = startSubpartitionIdx;
        IndexRange prePartitionIndexRange = subPartitionToPartitionIdx.get(startIdx);
        Map<IndexRange, IndexRange> mergedPartitionRanges = new LinkedHashMap<>();
        for (int i = startSubpartitionIdx; i <= endSubpartitionIdx; ++i) {
            IndexRange partitionIndexRange = subPartitionToPartitionIdx.get(i);
            if (!prePartitionIndexRange.equals(partitionIndexRange)) {
                mergedPartitionRanges.put(prePartitionIndexRange, new IndexRange(startIdx, i - 1));
                prePartitionIndexRange = partitionIndexRange;
                startIdx = i;
            }
        }
        mergedPartitionRanges.put(
                prePartitionIndexRange, new IndexRange(startIdx, endSubpartitionIdx));
        return reorganizePartitionRange(mergedPartitionRanges);
    }

    private static Map<IndexRange, IndexRange> reorganizePartitionRange(
            Map<IndexRange, IndexRange> mergedPartitionRanges) {
        TreeSet<Integer> pointSet = new TreeSet<>();
        for (IndexRange partitionIndexRange : mergedPartitionRanges.keySet()) {
            pointSet.add(partitionIndexRange.getStartIndex());
            pointSet.add(partitionIndexRange.getEndIndex() + 1);
        }

        Map<IndexRange, IndexRange> reorganizedPartitionRange = new LinkedHashMap<>();

        Iterator<Integer> iterator = pointSet.iterator();
        int prev = iterator.next();
        while (iterator.hasNext()) {
            int curr = iterator.next() - 1;
            if (prev <= curr) {
                IndexRange newPartitionRange = new IndexRange(prev, curr);
                reorganizedPartitionRange.put(
                        newPartitionRange,
                        constructSubpartitionIndexRange(newPartitionRange, mergedPartitionRanges));
            }
            prev = curr + 1;
        }
        return reorganizedPartitionRange;
    }

    private static IndexRange constructSubpartitionIndexRange(
            IndexRange partitionIndexRange, Map<IndexRange, IndexRange> mergedPartitionRanges) {
        int subPartitionStartIndex = Integer.MAX_VALUE;
        int subPartitionEndIndex = Integer.MIN_VALUE;
        for (Map.Entry<IndexRange, IndexRange> entry : mergedPartitionRanges.entrySet()) {
            IndexRange oldPartitionRange = entry.getKey();
            IndexRange oldSubPartitionRange = entry.getValue();
            if (partitionIndexRange.getStartIndex() <= oldPartitionRange.getEndIndex()
                    && partitionIndexRange.getEndIndex() >= oldPartitionRange.getStartIndex()) {
                subPartitionStartIndex =
                        Math.min(oldSubPartitionRange.getStartIndex(), subPartitionStartIndex);
                subPartitionEndIndex =
                        Math.max(oldSubPartitionRange.getEndIndex(), subPartitionEndIndex);
            }
        }
        return new IndexRange(subPartitionStartIndex, subPartitionEndIndex);
    }

    public static List<IndexRange> splitSkewPartition(
            Map<Integer, long[]> subPartitionBytesByPartitionIndex,
            int subPartitionIndex,
            long targetSize) {
        List<IndexRange> splitPartitionRange = new ArrayList<>();
        int partitionNum = subPartitionBytesByPartitionIndex.size();
        long tmpSum = 0;
        int startIndex = 0;
        for (int i = 0; i < partitionNum; ++i) {
            long[] subPartitionBytes = subPartitionBytesByPartitionIndex.get(i);
            long num = subPartitionBytes[subPartitionIndex];
            if (i == startIndex || tmpSum + num < targetSize) {
                tmpSum += num;
            } else {
                LOG.info("The total size is {}, split it.", tmpSum);
                splitPartitionRange.add(new IndexRange(startIndex, i - 1));
                startIndex = i;
                tmpSum = num;
            }
        }
        LOG.info("The total size is {}, split it.", tmpSum);
        splitPartitionRange.add(new IndexRange(startIndex, partitionNum - 1));
        return splitPartitionRange;
    }

    @VisibleForTesting
    public static long median(List<Long> nums) {
        int len = nums.size();
        List<Long> sortedNums = nums.stream().sorted().collect(Collectors.toList());
        if (len % 2 == 0) {
            return Math.max((sortedNums.get(len / 2) + sortedNums.get(len / 2 - 1)) / 2, 1L);
        } else {
            return Math.max(sortedNums.get(len / 2), 1L);
        }
    }

    private static long getSkewThreshold(
            long medSize, long skewedPartitionThreshold, double skewedPartitionFactor) {
        return (long) Math.max(skewedPartitionThreshold, medSize * skewedPartitionFactor);
    }

    private long getTargetSize(List<Long> subpartitionBytes, long skewedThreshold) {
        List<Long> nonSkewPartitions =
                subpartitionBytes.stream()
                        .filter(v -> v <= skewedThreshold)
                        .collect(Collectors.toList());
        if (nonSkewPartitions.isEmpty()) {
            return dataVolumePerTask / 2;
        } else {
            return Math.max(
                    dataVolumePerTask / 2,
                    nonSkewPartitions.stream().mapToLong(Long::longValue).sum()
                            / nonSkewPartitions.size());
        }
    }

    public static boolean hasSkewPartitions(
            BlockingResultInfo consumedResult,
            long skewedPartitionThreshold,
            double skewedPartitionFactor) {
        if (consumedResult.isBroadcast() || consumedResult.isPointwise()) {
            return false;
        }

        List<Long> subpartitionBytes =
                ((AllToAllBlockingResultInfo) consumedResult).getAggregatedSubpartitionBytes();

        long medSize = median(subpartitionBytes);
        long skewThreshold =
                getSkewThreshold(medSize, skewedPartitionThreshold, skewedPartitionFactor);

        for (Long subpartitionByte : subpartitionBytes) {
            if (subpartitionByte > skewThreshold) {
                return true;
            }
        }

        return false;
    }

    private static boolean isLegalParallelism(
            int parallelism, int minParallelism, int maxParallelism) {
        return parallelism >= minParallelism && parallelism <= maxParallelism;
    }

    private static int checkAndGetSubpartitionNum(List<BlockingResultInfo> consumedResults) {
        final Set<Integer> subpartitionNumSet =
                consumedResults.stream()
                        .flatMap(
                                resultInfo ->
                                        IntStream.range(0, resultInfo.getNumPartitions())
                                                .boxed()
                                                .map(resultInfo::getNumSubpartitions))
                        .collect(Collectors.toSet());
        // all partitions have the same subpartition num
        checkState(subpartitionNumSet.size() == 1);
        return subpartitionNumSet.iterator().next();
    }

    /**
     * Adjust the parallelism to the closest legal parallelism and return the computed subpartition
     * ranges.
     *
     * @param currentDataVolumeLimit current data volume limit
     * @param currentParallelism current parallelism
     * @param minParallelism the min parallelism
     * @param maxParallelism the max parallelism
     * @param minLimit the minimum data volume limit
     * @param maxLimit the maximum data volume limit
     * @param parallelismComputer a function to compute the parallelism according to the data volume
     *     limit
     * @param subpartitionRangesComputer a function to compute the subpartition ranges according to
     *     the data volume limit
     * @return the computed subpartition ranges or {@link Optional#empty()} if we can't find any
     *     legal parallelism
     */
    private static Optional<List<IndexRange>> adjustToClosestLegalParallelism(
            long currentDataVolumeLimit,
            int currentParallelism,
            int minParallelism,
            int maxParallelism,
            long minLimit,
            long maxLimit,
            Function<Long, Integer> parallelismComputer,
            Function<Long, List<IndexRange>> subpartitionRangesComputer) {
        long adjustedDataVolumeLimit = currentDataVolumeLimit;
        if (currentParallelism < minParallelism) {
            // Current parallelism is smaller than the user-specified lower-limit of parallelism ,
            // we need to adjust it to the closest/minimum possible legal parallelism. That is, we
            // need to find the maximum legal dataVolumeLimit.
            adjustedDataVolumeLimit =
                    BisectionSearchUtils.findMaxLegalValue(
                            value -> parallelismComputer.apply(value) >= minParallelism,
                            minLimit,
                            currentDataVolumeLimit);

            // When we find the minimum possible legal parallelism, the dataVolumeLimit that can
            // lead to this parallelism may be a range, and we need to find the minimum value of
            // this range to make the data distribution as even as possible (the smaller the
            // dataVolumeLimit, the more even the distribution)
            final long minPossibleLegalParallelism =
                    parallelismComputer.apply(adjustedDataVolumeLimit);
            adjustedDataVolumeLimit =
                    BisectionSearchUtils.findMinLegalValue(
                            value ->
                                    parallelismComputer.apply(value) == minPossibleLegalParallelism,
                            minLimit,
                            adjustedDataVolumeLimit);

        } else if (currentParallelism > maxParallelism) {
            // Current parallelism is larger than the user-specified upper-limit of parallelism ,
            // we need to adjust it to the closest/maximum possible legal parallelism. That is, we
            // need to find the minimum legal dataVolumeLimit.
            adjustedDataVolumeLimit =
                    BisectionSearchUtils.findMinLegalValue(
                            value -> parallelismComputer.apply(value) <= maxParallelism,
                            currentDataVolumeLimit,
                            maxLimit);
        }

        int adjustedParallelism = parallelismComputer.apply(adjustedDataVolumeLimit);
        if (isLegalParallelism(adjustedParallelism, minParallelism, maxParallelism)) {
            return Optional.of(subpartitionRangesComputer.apply(adjustedDataVolumeLimit));
        } else {
            return Optional.empty();
        }
    }

    private static ParallelismAndInputInfos createParallelismAndInputInfos(
            List<BlockingResultInfo> consumedResults, List<IndexRange> subpartitionRanges) {

        final Map<IntermediateDataSetID, JobVertexInputInfo> vertexInputInfos = new HashMap<>();
        consumedResults.forEach(
                resultInfo -> {
                    int sourceParallelism = resultInfo.getNumPartitions();
                    IndexRange partitionRange = new IndexRange(0, sourceParallelism - 1);

                    List<ExecutionVertexInputInfo> executionVertexInputInfos = new ArrayList<>();
                    for (int i = 0; i < subpartitionRanges.size(); ++i) {
                        IndexRange subpartitionRange;
                        if (resultInfo.isBroadcast()) {
                            subpartitionRange = new IndexRange(0, 0);
                        } else {
                            subpartitionRange = subpartitionRanges.get(i);
                        }
                        ExecutionVertexInputInfo executionVertexInputInfo =
                                new ExecutionVertexInputInfo(i, partitionRange, subpartitionRange);
                        executionVertexInputInfos.add(executionVertexInputInfo);
                    }

                    vertexInputInfos.put(
                            resultInfo.getResultId(),
                            new JobVertexInputInfo(executionVertexInputInfos));
                });
        return new ParallelismAndInputInfos(subpartitionRanges.size(), vertexInputInfos);
    }

    private static List<IndexRange> computeSubpartitionRanges(
            long[] nums, long limit, int maxRangeSize) {
        List<IndexRange> subpartitionRanges = new ArrayList<>();
        long tmpSum = 0;
        int startIndex = 0;
        for (int i = 0; i < nums.length; ++i) {
            long num = nums[i];
            if (i == startIndex
                    || (tmpSum + num <= limit && (i - startIndex + 1) <= maxRangeSize)) {
                tmpSum += num;
            } else {
                subpartitionRanges.add(new IndexRange(startIndex, i - 1));
                startIndex = i;
                tmpSum = num;
            }
        }
        subpartitionRanges.add(new IndexRange(startIndex, nums.length - 1));
        return subpartitionRanges;
    }

    private static int computeParallelism(long[] nums, long limit, int maxRangeSize) {
        long tmpSum = 0;
        int startIndex = 0;
        int count = 1;
        for (int i = 0; i < nums.length; ++i) {
            long num = nums[i];
            if (i == startIndex
                    || (tmpSum + num <= limit && (i - startIndex + 1) <= maxRangeSize)) {
                tmpSum += num;
            } else {
                startIndex = i;
                tmpSum = num;
                count += 1;
            }
        }
        return count;
    }

    private static List<IndexRange> computeSubpartitionRangesForSkewed(
            long[] nums,
            long limit,
            int maxRangeSize,
            Map<Integer, Integer> mapToSubpartitionIdx,
            double skewedPartitionMergeFactor) {
        List<IndexRange> subpartitionRanges = new ArrayList<>();

        long tmpSum = 0;
        int startIndex = 0;
        int preSubpartitionIndex = mapToSubpartitionIdx.get(0);

        long limitForSkewedPartition = (long) (limit * skewedPartitionMergeFactor);

        for (int i = 0; i < nums.length; ++i) {
            long num = nums[i];
            int currentSubpartitionIndex = mapToSubpartitionIdx.get(i);
            long mergeLimit =
                    currentSubpartitionIndex == preSubpartitionIndex
                            ? limitForSkewedPartition
                            : limit;
            if (i == startIndex
                    || (tmpSum + num <= mergeLimit
                            && (currentSubpartitionIndex - preSubpartitionIndex + 1)
                                    <= maxRangeSize)) {
                tmpSum += num;
            } else {
                subpartitionRanges.add(new IndexRange(startIndex, i - 1));
                startIndex = i;
                tmpSum = num;
                preSubpartitionIndex = currentSubpartitionIndex;
            }
        }
        subpartitionRanges.add(new IndexRange(startIndex, nums.length - 1));
        return subpartitionRanges;
    }

    private static int computeParallelismForSkewed(
            long[] nums,
            long limit,
            int maxRangeSize,
            Map<Integer, Integer> mapToSubpartitionIdx,
            double skewedPartitionMergeFactor) {

        long tmpSum = 0;
        int startIndex = 0;
        int preSubpartitionIndex = mapToSubpartitionIdx.get(0);

        long limitForSkewedPartition = (long) (limit * skewedPartitionMergeFactor);

        int count = 1;
        for (int i = 0; i < nums.length; ++i) {
            long num = nums[i];
            int currentSubpartitionIndex = mapToSubpartitionIdx.get(i);
            long mergeLimit =
                    currentSubpartitionIndex == preSubpartitionIndex
                            ? limitForSkewedPartition
                            : limit;
            if (i == startIndex
                    || (tmpSum + num <= mergeLimit
                            && (currentSubpartitionIndex - preSubpartitionIndex + 1)
                                    <= maxRangeSize)) {
                tmpSum += num;
            } else {
                ++count;
                startIndex = i;
                tmpSum = num;
                preSubpartitionIndex = currentSubpartitionIndex;
            }
        }
        return count;
    }

    private static int getMaxNumPartitions(List<BlockingResultInfo> consumedResults) {
        checkArgument(!consumedResults.isEmpty());
        return consumedResults.stream()
                .mapToInt(BlockingResultInfo::getNumPartitions)
                .max()
                .getAsInt();
    }

    private static int getMaxNumSubpartitions(List<BlockingResultInfo> consumedResults) {
        checkArgument(!consumedResults.isEmpty());
        return consumedResults.stream()
                .mapToInt(
                        resultInfo ->
                                IntStream.range(0, resultInfo.getNumPartitions())
                                        .boxed()
                                        .mapToInt(resultInfo::getNumSubpartitions)
                                        .sum())
                .max()
                .getAsInt();
    }

    private static List<BlockingResultInfo> getNonBroadcastResultInfos(
            List<BlockingResultInfo> consumedResults) {
        return consumedResults.stream()
                .filter(resultInfo -> !resultInfo.isBroadcast())
                .collect(Collectors.toList());
    }

    static DefaultVertexParallelismAndInputInfosDecider from(
            int maxParallelism, Configuration configuration) {
        return new DefaultVertexParallelismAndInputInfosDecider(
                maxParallelism,
                configuration.get(BatchExecutionOptions.ADAPTIVE_AUTO_PARALLELISM_MIN_PARALLELISM),
                configuration.get(
                        BatchExecutionOptions.ADAPTIVE_AUTO_PARALLELISM_AVG_DATA_VOLUME_PER_TASK),
                configuration.get(
                        BatchExecutionOptions.ADAPTIVE_AUTO_PARALLELISM_DEFAULT_SOURCE_PARALLELISM,
                        maxParallelism),
                configuration.get(BatchExecutionOptions.SKEWED_PARTITION_FACTOR),
                configuration
                        .get(BatchExecutionOptions.SKEWED_PARTITION_THRESHOLD_IN_BYTES)
                        .getBytes(),
                configuration.get(BatchExecutionOptions.SKEWED_PARTITION_MERGE_FACTOR));
    }
}
