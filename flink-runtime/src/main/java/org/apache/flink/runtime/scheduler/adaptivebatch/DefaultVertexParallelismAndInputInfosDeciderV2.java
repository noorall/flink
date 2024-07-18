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
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.Stack;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.LongStream;

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
public class DefaultVertexParallelismAndInputInfosDeciderV2
        implements VertexParallelismAndInputInfosDecider {

    private static final Logger LOG =
            LoggerFactory.getLogger(DefaultVertexParallelismAndInputInfosDeciderV2.class);

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

    private DefaultVertexParallelismAndInputInfosDeciderV2(
            int globalMaxParallelism,
            int globalMinParallelism,
            MemorySize dataVolumePerTask,
            int globalDefaultSourceParallelism,
            double skewedPartitionFactor,
            long skewedPartitionThreshold) {

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

        this.globalMaxParallelism = globalMaxParallelism;
        this.globalMinParallelism = globalMinParallelism;
        this.dataVolumePerTask = dataVolumePerTask.getBytes();
        this.globalDefaultSourceParallelism = globalDefaultSourceParallelism;
        this.skewedPartitionFactor = skewedPartitionFactor;
        this.skewedPartitionThreshold = skewedPartitionThreshold;
    }

    @Override
    public ParallelismAndInputInfos decideParallelismAndInputInfosForVertex(
            JobVertexID jobVertexId,
            List<BlockingInputInfo> inputInfos,
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

        if (inputInfos.isEmpty()) {
            // source job vertex
            int parallelism =
                    vertexInitialParallelism > 0
                            ? vertexInitialParallelism
                            : computeSourceParallelismUpperBound(jobVertexId, vertexMaxParallelism);
            return new ParallelismAndInputInfos(parallelism, Collections.emptyMap());
        }

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

        Map<Boolean, List<BlockingInputInfo>> inputsGroupByInterCorrelation =
                inputInfos.stream()
                        .collect(
                                Collectors.groupingBy(
                                        BlockingInputInfo::existInterInputsCorrelation));

        int parallelism =
                vertexInitialParallelism > 0
                        ? vertexInitialParallelism
                        : decideParallelism(
                                jobVertexId, inputInfos, minParallelism, maxParallelism);

        if (inputsGroupByInterCorrelation.size() == 2
                || inputsGroupByInterCorrelation.containsKey(false)
                || vertexInitialParallelism > 0) {
            minParallelism = parallelism;
            maxParallelism = parallelism;
        }

        Map<IntermediateDataSetID, JobVertexInputInfo> vertexInputInfoMap = new HashMap<>();

        if (inputsGroupByInterCorrelation.containsKey(true)) {
            vertexInputInfoMap.putAll(
                    computeVertexInputInfoForBalancedAllToAll(
                            jobVertexId,
                            inputsGroupByInterCorrelation.get(true),
                            parallelism,
                            minParallelism,
                            maxParallelism));
        }

        if (inputsGroupByInterCorrelation.containsKey(false)) {
            List<BlockingInputInfo> inputsWithoutInterCorrelation =
                    inputsGroupByInterCorrelation.get(false);
            for (BlockingInputInfo input : inputsWithoutInterCorrelation) {
                if (input.existIntraInputCorrelation()) {
                    vertexInputInfoMap.putAll(
                            computeVertexInputInfoForBalancedAllToAll(
                                    jobVertexId,
                                    Collections.singletonList(input),
                                    parallelism,
                                    minParallelism,
                                    maxParallelism));
                } else {
                    vertexInputInfoMap.put(
                            input.getResultId(),
                            computeVertexInputInfoForBalancedPointWise(input, parallelism));
                }
            }
        }
        int finalParallelism = checkAndGetParallelism(vertexInputInfoMap.values());

        Map<IntermediateDataSetID, JobVertexInputInfo> vertexInputInfoMapInOrder =
                new LinkedHashMap<>();

        for (BlockingInputInfo inputInfo : inputInfos) {
            vertexInputInfoMapInOrder.put(
                    inputInfo.getResultId(), vertexInputInfoMap.get(inputInfo.getResultId()));
        }

        return new ParallelismAndInputInfos(finalParallelism, vertexInputInfoMapInOrder);
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

    private JobVertexInputInfo computeVertexInputInfoForBalancedPointWise(
            BlockingInputInfo inputInfo, Integer parallelism) {
        BlockingResultInfo consumedResultInfo = inputInfo.getConsumedResultInfo();
        Map<Integer, long[]> subpartitionBytesByPartitionIndex =
                consumedResultInfo.getSubpartitionBytesByPartitionIndex();
        int numPartitions = inputInfo.getNumPartitions();
        int numSubPartitions = checkAndGetSubpartitionNum(Collections.singletonList(inputInfo));
        long[] nums = new long[numPartitions * numSubPartitions];
        long sum = 0L;
        long min = Integer.MAX_VALUE;
        for (int i = 0; i < numPartitions; ++i) {
            long[] subpartitionBytes = subpartitionBytesByPartitionIndex.get(i);
            for (int j = 0; j < numSubPartitions; ++j) {
                int k = i * numSubPartitions + j;
                nums[k] = subpartitionBytes[j];
                sum += nums[k];
                min = Math.min(nums[k], min);
            }
        }

        long bytesLimit =
                computeLimitForBalancedPointWise(nums, sum, min, parallelism, Integer.MAX_VALUE);

        LOG.info("The limit for per task is {}", bytesLimit);
        List<IndexRange> combinedPartitionRanges =
                computePartitionOrSubpartitionRangesEvenlyData(nums, bytesLimit, Integer.MAX_VALUE);

        if (combinedPartitionRanges.size() != parallelism) {
            LOG.info(
                    "The parallelism {} is not equal to the expected parallelism {}, fallback to computePartitionOrSubpartitionRangesEvenlySum",
                    combinedPartitionRanges.size(),
                    parallelism);
            combinedPartitionRanges =
                    computePartitionOrSubpartitionRangesEvenlySum(nums.length, parallelism);
        }

        if (combinedPartitionRanges.size() != parallelism) {
            Optional<List<IndexRange>> r =
                    adjustToClosestLegalParallelism(
                            dataVolumePerTask,
                            combinedPartitionRanges.size(),
                            parallelism,
                            parallelism,
                            min,
                            sum,
                            lim -> computeParallelism(nums, lim, Integer.MAX_VALUE),
                            lim ->
                                    computePartitionOrSubpartitionRangesEvenlyData(
                                            nums, lim, Integer.MAX_VALUE));
            LOG.info("The adjust result is: {},nums is {}", r, nums);
            LOG.info(
                    "The parallelism {} is not equal to the expected parallelism {}, fallback to computeVertexInputInfoForPointwise",
                    combinedPartitionRanges.size(),
                    parallelism);
            return VertexInputInfoComputationUtils.computeVertexInputInfoForPointwise(
                    numPartitions, parallelism, consumedResultInfo::getNumSubpartitions, true);
        }
        List<ExecutionVertexInputInfo> executionVertexInputInfos = new ArrayList<>();
        for (int i = 0; i < combinedPartitionRanges.size(); ++i) {
            ExecutionVertexInputInfo executionVertexInputInfo;
            if (consumedResultInfo.isBroadcast()) {
                executionVertexInputInfo =
                        new ExecutionVertexInputInfo(
                                i, new IndexRange(0, numPartitions - 1), new IndexRange(0, 0));
            } else {
                Map<IndexRange, IndexRange> mergedPartitionRanges =
                        computePartitionRangeForBalancedPointWise(
                                combinedPartitionRanges.get(i), numSubPartitions);
                if (mergedPartitionRanges.size() > 1) {
                    LOG.info("Input info for Task(balance) {} is {}", i, mergedPartitionRanges);
                }
                executionVertexInputInfo =
                        new ExecutionVertexInputInfo(i, mergedPartitionRanges, true);
            }
            executionVertexInputInfos.add(executionVertexInputInfo);
        }
        return new JobVertexInputInfo(executionVertexInputInfos);
    }

    private long computeLimitForBalancedPointWise(
            long[] nums, long sum, long min, int parallelism, int maxRangeSize) {
        long left = min;
        long right = sum;
        while (left < right) {
            long mid = left + (right - left) / 2;
            int count = computeParallelism(nums, mid, maxRangeSize);
            if (count > parallelism) {
                left = mid + 1;
            } else {
                right = mid;
            }
        }
        return left;
    }

    private static Map<IndexRange, IndexRange> computePartitionRangeForBalancedPointWise(
            IndexRange combinedRange, int numSubPartitions) {
        List<IndexRange> subPartitionRangeList = new ArrayList<>();
        int prePartitionIdx = combinedRange.getStartIndex() / numSubPartitions;
        int start = combinedRange.getStartIndex() % numSubPartitions;
        int end = start;
        for (int i = combinedRange.getStartIndex() + 1; i <= combinedRange.getEndIndex(); ++i) {
            int partitionIdx = i / numSubPartitions;
            if (partitionIdx == prePartitionIdx) {
                ++end;
            } else {
                subPartitionRangeList.add(new IndexRange(start, end));
                prePartitionIdx = partitionIdx;
                start = 0;
                end = start;
            }
        }
        subPartitionRangeList.add(new IndexRange(start, end));

        Map<IndexRange, IndexRange> partitionRangeMap = new LinkedHashMap<>();
        int startPartitionIdx = combinedRange.getStartIndex() / numSubPartitions;
        int endPartitionIdx = startPartitionIdx;
        IndexRange preSubpartitionRange = subPartitionRangeList.get(0);
        for (int i = 1; i < subPartitionRangeList.size(); ++i) {
            IndexRange subPartitionRange = subPartitionRangeList.get(i);
            if (subPartitionRange.equals(preSubpartitionRange)) {
                ++endPartitionIdx;
            } else {
                partitionRangeMap.put(
                        new IndexRange(startPartitionIdx, endPartitionIdx), preSubpartitionRange);
                preSubpartitionRange = subPartitionRange;
                startPartitionIdx = endPartitionIdx + 1;
                endPartitionIdx = startPartitionIdx;
            }
        }
        partitionRangeMap.put(
                new IndexRange(startPartitionIdx, endPartitionIdx), preSubpartitionRange);
        return partitionRangeMap;
    }

    private Map<IntermediateDataSetID, JobVertexInputInfo>
            computeVertexInputInfoForBalancedAllToAll(
                    JobVertexID jobVertexId,
                    List<BlockingInputInfo> inputInfos,
                    int parallelism,
                    int minParallelism,
                    int maxParallelism) {
        List<BlockingInputInfo> nonBroadcastInputInfos = getNonBroadcastInputInfos(inputInfos);
        List<BlockingInputInfo> broadcastInputInfos = getBroadcastInputInfos(inputInfos);
        if (nonBroadcastInputInfos.isEmpty()) {
            LOG.info(
                    "All inputs are nonBroadcast for vertex {}, fallback to num based all to all.",
                    jobVertexId);
            List<BlockingResultInfo> consumedResults =
                    inputInfos.stream()
                            .map(BlockingInputInfo::getConsumedResultInfo)
                            .collect(Collectors.toList());
            return VertexInputInfoComputationUtils.computeVertexInputInfos(
                    parallelism, consumedResults, true);
        }
        Map<Integer, List<BlockingInputInfo>> inputsByTypeNumber =
                nonBroadcastInputInfos.stream()
                        .collect(Collectors.groupingBy(BlockingInputInfo::getInputTypeNumber));

        checkArgument(isLegalGroups(inputsByTypeNumber));

        int subPartitionNum = checkAndGetSubpartitionNum(nonBroadcastInputInfos);

        Map<Integer, Integer> maxPartitionNumByTypeNumber =
                computeMaxNumPartitionMap(inputsByTypeNumber);

        Map<Integer, long[]> subpartitionBytesByTypeNumber =
                computeSubpartitionBytesMap(inputsByTypeNumber, subPartitionNum);

        Map<Integer, Boolean> existIntraCorrelationByTypeNumber =
                computeIsExistIntraCorrelationMap(inputsByTypeNumber);

        Map<Integer, Long> skewedThresholdByTypeNumber =
                computeSkewedThresholdMap(
                        subpartitionBytesByTypeNumber, existIntraCorrelationByTypeNumber);

        Map<Integer, Long> targetSizeByTypeNumber =
                computeTargetSizeMap(skewedThresholdByTypeNumber, subpartitionBytesByTypeNumber);

        Map<Integer, Map<Integer, long[]>> subpartitionBytesByPartitionIndexMap =
                computeSubpartitionBytesByPartitionIndexMap(inputsByTypeNumber, subPartitionNum);

        Map<Integer, List<IndexRange>> splitPartitionRangesByTypeNumber = new HashMap<>();

        // Create a Map between the spilt subPartitions and the original Partitions
        Map<Integer, Integer> mapToSubpartitionIdx = new HashMap<>();

        int subPartitionNumAfterSplit = 0;

        for (int i = 0; i < subPartitionNum; ++i) {
            Map<Integer, List<IndexRange>> partitionRangeByTypeNumber =
                    computePartitionRangeMap(
                            subpartitionBytesByTypeNumber,
                            existIntraCorrelationByTypeNumber,
                            skewedThresholdByTypeNumber,
                            targetSizeByTypeNumber,
                            subpartitionBytesByPartitionIndexMap,
                            maxPartitionNumByTypeNumber,
                            i);

            List<Integer> typeNumberList = new ArrayList<>(partitionRangeByTypeNumber.keySet());
            List<List<IndexRange>> originalRangeLists =
                    new ArrayList<>(partitionRangeByTypeNumber.values());

            List<List<IndexRange>> cartesianProductRangeList = cartesianProduct(originalRangeLists);

            for (List<IndexRange> splitPartitionRanges : cartesianProductRangeList) {
                for (int j = 0; j < splitPartitionRanges.size(); ++j) {
                    int typeNumber = typeNumberList.get(j);
                    splitPartitionRangesByTypeNumber
                            .computeIfAbsent(typeNumber, ignored -> new ArrayList<>())
                            .add(splitPartitionRanges.get(j));
                }
                mapToSubpartitionIdx.put(subPartitionNumAfterSplit, i);
                ++subPartitionNumAfterSplit;
            }
        }

        int maxNumPartitions = getMaxNumPartitions(nonBroadcastInputInfos);
        int maxRangeSize = MAX_NUM_SUBPARTITIONS_PER_TASK_CONSUME / maxNumPartitions;

        long minBytesSize = Long.MAX_VALUE;
        long sumBytesSize = 0L;

        Map<Integer, long[]> splitSubPartitionsBytesByTypeNumber = new HashMap<>();
        int finalSubPartitionNumAfterSplit = subPartitionNumAfterSplit;

        for (int i = 0; i < finalSubPartitionNumAfterSplit; ++i) {
            long total = 0L;
            for (Map.Entry<Integer, Map<Integer, long[]>> entry :
                    subpartitionBytesByPartitionIndexMap.entrySet()) {
                int typeNumber = entry.getKey();
                Map<Integer, long[]> subpartitionBytesByPartitionIndex = entry.getValue();

                IndexRange partitionRange = splitPartitionRangesByTypeNumber.get(typeNumber).get(i);
                int subPartitionIdx = mapToSubpartitionIdx.get(i);
                IndexRange subPartitionRange = new IndexRange(subPartitionIdx, subPartitionIdx);

                long[] bytes =
                        splitSubPartitionsBytesByTypeNumber.computeIfAbsent(
                                typeNumber, ignored -> new long[finalSubPartitionNumAfterSplit]);

                bytes[i] =
                        getNumBytesByIndexRange(
                                subpartitionBytesByPartitionIndex,
                                partitionRange,
                                subPartitionRange);

                total += bytes[i];
            }

            minBytesSize = Math.min(minBytesSize, total);
            sumBytesSize += total;
        }

        // compute subpartition ranges
        List<IndexRange> splitSubpartitionRanges =
                computeSubpartitionRangesForBalancedAllToAll(
                        dataVolumePerTask,
                        maxRangeSize,
                        finalSubPartitionNumAfterSplit,
                        mapToSubpartitionIdx,
                        splitSubPartitionsBytesByTypeNumber,
                        splitPartitionRangesByTypeNumber);

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
                            minBytesSize,
                            sumBytesSize,
                            limit ->
                                    computeParallelismForBalancedAllToAll(
                                            limit,
                                            maxRangeSize,
                                            finalSubPartitionNumAfterSplit,
                                            mapToSubpartitionIdx,
                                            splitSubPartitionsBytesByTypeNumber,
                                            splitPartitionRangesByTypeNumber),
                            limit ->
                                    computeSubpartitionRangesForBalancedAllToAll(
                                            limit,
                                            maxRangeSize,
                                            finalSubPartitionNumAfterSplit,
                                            mapToSubpartitionIdx,
                                            splitSubPartitionsBytesByTypeNumber,
                                            splitPartitionRangesByTypeNumber));
            if (!adjustedSubpartitionRanges.isPresent()) {
                // can't find any legal parallelism, fall back to evenly distribute subpartitions
                LOG.info(
                        "Cannot find a legal parallelism to evenly distribute skewed data for job vertex {}. "
                                + "Fall back to compute a parallelism that can evenly distribute data.",
                        jobVertexId);
                List<BlockingResultInfo> consumedResults =
                        inputInfos.stream()
                                .map(BlockingInputInfo::getConsumedResultInfo)
                                .collect(Collectors.toList());
                return VertexInputInfoComputationUtils.computeVertexInputInfos(
                        parallelism, consumedResults, true);
            }
            splitSubpartitionRanges = adjustedSubpartitionRanges.get();
        }

        checkState(
                isLegalParallelism(splitSubpartitionRanges.size(), minParallelism, maxParallelism));

        return createVertexInputInfosForBalancedAllToAll(
                splitPartitionRangesByTypeNumber,
                nonBroadcastInputInfos,
                broadcastInputInfos,
                splitSubpartitionRanges,
                mapToSubpartitionIdx);
    }

    private Map<Integer, Integer> computeMaxNumPartitionMap(
            Map<Integer, List<BlockingInputInfo>> inputsByTypeNumber) {
        return inputsByTypeNumber.entrySet().stream()
                .collect(
                        Collectors.toMap(
                                Map.Entry::getKey, entry -> getMaxNumPartitions(entry.getValue())));
    }

    private Map<Integer, long[]> computeSubpartitionBytesMap(
            Map<Integer, List<BlockingInputInfo>> inputsByTypeNumber, int subpartitionNum) {
        Map<Integer, long[]> subpartitionBytesMap = new HashMap<>();
        for (Map.Entry<Integer, List<BlockingInputInfo>> entry : inputsByTypeNumber.entrySet()) {
            Integer typeNumber = entry.getKey();
            List<BlockingInputInfo> inputInfos = entry.getValue();
            long[] subpartitionBytes = new long[subpartitionNum];
            for (BlockingInputInfo inputInfo : inputInfos) {
                List<Long> aggSubpartitionBytes =
                        ((AllToAllBlockingResultInfo) inputInfo.getConsumedResultInfo())
                                .getAggregatedSubpartitionBytes();
                for (int i = 0; i < aggSubpartitionBytes.size(); i++) {
                    subpartitionBytes[i] += aggSubpartitionBytes.get(i);
                }
            }
            subpartitionBytesMap.put(typeNumber, subpartitionBytes);
        }
        return subpartitionBytesMap;
    }

    private boolean checkAndGetIntraCorrelation(List<BlockingInputInfo> inputInfos) {
        Set<Boolean> intraCorrelationSet =
                inputInfos.stream()
                        .map(BlockingInputInfo::existIntraInputCorrelation)
                        .collect(Collectors.toSet());
        checkArgument(intraCorrelationSet.size() == 1);
        return intraCorrelationSet.iterator().next();
    }

    private boolean hasSamePartitionNums(List<BlockingInputInfo> inputInfos) {
        Set<Integer> intraCorrelationSet =
                inputInfos.stream()
                        .map(BlockingInputInfo::getNumPartitions)
                        .collect(Collectors.toSet());
        return intraCorrelationSet.size() == 1;
    }

    private Map<Integer, Boolean> computeIsExistIntraCorrelationMap(
            Map<Integer, List<BlockingInputInfo>> inputsByTypeNumber) {
        // TODO: When supporting splitting unions with inconsistent parallelism, modify the
        // implementation here
        return inputsByTypeNumber.entrySet().stream()
                .collect(
                        Collectors.toMap(
                                Map.Entry::getKey,
                                entry ->
                                        checkAndGetIntraCorrelation(entry.getValue())
                                                || !hasSamePartitionNums(entry.getValue())));
    }

    private Map<Integer, Map<Integer, long[]>> computeSubpartitionBytesByPartitionIndexMap(
            Map<Integer, List<BlockingInputInfo>> inputsByTypeNumber, int subpartitionNum) {
        Map<Integer, Map<Integer, long[]>> subpartitionBytesMap = new HashMap<>();
        for (Map.Entry<Integer, List<BlockingInputInfo>> entry : inputsByTypeNumber.entrySet()) {
            Integer typeNumber = entry.getKey();
            List<BlockingInputInfo> inputInfos = entry.getValue();
            Map<Integer, long[]> subPartitionBytesByPartition = new HashMap<>();
            for (BlockingInputInfo inputInfo : inputInfos) {
                inputInfo
                        .getConsumedResultInfo()
                        .getSubpartitionBytesByPartitionIndex()
                        .forEach(
                                (partitionIdx, subPartitionBytes) -> {
                                    long[] aggSubPartitionBytes =
                                            subPartitionBytesByPartition.computeIfAbsent(
                                                    partitionIdx, v -> new long[subpartitionNum]);
                                    for (int i = 0; i < subpartitionNum; i++) {
                                        aggSubPartitionBytes[i] += subPartitionBytes[i];
                                    }
                                });
            }
            subpartitionBytesMap.put(typeNumber, subPartitionBytesByPartition);
        }
        return subpartitionBytesMap;
    }

    Map<Integer, Long> computeSkewedThresholdMap(
            Map<Integer, long[]> subpartitionBytesByTypeNumber,
            Map<Integer, Boolean> existIntraCorrelationByTypeNumber) {
        Map<Integer, Long> skewedThresholdMap = new HashMap<>();
        for (Map.Entry<Integer, long[]> entry : subpartitionBytesByTypeNumber.entrySet()) {
            Integer typeNumber = entry.getKey();
            if (existIntraCorrelationByTypeNumber.get(typeNumber)) {
                continue;
            }
            long[] subpartitionBytes = entry.getValue();
            long medSize = median(subpartitionBytes);
            skewedThresholdMap.put(
                    typeNumber,
                    getSkewThreshold(medSize, skewedPartitionThreshold, skewedPartitionFactor));
        }
        return skewedThresholdMap;
    }

    Map<Integer, Long> computeTargetSizeMap(
            Map<Integer, Long> skewedThresholdByTypeNumber,
            Map<Integer, long[]> subpartitionBytesByTypeNumber) {
        Map<Integer, Long> targetSizeMap = new HashMap<>();
        for (Map.Entry<Integer, Long> entry : skewedThresholdByTypeNumber.entrySet()) {
            Integer typeNumber = entry.getKey();
            Long skewedThreshold = entry.getValue();
            Long targetSize =
                    getTargetSize(subpartitionBytesByTypeNumber.get(typeNumber), skewedThreshold);
            targetSizeMap.put(typeNumber, targetSize);
        }
        return targetSizeMap;
    }

    Map<Integer, List<IndexRange>> computePartitionRangeMap(
            Map<Integer, long[]> subpartitionBytesByTypeNumber,
            Map<Integer, Boolean> existIntraCorrelationByTypeNumber,
            Map<Integer, Long> skewedThresholdByTypeNumber,
            Map<Integer, Long> targetSizeByTypeNumber,
            Map<Integer, Map<Integer, long[]>> subpartitionBytesByPartitionIndexMap,
            Map<Integer, Integer> maxPartitionNumByTypeNumber,
            int subpartitionIndex) {
        Map<Integer, List<IndexRange>> partitionRangeMap = new HashMap<>();
        for (Map.Entry<Integer, long[]> entry : subpartitionBytesByTypeNumber.entrySet()) {
            Integer typeNumber = entry.getKey();
            long[] subpartitionBytes = entry.getValue();

            boolean isSkewed = false;
            if (!existIntraCorrelationByTypeNumber.get(typeNumber)) {
                long skewedThreshold = skewedThresholdByTypeNumber.get(typeNumber);
                isSkewed = subpartitionBytes[subpartitionIndex] > skewedThreshold;
            }
            List<IndexRange> partitionRange;
            if (isSkewed) {
                partitionRange =
                        splitSkewPartition(
                                subpartitionBytesByPartitionIndexMap.get(typeNumber),
                                subpartitionIndex,
                                targetSizeByTypeNumber.get(typeNumber));
            } else {
                partitionRange =
                        Collections.singletonList(
                                new IndexRange(0, maxPartitionNumByTypeNumber.get(typeNumber) - 1));
            }
            partitionRangeMap.put(typeNumber, partitionRange);
        }
        return partitionRangeMap;
    }

    public static <T> List<List<T>> cartesianProduct(List<List<T>> lists) {
        List<List<T>> resultLists = new ArrayList<>();
        if (lists.isEmpty()) {
            resultLists.add(new ArrayList<>());
            return resultLists;
        } else {
            List<T> firstList = lists.get(0);
            List<List<T>> remainingLists = cartesianProduct(lists.subList(1, lists.size()));
            for (T condition : firstList) {
                for (List<T> remainingList : remainingLists) {
                    ArrayList<T> resultList = new ArrayList<>();
                    resultList.add(condition);
                    resultList.addAll(remainingList);
                    resultLists.add(resultList);
                }
            }
        }
        return resultLists;
    }

    private boolean isLegalGroups(Map<Integer, List<BlockingInputInfo>> inputGroups) {
        return inputGroups.values().stream()
                .allMatch(
                        inputs ->
                                inputs.stream()
                                                .map(BlockingInputInfo::existIntraInputCorrelation)
                                                .distinct()
                                                .count()
                                        == 1);
    }

    int decideParallelism(
            JobVertexID jobVertexId,
            List<BlockingInputInfo> inputs,
            int minParallelism,
            int maxParallelism) {
        checkArgument(!inputs.isEmpty());

        // Considering that the sizes of broadcast results are usually very small, we compute the
        // parallelism only based on sizes of non-broadcast results
        final List<BlockingInputInfo> nonBroadcastResults = getNonBroadcastInputInfos(inputs);
        if (nonBroadcastResults.isEmpty()) {
            return minParallelism;
        }

        long totalBytes =
                nonBroadcastResults.stream()
                        .mapToLong(BlockingInputInfo::getNumBytesProduced)
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

    private static long getNumBytesByIndexRange(
            Map<Integer, long[]> subpartitionBytesByPartitionIndex,
            IndexRange partitionIndexRange,
            IndexRange subpartitionIndexRange) {
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

    private static Optional<IndexRange> adjustToLegalIndexRange(
            IndexRange originRange, int numPartitions) {
        if (originRange.getStartIndex() < numPartitions
                && originRange.getEndIndex() < numPartitions) {
            return Optional.of(originRange);
        } else if (originRange.getStartIndex() < numPartitions
                && originRange.getEndIndex() >= numPartitions) {
            return Optional.of(new IndexRange(originRange.getStartIndex(), numPartitions - 1));
        } else {
            return Optional.empty();
        }
    }

    private static Map<IntermediateDataSetID, JobVertexInputInfo>
            createVertexInputInfosForBalancedAllToAll(
                    Map<Integer, List<IndexRange>> splitPartitionRangesByTypeNumber,
                    List<BlockingInputInfo> nonBroadcastInputInfos,
                    List<BlockingInputInfo> broadcastInputInfos,
                    List<IndexRange> splitSubPartitionRanges,
                    Map<Integer, Integer> mapToSubpartitionIdx) {
        final Map<IntermediateDataSetID, JobVertexInputInfo> vertexInputInfos = new HashMap<>();
        for (BlockingInputInfo inputInfo : nonBroadcastInputInfos) {
            int typeNumber = inputInfo.getInputTypeNumber();
            // TODO: modify this part
            List<IndexRange> splitPartitionRanges =
                    splitPartitionRangesByTypeNumber.get(typeNumber).stream()
                            .map(
                                    range ->
                                            adjustToLegalIndexRange(
                                                    range, inputInfo.getNumPartitions()))
                            .filter(Optional::isPresent)
                            .map(Optional::get)
                            .collect(Collectors.toList());

            List<ExecutionVertexInputInfo> executionVertexInputInfos =
                    createdExecutionVertexInputInfos(
                            inputInfo,
                            splitSubPartitionRanges,
                            splitPartitionRanges,
                            mapToSubpartitionIdx);

            vertexInputInfos.put(
                    inputInfo.getResultId(), new JobVertexInputInfo(executionVertexInputInfos));
        }

        for (BlockingInputInfo inputInfo : broadcastInputInfos) {
            List<ExecutionVertexInputInfo> executionVertexInputInfos =
                    createdExecutionVertexInputInfos(
                            inputInfo,
                            splitSubPartitionRanges,
                            Collections.emptyList(),
                            mapToSubpartitionIdx);
            vertexInputInfos.put(
                    inputInfo.getResultId(), new JobVertexInputInfo(executionVertexInputInfos));
        }

        return vertexInputInfos;
    }

    public static List<ExecutionVertexInputInfo> createdExecutionVertexInputInfos(
            BlockingInputInfo inputInfo,
            List<IndexRange> combinedPartitionRanges,
            List<IndexRange> splitPartitionRanges,
            Map<Integer, Integer> mapToSubpartitionIdx) {
        int sourceParallelism = inputInfo.getNumPartitions();
        List<ExecutionVertexInputInfo> executionVertexInputInfos = new ArrayList<>();
        for (int i = 0; i < combinedPartitionRanges.size(); ++i) {
            ExecutionVertexInputInfo executionVertexInputInfo;
            if (inputInfo.isBroadcast()) {
                executionVertexInputInfo =
                        new ExecutionVertexInputInfo(
                                i, new IndexRange(0, sourceParallelism - 1), new IndexRange(0, 0));
            } else {
                IndexRange splitSubpartitionRange = combinedPartitionRanges.get(i);
                Map<IndexRange, IndexRange> mergedPartitionRanges =
                        mergePartitionRanges(
                                splitSubpartitionRange, splitPartitionRanges, mapToSubpartitionIdx);
                if (mergedPartitionRanges.size() > 1) {
                    LOG.info("Input info for Task {} is {}", i, mergedPartitionRanges);
                }
                executionVertexInputInfo =
                        new ExecutionVertexInputInfo(i, mergedPartitionRanges, true);
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

        Map<Integer, Stack<IndexRange>> subPartitionToPartitionIdxMap = new TreeMap<>();
        for (int i = combinedPartitionRange.getStartIndex();
                i <= combinedPartitionRange.getEndIndex();
                i++) {
            IndexRange newRange = splitPartitions.get(i);
            int subPartitionIdx = mapToSubpartitionIdx.get(i);
            if (!subPartitionToPartitionIdxMap.containsKey(subPartitionIdx)) {
                Stack<IndexRange> newStack = new Stack<>();
                newStack.add(new IndexRange(newRange.getStartIndex(), newRange.getEndIndex()));
                subPartitionToPartitionIdxMap.put(subPartitionIdx, newStack);
                continue;
            }
            Stack<IndexRange> rangeStack = subPartitionToPartitionIdxMap.get(subPartitionIdx);
            IndexRange oldRange = rangeStack.pop();
            Optional<IndexRange> mergedRange = mergeTowRange(oldRange, newRange);
            if (!mergedRange.isPresent()) {
                rangeStack.add(oldRange);
                rangeStack.add(newRange);
                continue;
            }
            if (rangeStack.empty()) {
                rangeStack.add(mergedRange.get());
                continue;
            }
            oldRange = rangeStack.pop();
            Optional<IndexRange> mergedRange2 = mergeTowRange(oldRange, mergedRange.get());
            if (mergedRange2.isPresent()) {
                rangeStack.add(mergedRange2.get());
            } else {
                rangeStack.add(oldRange);
                rangeStack.add(mergedRange.get());
            }
        }
        int startIdx = startSubpartitionIdx;
        Stack<IndexRange> preRangeStack = subPartitionToPartitionIdxMap.get(startIdx);

        Map<IndexRange, IndexRange> mergedPartitionRanges = new LinkedHashMap<>();

        for (int i = startSubpartitionIdx + 1; i <= endSubpartitionIdx; ++i) {
            Stack<IndexRange> rangeStack = subPartitionToPartitionIdxMap.get(i);
            if (preRangeStack.equals(rangeStack)) {
                continue;
            }
            checkArgument(preRangeStack.size() == 1);
            mergedPartitionRanges.put(preRangeStack.pop(), new IndexRange(startIdx, i - 1));
            preRangeStack = rangeStack;
            startIdx = i;
        }

        mergedPartitionRanges.put(
                preRangeStack.pop(), new IndexRange(startIdx, endSubpartitionIdx));
        if (!preRangeStack.empty()) {
            mergedPartitionRanges.put(
                    preRangeStack.pop(), new IndexRange(startIdx, endSubpartitionIdx));
        }

        return reorganizePartitionRange(mergedPartitionRanges);
    }

    public static Optional<IndexRange> mergeTowRange(IndexRange r1, IndexRange r2) {
        if (r1.getStartIndex() > r2.getStartIndex()) {
            IndexRange tmp = r1;
            r1 = r2;
            r2 = tmp;
        }
        if (r1.getEndIndex() + 1 >= r2.getStartIndex()) {
            return Optional.of(
                    new IndexRange(
                            r1.getStartIndex(), Math.max(r1.getEndIndex(), r2.getEndIndex())));
        }
        return Optional.empty();
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
                constructSubpartitionIndexRange(newPartitionRange, mergedPartitionRanges)
                        .ifPresent(
                                range -> reorganizedPartitionRange.put(newPartitionRange, range));
            }
            prev = curr + 1;
        }
        return reorganizedPartitionRange;
    }

    private static Optional<IndexRange> constructSubpartitionIndexRange(
            IndexRange partitionIndexRange, Map<IndexRange, IndexRange> mergedPartitionRanges) {
        int subPartitionStartIndex = Integer.MAX_VALUE;
        int subPartitionEndIndex = Integer.MIN_VALUE;
        for (Map.Entry<IndexRange, IndexRange> entry : mergedPartitionRanges.entrySet()) {
            IndexRange oldPartitionRange = entry.getKey();
            IndexRange oldSubPartitionRange = entry.getValue();
            if (oldPartitionRange.getStartIndex() <= partitionIndexRange.getStartIndex()
                    && oldPartitionRange.getEndIndex() >= partitionIndexRange.getEndIndex()) {
                subPartitionStartIndex =
                        Math.min(oldSubPartitionRange.getStartIndex(), subPartitionStartIndex);
                subPartitionEndIndex =
                        Math.max(oldSubPartitionRange.getEndIndex(), subPartitionEndIndex);
            }
        }
        if (subPartitionStartIndex != Integer.MAX_VALUE
                || subPartitionEndIndex != Integer.MIN_VALUE) {
            return Optional.of(new IndexRange(subPartitionStartIndex, subPartitionEndIndex));
        }
        return Optional.empty();
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

    @Deprecated
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

    @VisibleForTesting
    public static long median(long[] nums) {
        int len = nums.length;
        long[] sortedNums = LongStream.of(nums).sorted().toArray();
        if (len % 2 == 0) {
            return Math.max((sortedNums[len / 2] + sortedNums[len / 2 - 1]) / 2, 1L);
        } else {
            return Math.max(sortedNums[len / 2], 1L);
        }
    }

    private static long getSkewThreshold(
            long medSize, long skewedPartitionThreshold, double skewedPartitionFactor) {
        return (long) Math.max(skewedPartitionThreshold, medSize * skewedPartitionFactor);
    }

    private long getTargetSize(long[] subpartitionBytes, long skewedThreshold) {
        long[] nonSkewPartitions =
                LongStream.of(subpartitionBytes).filter(v -> v <= skewedThreshold).toArray();
        if (nonSkewPartitions.length == 0) {
            return dataVolumePerTask;
        } else {
            return Math.max(
                    dataVolumePerTask,
                    LongStream.of(nonSkewPartitions).sum() / nonSkewPartitions.length);
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

    private static int checkAndGetSubpartitionNum(List<BlockingInputInfo> inputInfos) {
        final Set<Integer> subpartitionNumSet =
                inputInfos.stream()
                        .flatMap(
                                inputInfo ->
                                        IntStream.range(0, inputInfo.getNumPartitions())
                                                .boxed()
                                                .map(inputInfo::getNumSubpartitions))
                        .collect(Collectors.toSet());
        // all partitions have the same subpartition num
        checkState(subpartitionNumSet.size() == 1);
        return subpartitionNumSet.iterator().next();
    }

    private static int checkAndGetParallelism(Collection<JobVertexInputInfo> vertexInputInfos) {
        final Set<Integer> parallelismSet =
                vertexInputInfos.stream()
                        .map(
                                vertexInputInfo ->
                                        vertexInputInfo.getExecutionVertexInputInfos().size())
                        .collect(Collectors.toSet());
        checkState(parallelismSet.size() == 1);
        return parallelismSet.iterator().next();
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
    public static Optional<List<IndexRange>> adjustToClosestLegalParallelism(
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

    private static List<IndexRange> computePartitionOrSubpartitionRangesEvenlyData(
            long[] nums, long limit, int maxRangeSize) {
        List<IndexRange> ranges = new ArrayList<>();
        long tmpSum = 0;
        int startIndex = 0;
        for (int i = 0; i < nums.length; ++i) {
            long num = nums[i];
            if (i == startIndex
                    || (tmpSum + num <= limit && (i - startIndex + 1) <= maxRangeSize)) {
                tmpSum += num;
            } else {
                ranges.add(new IndexRange(startIndex, i - 1));
                startIndex = i;
                tmpSum = num;
            }
        }
        ranges.add(new IndexRange(startIndex, nums.length - 1));
        return ranges;
    }

    private static List<IndexRange> computePartitionOrSubpartitionRangesEvenlySum(
            int totalSubpartitions, int parallelism) {
        List<IndexRange> ranges = new ArrayList<>();
        int baseSize = totalSubpartitions / parallelism;
        int remainder = totalSubpartitions % parallelism;
        int start = 0;
        for (int i = 0; i < parallelism; i++) {
            int end = start + baseSize - 1;
            if (i < remainder) {
                end += 1;
            }
            ranges.add(new IndexRange(start, end));
            start = end + 1;
        }
        checkArgument(start == totalSubpartitions);
        return ranges;
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

    public static List<IndexRange> computeSubpartitionRangesForBalancedAllToAll(
            long limit,
            int maxRangeSize,
            int size,
            Map<Integer, Integer> mapToSubpartitionIdx,
            Map<Integer, long[]> splitSubPartitionsBytesByTypeNumber,
            Map<Integer, List<IndexRange>> splitPartitionRangesByTypeNumber) {
        List<IndexRange> subpartitionRanges = new ArrayList<>();
        long tmpSum = 0;
        int startIndex = 0;
        int preSubpartitionIndex = mapToSubpartitionIdx.get(0);
        Map<Integer, Map<Integer, Set<IndexRange>>> bucketsByTypeNumber = new HashMap<>();

        for (int i = 0; i < size; ++i) {
            Integer currentSubpartitionIndex = mapToSubpartitionIdx.get(i);

            long num = 0L;
            long originNum = 0L;
            for (Map.Entry<Integer, List<IndexRange>> entry :
                    splitPartitionRangesByTypeNumber.entrySet()) {
                Integer typeNumber = entry.getKey();
                List<IndexRange> partitionRanges = entry.getValue();
                Map<Integer, Set<IndexRange>> bucket =
                        bucketsByTypeNumber.computeIfAbsent(typeNumber, ignored -> new HashMap<>());
                long[] byteSizes = splitSubPartitionsBytesByTypeNumber.get(typeNumber);
                IndexRange partitionRange = partitionRanges.get(i);
                if (!bucket.computeIfAbsent(currentSubpartitionIndex, ignored -> new HashSet<>())
                        .contains(partitionRange)) {
                    num += byteSizes[i];
                }
                originNum += byteSizes[i];
            }

            if (i == startIndex
                    || (tmpSum + num <= limit
                            && (currentSubpartitionIndex - preSubpartitionIndex + 1)
                                    <= maxRangeSize)) {
                tmpSum += num;
            } else {
                subpartitionRanges.add(new IndexRange(startIndex, i - 1));
                startIndex = i;
                tmpSum = originNum;
                preSubpartitionIndex = currentSubpartitionIndex;
                bucketsByTypeNumber.clear();
            }

            for (Map.Entry<Integer, List<IndexRange>> entry :
                    splitPartitionRangesByTypeNumber.entrySet()) {
                Integer typeNumber = entry.getKey();
                List<IndexRange> partitionRanges = entry.getValue();
                Map<Integer, Set<IndexRange>> bucket =
                        bucketsByTypeNumber.computeIfAbsent(typeNumber, ignored -> new HashMap<>());
                IndexRange partitionRange = partitionRanges.get(i);
                bucket.computeIfAbsent(currentSubpartitionIndex, ignored -> new HashSet<>())
                        .add(partitionRange);
            }
        }

        subpartitionRanges.add(new IndexRange(startIndex, size - 1));
        return subpartitionRanges;
    }

    public static int computeParallelismForBalancedAllToAll(
            long limit,
            int maxRangeSize,
            int size,
            Map<Integer, Integer> mapToSubpartitionIdx,
            Map<Integer, long[]> splitSubPartitionsBytesByTypeNumber,
            Map<Integer, List<IndexRange>> splitPartitionRangesByTypeNumber) {
        int count = 1;

        long tmpSum = 0;
        int startIndex = 0;
        int preSubpartitionIndex = mapToSubpartitionIdx.get(0);
        Map<Integer, Map<Integer, Set<IndexRange>>> bucketsByTypeNumber = new HashMap<>();

        for (int i = 0; i < size; ++i) {
            Integer currentSubpartitionIndex = mapToSubpartitionIdx.get(i);

            long num = 0L;
            long originNum = 0L;
            for (Map.Entry<Integer, List<IndexRange>> entry :
                    splitPartitionRangesByTypeNumber.entrySet()) {
                Integer typeNumber = entry.getKey();
                List<IndexRange> partitionRanges = entry.getValue();
                Map<Integer, Set<IndexRange>> bucket =
                        bucketsByTypeNumber.computeIfAbsent(typeNumber, ignored -> new HashMap<>());
                long[] byteSizes = splitSubPartitionsBytesByTypeNumber.get(typeNumber);
                IndexRange partitionRange = partitionRanges.get(i);
                if (!bucket.computeIfAbsent(currentSubpartitionIndex, ignored -> new HashSet<>())
                        .contains(partitionRange)) {
                    num += byteSizes[i];
                }
                originNum += byteSizes[i];
            }

            if (i == startIndex
                    || (tmpSum + num <= limit
                            && (currentSubpartitionIndex - preSubpartitionIndex + 1)
                                    <= maxRangeSize)) {
                tmpSum += num;
            } else {
                ++count;
                startIndex = i;
                tmpSum = originNum;
                preSubpartitionIndex = currentSubpartitionIndex;
                bucketsByTypeNumber.clear();
            }

            for (Map.Entry<Integer, List<IndexRange>> entry :
                    splitPartitionRangesByTypeNumber.entrySet()) {
                Integer typeNumber = entry.getKey();
                List<IndexRange> partitionRanges = entry.getValue();
                Map<Integer, Set<IndexRange>> bucket =
                        bucketsByTypeNumber.computeIfAbsent(typeNumber, ignored -> new HashMap<>());
                IndexRange partitionRange = partitionRanges.get(i);
                bucket.computeIfAbsent(currentSubpartitionIndex, ignored -> new HashSet<>())
                        .add(partitionRange);
            }
        }
        return count;
    }

    private static int getMaxNumPartitions(List<BlockingInputInfo> inputInfos) {
        checkArgument(!inputInfos.isEmpty());
        return inputInfos.stream().mapToInt(BlockingInputInfo::getNumPartitions).max().getAsInt();
    }

    private static int getMaxNumSubpartitions(List<BlockingInputInfo> inputs) {
        checkArgument(!inputs.isEmpty());
        return inputs.stream()
                .mapToInt(
                        info ->
                                IntStream.range(0, info.getNumPartitions())
                                        .boxed()
                                        .mapToInt(info::getNumSubpartitions)
                                        .sum())
                .max()
                .getAsInt();
    }

    private static List<BlockingInputInfo> getNonBroadcastInputInfos(
            List<BlockingInputInfo> inputs) {
        return inputs.stream().filter(input -> !input.isBroadcast()).collect(Collectors.toList());
    }

    private static List<BlockingInputInfo> getBroadcastInputInfos(List<BlockingInputInfo> inputs) {
        return inputs.stream().filter(BlockingInputInfo::isBroadcast).collect(Collectors.toList());
    }

    static DefaultVertexParallelismAndInputInfosDeciderV2 from(
            int maxParallelism, Configuration configuration) {
        return new DefaultVertexParallelismAndInputInfosDeciderV2(
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
                        .getBytes());
    }
}
