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

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.configuration.BatchExecutionOptions;
import org.apache.flink.configuration.BatchExecutionOptionsInternal;
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

    private DefaultVertexParallelismAndInputInfosDecider(
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
            List<BlockingInputInfoView> consumedResults,
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

        int parallelism =
                vertexInitialParallelism > 0
                        ? vertexInitialParallelism
                        : decideParallelism(
                                jobVertexId, consumedResults, minParallelism, maxParallelism);

        Map<Boolean, List<BlockingInputInfoView>> inputsGroupByInterCorrelation =
                consumedResults.stream()
                        .collect(
                                Collectors.groupingBy(
                                        BlockingInputInfoView::existInterInputsKeyCorrelation));

        // For AllToAll like inputs, we derive parallelism as a whole, while for Pointwise inputs,
        // we need to derive parallelism separately for each input.
        //
        // In the following cases, we need to reset min parallelism and max parallelism to ensure
        // that the decide parallelism for all inputs is consistent :
        // 1.  Vertex has a specified parallelism
        // 2.  There are edges that don't need to follow intergroup constraint
        if (vertexInitialParallelism > 0 || inputsGroupByInterCorrelation.containsKey(false)) {
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
            List<BlockingInputInfoView> inputsWithoutInterCorrelation =
                    inputsGroupByInterCorrelation.get(false);
            for (BlockingInputInfoView input : inputsWithoutInterCorrelation) {
                if (input.existIntraInputKeyCorrelation()) {
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
                            computeVertexInputInfoForBalancedPointwise(input, parallelism));
                }
            }
        }
        int finalParallelism = checkAndGetParallelism(vertexInputInfoMap.values());

        Map<IntermediateDataSetID, JobVertexInputInfo> vertexInputInfoMapInOrder =
                new LinkedHashMap<>();

        for (BlockingInputInfoView inputInfo : consumedResults) {
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

    /**
     * Decide parallelism and input infos, which will make the data be evenly distributed to
     * downstream subtasks for POINTWISE, such that different downstream subtasks consume roughly
     * the same amount of data.
     *
     * @param inputInfo The information of consumed blocking results
     * @param parallelism The parallelism of the job vertex
     * @return the vertex input info
     */
    private JobVertexInputInfo computeVertexInputInfoForBalancedPointwise(
            BlockingInputInfoView inputInfo, Integer parallelism) {
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
                computeLimitForBalancedPointwise(
                        nums, sum, min, parallelism, MAX_NUM_SUBPARTITIONS_PER_TASK_CONSUME);

        List<IndexRange> combinedPartitionRanges =
                computeSubpartitionRangesEvenlyData(
                        nums, bytesLimit, MAX_NUM_SUBPARTITIONS_PER_TASK_CONSUME);

        if (combinedPartitionRanges.size() != parallelism) {
            LOG.info(
                    "The parallelism {} is not equal to the expected parallelism {}, fallback to computePartitionOrSubpartitionRangesEvenlySum",
                    combinedPartitionRanges.size(),
                    parallelism);
            combinedPartitionRanges = computeSubpartitionRangesEvenlySum(nums.length, parallelism);
        }

        if (combinedPartitionRanges.size() != parallelism) {
            Optional<List<IndexRange>> adjustedSubpartitionRanges =
                    adjustToClosestLegalParallelism(
                            dataVolumePerTask,
                            combinedPartitionRanges.size(),
                            parallelism,
                            parallelism,
                            min,
                            sum,
                            lim ->
                                    computeParallelism(
                                            nums, lim, MAX_NUM_SUBPARTITIONS_PER_TASK_CONSUME),
                            lim ->
                                    computeSubpartitionRangesEvenlyData(
                                            nums, lim, MAX_NUM_SUBPARTITIONS_PER_TASK_CONSUME));
            if (adjustedSubpartitionRanges.isEmpty()) {
                LOG.info(
                        "The parallelism {} is not equal to the expected parallelism {}, fallback to computeVertexInputInfoForPointwise",
                        combinedPartitionRanges.size(),
                        parallelism);
                return VertexInputInfoComputationUtils.computeVertexInputInfoForPointwise(
                        numPartitions, parallelism, consumedResultInfo::getNumSubpartitions, true);
            }
            combinedPartitionRanges = adjustedSubpartitionRanges.get();
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
                        computePartitionRangeForBalancedPointwise(
                                combinedPartitionRanges.get(i), numSubPartitions);
                executionVertexInputInfo = new ExecutionVertexInputInfo(i, mergedPartitionRanges);
            }
            executionVertexInputInfos.add(executionVertexInputInfo);
        }
        return new JobVertexInputInfo(executionVertexInputInfos);
    }

    private long computeLimitForBalancedPointwise(
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

    private static Map<IndexRange, IndexRange> computePartitionRangeForBalancedPointwise(
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

    /**
     * Decide parallelism and input infos, which will make the data be evenly distributed to
     * downstream subtasks for ALL_TO_ALL, such that different downstream subtasks consume roughly
     * the same amount of data.
     *
     * @param jobVertexId The job vertex id
     * @param inputInfos The information of consumed blocking results
     * @param parallelism The parallelism of the job vertex
     * @param minParallelism the min parallelism
     * @param maxParallelism the max parallelism
     * @return the parallelism and vertex input infos
     */
    private Map<IntermediateDataSetID, JobVertexInputInfo>
            computeVertexInputInfoForBalancedAllToAll(
                    JobVertexID jobVertexId,
                    List<BlockingInputInfoView> inputInfos,
                    int parallelism,
                    int minParallelism,
                    int maxParallelism) {
        List<BlockingInputInfoView> nonBroadcastInputInfos = getNonBroadcastInputInfos(inputInfos);
        List<BlockingInputInfoView> broadcastInputInfos = getBroadcastInputInfos(inputInfos);
        if (nonBroadcastInputInfos.isEmpty()) {
            LOG.info(
                    "All inputs are nonBroadcast for vertex {}, fallback to num based all to all.",
                    jobVertexId);
            return VertexInputInfoComputationUtils.computeVertexInputInfos(
                    parallelism, inputInfos, true);
        }
        Map<Integer, List<BlockingInputInfoView>> inputsByTypeNumber =
                nonBroadcastInputInfos.stream()
                        .collect(Collectors.groupingBy(BlockingInputInfoView::getInputTypeNumber));

        checkArgument(isLegalInputGroups(inputsByTypeNumber));

        int subPartitionNum = checkAndGetSubpartitionNum(nonBroadcastInputInfos);

        Map<Integer, List<SubpartitionSlice>> splitSubpartitionGroups =
                divideSubpartitionsEvenlyDistributeData(inputsByTypeNumber, subPartitionNum);

        int subpartitionGroupSize = checkAdnGetSubpartitionGroupSize(splitSubpartitionGroups);

        int maxNumPartitions = getMaxNumPartitions(nonBroadcastInputInfos);
        int maxRangeSize = MAX_NUM_SUBPARTITIONS_PER_TASK_CONSUME / maxNumPartitions;

        // compute subpartition ranges
        List<IndexRange> splitSubpartitionRanges =
                computeSubpartitionSliceRangeForBalancedAllToAll(
                        dataVolumePerTask,
                        maxRangeSize,
                        subpartitionGroupSize,
                        splitSubpartitionGroups);

        // if the parallelism is not legal, adjust to a legal parallelism
        if (!isLegalParallelism(splitSubpartitionRanges.size(), minParallelism, maxParallelism)) {
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
                                    computeSubpartitionSliceRangeForBalancedAllToAll(
                                            limit,
                                            maxRangeSize,
                                            finalSubPartitionNumAfterSplit,
                                            mapToSubpartitionIdx,
                                            splitSubPartitionsBytesByTypeNumber,
                                            splitPartitionRangesByTypeNumber));
            if (adjustedSubpartitionRanges.isEmpty()) {
                // can't find any legal parallelism, fall back to evenly distribute subpartitions
                LOG.info(
                        "Cannot find a legal parallelism to evenly distribute skewed data for job vertex {}. "
                                + "Fall back to compute a parallelism that can evenly distribute data.",
                        jobVertexId);
                return VertexInputInfoComputationUtils.computeVertexInputInfos(
                        parallelism, inputInfos, true);
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

    private Map<Integer, List<SubpartitionSlice>> divideSubpartitionsEvenlyDistributeData(
            Map<Integer, List<BlockingInputInfoView>> inputsByTypeNumber, int subPartitionNum) {
        // The key is the type number, and the value is a map which the key is the partition
        // index and the value contains the size information of the subpartitions.
        Map<Integer, Map<Integer, long[]>> subpartitionBytesByTypeNumber =
                computeSubpartitionBytesByPartitionIndex(inputsByTypeNumber, subPartitionNum);

        Map<Integer, Integer> maxPartitionNumByTypeNumber =
                computeMaxNumPartitionMap(inputsByTypeNumber);

        Map<Integer, long[]> aggregatedSubpartitionBytesByTypeNumber =
                computeAggregatedSubpartitionBytesByTypeNumber(inputsByTypeNumber, subPartitionNum);

        Map<Integer, Boolean> existIntraCorrelationByTypeNumber =
                computeIsExistIntraCorrelationByTypeNumber(inputsByTypeNumber);

        Map<Integer, Long> skewedThresholdByTypeNumber =
                computeSkewedThresholdByTypeNumber(
                        aggregatedSubpartitionBytesByTypeNumber, existIntraCorrelationByTypeNumber);

        Map<Integer, Long> targetSizeByTypeNumber =
                computeTargetSizeByTypeNumber(
                        skewedThresholdByTypeNumber, aggregatedSubpartitionBytesByTypeNumber);

        Map<Integer, List<SubpartitionSlice>> splitPartitionGroupByTypeNumber = new HashMap<>();

        for (int subpartitionIndex = 0; subpartitionIndex < subPartitionNum; ++subpartitionIndex) {
            Map<Integer, List<IndexRange>> partitionRangeByTypeNumber =
                    computePartitionRangeMap(
                            subpartitionBytesByTypeNumber,
                            aggregatedSubpartitionBytesByTypeNumber,
                            existIntraCorrelationByTypeNumber,
                            maxPartitionNumByTypeNumber,
                            skewedThresholdByTypeNumber,
                            targetSizeByTypeNumber,
                            subpartitionIndex);
            List<Integer> typeNumberList = new ArrayList<>(partitionRangeByTypeNumber.keySet());
            List<List<IndexRange>> originalRangeLists =
                    new ArrayList<>(partitionRangeByTypeNumber.values());
            // Perform the Cartesian product for inputs with inter-input key correlation.
            List<List<IndexRange>> cartesianProductRangeList = cartesianProduct(originalRangeLists);
            for (List<IndexRange> splitPartitionRanges : cartesianProductRangeList) {
                for (int j = 0; j < splitPartitionRanges.size(); ++j) {
                    int typeNumber = typeNumberList.get(j);
                    splitPartitionGroupByTypeNumber
                            .computeIfAbsent(typeNumber, ignored -> new ArrayList<>())
                            .add(
                                    SubpartitionSlice.createSubpartitionSlice(
                                            subpartitionIndex,
                                            splitPartitionRanges.get(j),
                                            subpartitionBytesByTypeNumber.get(typeNumber)));
                }
            }
        }
        return splitPartitionGroupByTypeNumber;
    }

    private Map<Integer, Integer> computeMaxNumPartitionMap(
            Map<Integer, List<BlockingInputInfoView>> inputsByTypeNumber) {
        return inputsByTypeNumber.entrySet().stream()
                .collect(
                        Collectors.toMap(
                                Map.Entry::getKey, entry -> getMaxNumPartitions(entry.getValue())));
    }

    private static long[] computeAggregatedSubpartitionBytesByTypeNumber(
            List<BlockingInputInfoView> inputInfos, int subpartitionNum) {
            long[] aggregatedSubpartitionBytes = new long[subpartitionNum];
            for (BlockingInputInfoView inputInfo : inputInfos) {
                List<Long> subpartitionBytes = inputInfo.getAggregatedSubpartitionBytes();
                for (int i = 0; i < subpartitionBytes.size(); i++) {
                    aggregatedSubpartitionBytes[i] += subpartitionBytes.get(i);
                }
            }
            return aggregatedSubpartitionBytes;
    }

    private int checkAdnGetSubpartitionGroupSize(
            Map<Integer, List<SubpartitionSlice>> subpartitionGroups) {
        Set<Integer> subpartitionGroupSizeSet =
                subpartitionGroups.values().stream().map(List::size).collect(Collectors.toSet());
        checkArgument(subpartitionGroupSizeSet.size() == 1);
        return subpartitionGroupSizeSet.iterator().next();
    }

    private static boolean checkAndGetIntraCorrelation(List<BlockingInputInfoView> inputInfos) {
        Set<Boolean> intraCorrelationSet =
                inputInfos.stream()
                        .map(BlockingInputInfoView::existIntraInputKeyCorrelation)
                        .collect(Collectors.toSet());
        checkArgument(intraCorrelationSet.size() == 1);
        return intraCorrelationSet.iterator().next();
    }

    private boolean hasSamePartitionNums(List<BlockingInputInfoView> inputInfos) {
        Set<Integer> intraCorrelationSet =
                inputInfos.stream()
                        .map(BlockingInputInfoView::getNumPartitions)
                        .collect(Collectors.toSet());
        return intraCorrelationSet.size() == 1;
    }

    private Map<Integer, Boolean> computeIsExistIntraCorrelationByTypeNumber(
            Map<Integer, List<BlockingInputInfoView>> inputsByTypeNumber) {
        return inputsByTypeNumber.entrySet().stream()
                .collect(
                        Collectors.toMap(
                                Map.Entry::getKey,
                                entry ->
                                        checkAndGetIntraCorrelation(entry.getValue())
                                                || !hasSamePartitionNums(entry.getValue())));
    }

    private static Map<Integer, long[]> computeSubpartitionBytesByPartitionIndex(
            List<BlockingInputInfoView> inputInfos, int subpartitionNum) {
        Map<Integer, long[]> subpartitionBytesByPartitionIndex = new HashMap<>();
        for (BlockingInputInfoView inputInfo : inputInfos) {
            inputInfo
                    .getSubpartitionBytesByPartitionIndex()
                    .forEach(
                            (partitionIdx, subPartitionBytes) -> {
                                long[] subpartitionBytes =
                                        subpartitionBytesByPartitionIndex.computeIfAbsent(
                                                partitionIdx, v -> new long[subpartitionNum]);
                                for (int i = 0; i < subpartitionNum; i++) {
                                    subpartitionBytes[i] += subPartitionBytes[i];
                                }
                            });
        }
        return subpartitionBytesByPartitionIndex;
    }

    long computeSkewedThreshold(
            long[] aggregatedSubpartitionBytesByTypeNumber,
            boolean existIntraCorrelationByTypeNumber) {
        Map<Integer, Long> skewedThresholdMap = new HashMap<>();
        for (Map.Entry<Integer, long[]> entry :
                aggregatedSubpartitionBytesByTypeNumber.entrySet()) {
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

    Map<Integer, Long> computeTargetSizeByTypeNumber(
            Map<Integer, Long> skewedThresholdByTypeNumber,
            Map<Integer, long[]> aggregatedSubpartitionBytesByTypeNumber) {
        Map<Integer, Long> targetSizeMap = new HashMap<>();
        for (Map.Entry<Integer, Long> entry : skewedThresholdByTypeNumber.entrySet()) {
            Integer typeNumber = entry.getKey();
            Long skewedThreshold = entry.getValue();
            Long targetSize =
                    getTargetSize(
                            aggregatedSubpartitionBytesByTypeNumber.get(typeNumber),
                            skewedThreshold);
            targetSizeMap.put(typeNumber, targetSize);
        }
        return targetSizeMap;
    }

    Map<Integer, List<IndexRange>> computePartitionRangeMap(
            Map<Integer, Map<Integer, long[]>> subpartitionBytesByTypeNumber,
            Map<Integer, long[]> aggregatedSubpartitionBytesByTypeNumber,
            Map<Integer, Boolean> existIntraCorrelationByTypeNumber,
            Map<Integer, Integer> maxPartitionNumByTypeNumber,
            Map<Integer, Long> skewedThresholdByTypeNumber,
            Map<Integer, Long> targetSizeByTypeNumber,
            int subpartitionIndex) {
        Map<Integer, List<SubpartitionSlice>> subpartitionSlices = new HashMap<>();
        Map<Integer, List<IndexRange>> partitionRangeMap = new HashMap<>();
        for (Map.Entry<Integer, long[]> entry :
                aggregatedSubpartitionBytesByTypeNumber.entrySet()) {
            Integer typeNumber = entry.getKey();
            long[] aggregatedSubpartitionBytes = entry.getValue();
            Map<Integer, long[]> subpartitionBytesByPartition = subpartitionBytesByTypeNumber.get(typeNumber);
            boolean existIntraCorrelation = existIntraCorrelationByTypeNumber.get(typeNumber);

            boolean isSkewed = false;
            if (!existIntraCorrelation
                    && !subpartitionBytesByPartition.isEmpty()) {
                long skewedThreshold = skewedThresholdByTypeNumber.get(typeNumber);
                isSkewed = aggregatedSubpartitionBytes[subpartitionIndex] > skewedThreshold;
            }
            List<IndexRange> partitionRanges;
            if (isSkewed) {
                partitionRanges =
                        computePartitionRangesEvenlyData(
                                subpartitionBytesByPartition,
                                subpartitionIndex,
                                targetSizeByTypeNumber.get(typeNumber));
                subpartitionSlices.put(typeNumber,SubpartitionSlice.createSubpartitionSlices(subpartitionIndex,partitionRanges,subpartitionBytesByTypeNumber.get(typeNumber)));
            } else {
                partitionRanges =
                        Collections.singletonList(
                                new IndexRange(0, maxPartitionNumByTypeNumber.get(typeNumber) - 1));
            }
            partitionRangeMap.put(typeNumber, partitionRanges);

            createSubpartitionSlices
        }
        return partitionRangeMap;
    }

    private static <T> List<List<T>> cartesianProduct(List<List<T>> lists) {
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

    private boolean isLegalInputGroups(Map<Integer, List<BlockingInputInfoView>> inputGroups) {
        return inputGroups.values().stream()
                .allMatch(
                        inputs ->
                                inputs.stream()
                                                .map(
                                                        BlockingInputInfoView
                                                                ::existIntraInputKeyCorrelation)
                                                .distinct()
                                                .count()
                                        == 1);
    }

    int decideParallelism(
            JobVertexID jobVertexId,
            List<BlockingInputInfoView> consumedResults,
            int minParallelism,
            int maxParallelism) {
        checkArgument(!consumedResults.isEmpty());

        // Considering that the sizes of broadcast results are usually very small, we compute the
        // parallelism only based on sizes of non-broadcast results
        final List<BlockingInputInfoView> nonBroadcastResults =
                getNonBroadcastInputInfos(consumedResults);
        if (nonBroadcastResults.isEmpty()) {
            return minParallelism;
        }

        long totalBytes =
                nonBroadcastResults.stream()
                        .mapToLong(BlockingInputInfoView::getNumBytesProduced)
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
            int subpartitionIndex,
            IndexRange partitionIndexRange,
            Map<Integer, long[]> subpartitionBytesByPartitionIndex) {
        return IntStream.rangeClosed(
                        partitionIndexRange.getStartIndex(), partitionIndexRange.getEndIndex())
                .mapToLong(i -> subpartitionBytesByPartitionIndex.get(i)[subpartitionIndex])
                .sum();
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
                    List<BlockingInputInfoView> nonBroadcastInputInfos,
                    List<BlockingInputInfoView> broadcastInputInfos,
                    List<IndexRange> splitSubPartitionRanges,
                    Map<Integer, Integer> mapToSubpartitionIdx) {
        final Map<IntermediateDataSetID, JobVertexInputInfo> vertexInputInfos = new HashMap<>();
        for (BlockingInputInfoView inputInfo : nonBroadcastInputInfos) {
            int typeNumber = inputInfo.getInputTypeNumber();
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

        for (BlockingInputInfoView inputInfo : broadcastInputInfos) {
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
            BlockingInputInfoView inputInfo,
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
                executionVertexInputInfo = new ExecutionVertexInputInfo(i, mergedPartitionRanges);
            }
            executionVertexInputInfos.add(executionVertexInputInfo);
        }
        return executionVertexInputInfos;
    }

    // Merge a set of input combinations of PartitionRange and SubPartitionRange, for example, the
    // original description: {<[0,0],[0,3]>, <[1,1],[0,3]>}, will be merged into: {<[0,1],[0,3]>}.
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
            if (mergedRange.isEmpty()) {
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

    private static Optional<IndexRange> mergeTowRange(IndexRange r1, IndexRange r2) {
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

    // This method will reorganize a set of input descriptions to ensure that there are no overlaps
    // between their PartitionRanges. For example, the original description: {<[0,3],[0,0]>,
    // <[0,1],[1,1]>} will be reorganized into: {<[0,1],[0,1]>, <[2,3],[0,0]>}.
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

    private static long median(long[] nums) {
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

    private static boolean isLegalParallelism(
            int parallelism, int minParallelism, int maxParallelism) {
        return parallelism >= minParallelism && parallelism <= maxParallelism;
    }

    private static int checkAndGetSubpartitionNum(List<BlockingInputInfoView> consumedResults) {
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
     * Determine whether there is data skew in the subpartition groups.
     *
     * @param consumedResult the consumed result
     * @param skewedThreshold the skewed threshold
     * @param skewedFactor the skewed factor
     * @return Whether there is data skew in the consumed result.
     */
    public static boolean existSkewSubpartitions(
            BlockingResultInfo consumedResult, long skewedThreshold, double skewedFactor) {
        if (consumedResult.isBroadcast() || consumedResult.isPointwise()) {
            return false;
        }

        long[] subpartitionBytes =
                ((AllToAllBlockingResultInfo) consumedResult)
                        .getAggregatedSubpartitionBytes().stream()
                                .mapToLong(Long::longValue)
                                .toArray();
        long medSize = median(subpartitionBytes);
        long skewThreshold = getSkewThreshold(medSize, skewedThreshold, skewedFactor);

        for (Long subpartitionByte : subpartitionBytes) {
            if (subpartitionByte > skewThreshold) {
                return true;
            }
        }

        return false;
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

    public static List<IndexRange> computePartitionRangesEvenlyData(
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
                splitPartitionRange.add(new IndexRange(startIndex, i - 1));
                startIndex = i;
                tmpSum = num;
            }
        }
        splitPartitionRange.add(new IndexRange(startIndex, partitionNum - 1));
        return splitPartitionRange;
    }

    private static List<IndexRange> computeSubpartitionRangesEvenlyData(
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

    private static List<IndexRange> computeSubpartitionRangesEvenlySum(
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

    private static List<IndexRange> computeSubpartitionSliceRangeForBalancedAllToAll(
            long limit,
            int maxRangeSize,
            int subpartitionGroupSize,
            Map<Integer, List<SubpartitionSlice>> subpartitionSlices) {
        List<IndexRange> subpartitionSliceRanges = new ArrayList<>();
        long totalSize = 0;
        int startIndex = 0;
        Map<Integer, Set<SubpartitionSlice>> bucketsByTypeNumber = new HashMap<>();
        for (int i = 0; i < subpartitionGroupSize; ++i) {
            long currentSize = 0L;
            // Bytes size after deduplication.
            long distinctSize = 0L;
            for (Map.Entry<Integer, List<SubpartitionSlice>> entry :
                    subpartitionSlices.entrySet()) {
                Integer typeNumber = entry.getKey();
                SubpartitionSlice subpartitionSlice = entry.getValue().get(i);
                Set<SubpartitionSlice> bucket =
                        bucketsByTypeNumber.computeIfAbsent(typeNumber, ignored -> new HashSet<>());
                // When the bucket already contains duplicate subpartitionSlices, its size should be
                // ignored.
                if (!bucket.contains(subpartitionSlice)) {
                    distinctSize += subpartitionSlice.getSize();
                }
                currentSize += subpartitionSlice.getSize();
            }
            if (i == startIndex
                    || (totalSize + distinctSize <= limit
                            && (i - startIndex + 1) <= maxRangeSize)) {
                totalSize += distinctSize;
            } else {
                subpartitionSliceRanges.add(new IndexRange(startIndex, i - 1));
                startIndex = i;
                totalSize = currentSize;
                bucketsByTypeNumber.clear();
            }
            for (Map.Entry<Integer, List<SubpartitionSlice>> entry :
                    subpartitionSlices.entrySet()) {
                Integer typeNumber = entry.getKey();
                SubpartitionSlice subpartitionSlice = entry.getValue().get(i);
                bucketsByTypeNumber
                        .computeIfAbsent(typeNumber, ignored -> new HashSet<>())
                        .add(subpartitionSlice);
            }
        }
        subpartitionSliceRanges.add(new IndexRange(startIndex, subpartitionGroupSize - 1));
        return subpartitionSliceRanges;
    }

    private static int computeParallelismForBalancedAllToAll(
            long limit,
            int maxRangeSize,
            int size,
            Map<Integer, Integer> mapToSubpartitionIdx,
            Map<Integer, long[]> splitSubPartitionsBytesByTypeNumber,
            Map<Integer, List<IndexRange>> splitPartitionRangesByTypeNumber) {
        int count = 1;
        int startIndex = 0;
        int preSubpartitionIndex = mapToSubpartitionIdx.get(0);
        long tmpSum = 0;
        Map<Integer, Map<Integer, Set<IndexRange>>> bucketsByTypeNumber = new HashMap<>();
        for (int i = 0; i < size; ++i) {
            long num = 0L;
            long originNum = 0L;
            Integer currentSubpartitionIndex = mapToSubpartitionIdx.get(i);
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

    private static int getMaxNumPartitions(List<BlockingInputInfoView> consumedResults) {
        checkArgument(!consumedResults.isEmpty());
        return consumedResults.stream()
                .mapToInt(BlockingInputInfoView::getNumPartitions)
                .max()
                .getAsInt();
    }

    private static int getMaxNumSubpartitions(List<BlockingInputInfoView> consumedResults) {
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

    private static List<BlockingInputInfoView> getNonBroadcastInputInfos(
            List<BlockingInputInfoView> consumedResults) {
        return consumedResults.stream()
                .filter(resultInfo -> !resultInfo.isBroadcast())
                .collect(Collectors.toList());
    }

    private static List<BlockingInputInfoView> getBroadcastInputInfos(
            List<BlockingInputInfoView> consumedResults) {
        return consumedResults.stream()
                .filter(BlockingInputInfoView::isBroadcast)
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
                configuration.get(
                        BatchExecutionOptionsInternal.ADAPTIVE_SKEWED_OPTIMIZATION_SKEWED_FACTOR),
                configuration
                        .get(
                                BatchExecutionOptionsInternal
                                        .ADAPTIVE_SKEWED_OPTIMIZATION_SKEWED_THRESHOLD)
                        .getBytes());
    }

    private static class SubpartitionSlice {

        int subpartitionIndex;
        IndexRange partitionRange;

        long size;

        public SubpartitionSlice(int subpartitionIndex, IndexRange partitionRange, long size) {
            this.subpartitionIndex = subpartitionIndex;
            this.partitionRange = partitionRange;
            this.size = size;
        }

        public long getSize() {
            return size;
        }

        public static SubpartitionSlice createSubpartitionSlice(
                int subpartitionIndex,
                IndexRange partitionRange,
                Map<Integer, long[]> subpartitionBytesByPartitionIndex) {
            return new SubpartitionSlice(
                    subpartitionIndex,
                    partitionRange,
                    getNumBytesByIndexRange(
                            subpartitionIndex, partitionRange, subpartitionBytesByPartitionIndex));
        }

        public static List<SubpartitionSlice> createSubpartitionSlices(
                int subpartitionIndex,
                List<IndexRange> partitionRanges,
                Map<Integer, long[]> subpartitionBytesByPartitionIndex) {
            List<SubpartitionSlice> subpartitionSlices = new ArrayList<>();
            for (IndexRange partitionRange : partitionRanges) {
                subpartitionSlices.add(
                        createSubpartitionSlice(
                                subpartitionIndex,
                                partitionRange,
                                subpartitionBytesByPartitionIndex));
            }
            return subpartitionSlices;
        }
    }

    private static class BlockingInputInfoContext {
        private final Map<Integer, long[]> subpartitionBytesByPartition;
        private final long[] aggregatedSubpartitionBytes;
        private final int maxPartitionNum;
        private final boolean existIntraInputCorrelation;
        private final long skewedThreshold;
        private final long targetSize;

        public BlockingInputInfoContext(){

        }

        public static BlockingInputInfoContext createBlockingInputInfoContext(List<BlockingInputInfoView> inputInfos, int subPartitionNum) {


            computeSubpartitionBytesByPartitionIndex(inputInfos, subPartitionNum);

            getMaxNumPartitions(inputInfos);

            computeAggregatedSubpartitionBytesByTypeNumber(inputInfos, subPartitionNum);

            checkAndGetIntraCorrelation(inputInfos);

            Map<Integer, Long> skewedThresholdByTypeNumber =
                    computeSkewedThresholdByTypeNumber(
                            aggregatedSubpartitionBytesByTypeNumber, existIntraCorrelationByTypeNumber);

            Map<Integer, Long> targetSizeByTypeNumber =
                    computeTargetSizeByTypeNumber(
                            skewedThresholdByTypeNumber, aggregatedSubpartitionBytesByTypeNumber);
        }
    }
}
