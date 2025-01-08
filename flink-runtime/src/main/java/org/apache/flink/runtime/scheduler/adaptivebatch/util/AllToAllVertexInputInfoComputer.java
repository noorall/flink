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

package org.apache.flink.runtime.scheduler.adaptivebatch.util;

import static org.apache.flink.runtime.scheduler.adaptivebatch.DefaultVertexParallelismAndInputInfosDecider.MAX_NUM_SUBPARTITIONS_PER_TASK_CONSUME;
import static org.apache.flink.runtime.scheduler.adaptivebatch.util.SubpartitionSlice.createSubpartitionSlicesByMultiPartitionRanges;
import static org.apache.flink.runtime.scheduler.adaptivebatch.util.VertexParallelismAndInputInfosDeciderUtils.cartesianProduct;
import static org.apache.flink.runtime.scheduler.adaptivebatch.util.VertexParallelismAndInputInfosDeciderUtils.checkAndGetParallelism;
import static org.apache.flink.runtime.scheduler.adaptivebatch.util.VertexParallelismAndInputInfosDeciderUtils.checkAndGetSubpartitionNum;
import static org.apache.flink.runtime.scheduler.adaptivebatch.util.VertexParallelismAndInputInfosDeciderUtils.createdExecutionVertexInputInfosForBroadcast;
import static org.apache.flink.runtime.scheduler.adaptivebatch.util.VertexParallelismAndInputInfosDeciderUtils.createdExecutionVertexInputInfosForNonBroadcast;
import static org.apache.flink.runtime.scheduler.adaptivebatch.util.VertexParallelismAndInputInfosDeciderUtils.getBroadcastInputInfos;
import static org.apache.flink.runtime.scheduler.adaptivebatch.util.VertexParallelismAndInputInfosDeciderUtils.getMaxNumPartitions;
import static org.apache.flink.runtime.scheduler.adaptivebatch.util.VertexParallelismAndInputInfosDeciderUtils.getNonBroadcastInputInfos;
import static org.apache.flink.runtime.scheduler.adaptivebatch.util.VertexParallelismAndInputInfosDeciderUtils.isLegalParallelism;
import static org.apache.flink.runtime.scheduler.adaptivebatch.util.VertexParallelismAndInputInfosDeciderUtils.tryComputeSubpartitionSliceRange;
import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkState;

import org.apache.flink.runtime.executiongraph.ExecutionVertexInputInfo;
import org.apache.flink.runtime.executiongraph.IndexRange;
import org.apache.flink.runtime.executiongraph.JobVertexInputInfo;
import org.apache.flink.runtime.executiongraph.VertexInputInfoComputationUtils;
import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.scheduler.adaptivebatch.BlockingInputInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

/** Helper class that computes VertexInputInfo for all to all like inputs. */
public class AllToAllVertexInputInfoComputer {
    private static final Logger LOG =
            LoggerFactory.getLogger(AllToAllVertexInputInfoComputer.class);

    private final long dataVolumePerTask;
    private final double skewedFactor;
    private final long defaultSkewedThreshold;

    public AllToAllVertexInputInfoComputer(
            long dataVolumePerTask, double skewedFactor, long defaultSkewedThreshold) {
        this.dataVolumePerTask = dataVolumePerTask;
        this.skewedFactor = skewedFactor;
        this.defaultSkewedThreshold = defaultSkewedThreshold;
    }

    /**
     * Decide parallelism and input infos, which will make the data be evenly distributed to
     * downstream subtasks for ALL_TO_ALL, such that different downstream subtasks consume roughly
     * the same amount of data.
     *
     * <p>Assume there are two input infos upstream, each with three partitions and two
     * subpartitions, their data bytes information are: input1: 0->[1,1] 1->[2,2] 2->[3,3], input2:
     * 0->[1,1] 1->[1,1] 2->[1,1]. This method processes the data as follows: <br>
     * 1. Create subpartition slices for inputs with same type number, different from pointwise
     * computer, this method creates subpartition slices by following these steps: Firstly,
     * reorganize the data by subpartition index: input1: {0->[1,2,3],1->[1,2,3]}, input2:
     * {0->[1,1,1],1->[1,1,1]}. Secondly, split subpartitions with the same index into relatively
     * balanced n parts (if possible): {0->[1,2][3],1->[1,2][3]}, {0->[1,1,1],1->[1,1,1]}. Then
     * perform a cartesian product operation to ensure data correctness input1:
     * {0->[1,2],0->[3],1->[1,2],1->[3]}, input2: {0->[1,1,1],0->[1,1,1],1->[1,1,1],1->[1,1,1]},
     * Finally, create subpartition slices base on the result of the previous step. i.e., each input
     * has four balanced subpartition slices.<br>
     * 2. Based on the above subpartition slices, calculate the subpartition slice range each task
     * needs to subscribe to, considering data volume and parallelism constraints:
     * [0,0],[1,1],[2,2],[3,3]<br>
     * 3. Convert the calculated subpartition slice range to the form of partition index range ->
     * subpartition index range:<br>
     * task0: input1: {[0,1]->[0]} input2:{[0,2]->[0]}<br>
     * task1: input1: {[2,2]->[0]} input2:{[0,2]->[0]}<br>
     * task2: input1: {[0,1]->[1]} input2:{[0,2]->[1]}<br>
     * task3: input1: {[2,2]->[1]} input2:{[0,2]->[1]}
     *
     * @param jobVertexId The job vertex id
     * @param inputInfos The information of consumed blocking results
     * @param parallelism The parallelism of the job vertex
     * @param minParallelism the min parallelism
     * @param maxParallelism the max parallelism
     * @return the parallelism and vertex input infos
     */
    public Map<IntermediateDataSetID, JobVertexInputInfo> compute(
            JobVertexID jobVertexId,
            List<BlockingInputInfo> inputInfos,
            int parallelism,
            int minParallelism,
            int maxParallelism) {
        // For inputs without inter-partition and intra-partition correlations, the balanced
        // partitioning result should be consistent with pointwise mode(e.g. rebalanced). We will
        // handle these cases in the final processing step.
        // Note: Currently, there are only two cases of AllToAll's correlation:
        // 1.There is no inter or intra correlation. 2. There must be an inter Correlation
        List<BlockingInputInfo> inputInfosWithoutInterAndIntraCorrelations = new ArrayList<>();
        List<BlockingInputInfo> inputInfosWithInterOrIntraCorrelations = new ArrayList<>();
        for (BlockingInputInfo inputInfo : inputInfos) {
            if (inputInfo.isIntraInputKeyCorrelated() || inputInfo.areInterInputsKeysCorrelated()) {
                inputInfosWithInterOrIntraCorrelations.add(inputInfo);
            } else {
                inputInfosWithoutInterAndIntraCorrelations.add(inputInfo);
            }
        }

        Map<IntermediateDataSetID, JobVertexInputInfo> vertexInputInfos = new HashMap<>();
        if (!inputInfosWithInterOrIntraCorrelations.isEmpty()) {
            vertexInputInfos.putAll(
                    computeJobVertexInputInfosForInputWithInterOrIntraCorrelation(
                            jobVertexId,
                            inputInfosWithInterOrIntraCorrelations,
                            parallelism,
                            minParallelism,
                            maxParallelism));
            // keep the parallelism consistent
            parallelism = checkAndGetParallelism(vertexInputInfos.values());
        }

        if (!inputInfosWithoutInterAndIntraCorrelations.isEmpty()) {
            vertexInputInfos.putAll(
                    computeJobVertexInputInfosForInputWithoutInterAndIntraCorrelations(
                            inputInfosWithoutInterAndIntraCorrelations, parallelism));
        }

        return vertexInputInfos;
    }

    private Map<IntermediateDataSetID, JobVertexInputInfo>
            computeJobVertexInputInfosForInputWithInterOrIntraCorrelation(
                    JobVertexID jobVertexId,
                    List<BlockingInputInfo> inputInfos,
                    int parallelism,
                    int minParallelism,
                    int maxParallelism) {
        checkArgument(!inputInfos.isEmpty());
        List<BlockingInputInfo> nonBroadcastInputInfos = getNonBroadcastInputInfos(inputInfos);
        List<BlockingInputInfo> broadcastInputInfos = getBroadcastInputInfos(inputInfos);
        if (nonBroadcastInputInfos.isEmpty()) {
            LOG.info(
                    "All inputs are broadcast for vertex {}, fallback to num based all to all.",
                    jobVertexId);
            return VertexInputInfoComputationUtils.computeVertexInputInfos(
                    parallelism, inputInfos, true);
        }

        // Divide the data into balanced n parts and describe each part by SubpartitionSlice.
        Map<Integer, List<SubpartitionSlice>> subpartitionSlicesByTypeNumber =
                createSubpartitionSlices(nonBroadcastInputInfos);

        // Distribute the input data evenly among the downstream tasks and record the
        // subpartition slice range for each task.
        Optional<List<IndexRange>> optionalSubpartitionSliceRanges =
                tryComputeSubpartitionSliceRange(
                        minParallelism,
                        maxParallelism,
                        getMaxSubpartitionSliceRangePerTask(nonBroadcastInputInfos),
                        dataVolumePerTask,
                        subpartitionSlicesByTypeNumber);

        if (optionalSubpartitionSliceRanges.isEmpty()) {
            // can't find any legal parallelism, fall back to evenly distribute subpartitions
            LOG.info(
                    "Cannot find a legal parallelism to evenly distribute skewed data for job vertex {}. "
                            + "Fall back to compute a parallelism that can evenly distribute data.",
                    jobVertexId);
            return VertexInputInfoComputationUtils.computeVertexInputInfos(
                    parallelism, inputInfos, true);
        }

        List<IndexRange> subpartitionSliceRanges = optionalSubpartitionSliceRanges.get();

        checkState(
                isLegalParallelism(subpartitionSliceRanges.size(), minParallelism, maxParallelism));

        // Create vertex input info based on the subpartition slice and its range.
        return createJobVertexInputInfos(
                subpartitionSlicesByTypeNumber,
                nonBroadcastInputInfos,
                broadcastInputInfos,
                subpartitionSliceRanges);
    }

    private Map<IntermediateDataSetID, JobVertexInputInfo>
            computeJobVertexInputInfosForInputWithoutInterAndIntraCorrelations(
                    List<BlockingInputInfo> inputInfos, int parallelism) {
        checkArgument(!inputInfos.isEmpty());
        Map<IntermediateDataSetID, JobVertexInputInfo> vertexInputInfos = new HashMap<>();
        for (BlockingInputInfo inputInfo : inputInfos) {
            vertexInputInfos.put(
                    inputInfo.getResultId(),
                    PointwiseVertexInputInfoComputer.computeVertexInputInfo(
                            inputInfo, parallelism, dataVolumePerTask));
        }
        return vertexInputInfos;
    }

    private Map<Integer, List<SubpartitionSlice>> createSubpartitionSlices(
            List<BlockingInputInfo> nonBroadcastInputInfos) {
        // Currently, we consider all inputs in this method to have inter-inputs key correlation,
        checkArgument(
                nonBroadcastInputInfos.stream()
                        .allMatch(BlockingInputInfo::areInterInputsKeysCorrelated));

        int subPartitionNum = checkAndGetSubpartitionNum(nonBroadcastInputInfos);
        // Aggregate input info with the same type number.
        Map<Integer, AggregatedBlockingInputInfo> aggregatedInputInfoByTypeNumber =
                createAggregatedBlockingInputInfos(subPartitionNum, nonBroadcastInputInfos);

        Map<Integer, List<SubpartitionSlice>> subpartitionSliceGroupByTypeNumber = new HashMap<>();
        for (int subpartitionIndex = 0; subpartitionIndex < subPartitionNum; ++subpartitionIndex) {
            // Split the given subpartition group into balanced subpartition slices.
            Map<Integer, List<SubpartitionSlice>> subpartitionSlices =
                    createBalancedSubpartitionSlicesForCorrelatedInputs(
                            subpartitionIndex, aggregatedInputInfoByTypeNumber);

            List<Integer> typeNumberList = new ArrayList<>(subpartitionSlices.keySet());

            List<List<SubpartitionSlice>> originalRangeLists =
                    new ArrayList<>(subpartitionSlices.values());

            // Perform the Cartesian product for inputs with inter-inputs key correlation.
            List<List<SubpartitionSlice>> cartesianProductRangeList =
                    cartesianProduct(originalRangeLists);

            for (List<SubpartitionSlice> subpartitionSlice : cartesianProductRangeList) {
                for (int j = 0; j < subpartitionSlice.size(); ++j) {
                    int typeNumber = typeNumberList.get(j);
                    subpartitionSliceGroupByTypeNumber
                            .computeIfAbsent(typeNumber, ignored -> new ArrayList<>())
                            .add(subpartitionSlice.get(j));
                }
            }
        }

        return subpartitionSliceGroupByTypeNumber;
    }

    private Map<Integer, AggregatedBlockingInputInfo> createAggregatedBlockingInputInfos(
            int subPartitionNum, List<BlockingInputInfo> nonBroadcastInputInfos) {

        Map<Integer, List<BlockingInputInfo>> inputsByTypeNumber =
                nonBroadcastInputInfos.stream()
                        .collect(Collectors.groupingBy(BlockingInputInfo::getInputTypeNumber));

        checkArgument(hasSameIntraInputKeyCorrelation(inputsByTypeNumber));

        Map<Integer, AggregatedBlockingInputInfo> blockingInputInfoContexts = new HashMap<>();
        for (Map.Entry<Integer, List<BlockingInputInfo>> entry : inputsByTypeNumber.entrySet()) {
            Integer typeNumber = entry.getKey();
            List<BlockingInputInfo> inputInfos = entry.getValue();
            blockingInputInfoContexts.put(
                    typeNumber,
                    AggregatedBlockingInputInfo.createAggregatedBlockingInputInfo(
                            defaultSkewedThreshold,
                            skewedFactor,
                            dataVolumePerTask,
                            subPartitionNum,
                            inputInfos));
        }

        return blockingInputInfoContexts;
    }

    private static Map<Integer, List<SubpartitionSlice>>
            createBalancedSubpartitionSlicesForCorrelatedInputs(
                    int subpartitionIndex,
                    Map<Integer, AggregatedBlockingInputInfo> aggregatedInputInfoByTypeNumber) {
        Map<Integer, List<SubpartitionSlice>> subpartitionSlices = new HashMap<>();
        IndexRange subpartitionRange = new IndexRange(subpartitionIndex, subpartitionIndex);
        for (Map.Entry<Integer, AggregatedBlockingInputInfo> entry :
                aggregatedInputInfoByTypeNumber.entrySet()) {
            Integer typeNumber = entry.getKey();
            AggregatedBlockingInputInfo aggregatedBlockingInputInfo = entry.getValue();
            if (aggregatedBlockingInputInfo.isSplittable()
                    && aggregatedBlockingInputInfo.isSkewedSubpartition(subpartitionIndex)) {
                List<IndexRange> partitionRanges =
                        computePartitionRangesEvenlyData(
                                subpartitionIndex,
                                aggregatedBlockingInputInfo.getTargetSize(),
                                aggregatedBlockingInputInfo.getSubpartitionBytesByPartition());
                subpartitionSlices.put(
                        typeNumber,
                        createSubpartitionSlicesByMultiPartitionRanges(
                                partitionRanges,
                                subpartitionRange,
                                aggregatedBlockingInputInfo.getSubpartitionBytesByPartition()));
            } else {
                IndexRange partitionRange =
                        new IndexRange(0, aggregatedBlockingInputInfo.getMaxPartitionNum() - 1);
                subpartitionSlices.put(
                        typeNumber,
                        Collections.singletonList(
                                SubpartitionSlice.createSubpartitionSlice(
                                        partitionRange,
                                        subpartitionRange,
                                        aggregatedBlockingInputInfo.getAggregatedSubpartitionBytes(
                                                subpartitionIndex))));
            }
        }
        return subpartitionSlices;
    }

    /**
     * Splits a group of subpartitions with the same subpartition index into balanced slices based
     * on the target size and returns the corresponding partition ranges.
     *
     * @param subPartitionIndex The index of the subpartition to be split.
     * @param targetSize The target size for each slice.
     * @param subPartitionBytesByPartitionIndex The byte size information of subpartitions in each
     *     partition, with the partition index as the key and the byte array as the value.
     * @return A list of {@link IndexRange} objects representing the partition ranges of each slice.
     */
    private static List<IndexRange> computePartitionRangesEvenlyData(
            int subPartitionIndex,
            long targetSize,
            Map<Integer, long[]> subPartitionBytesByPartitionIndex) {
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

    private static Map<IntermediateDataSetID, JobVertexInputInfo> createJobVertexInputInfos(
            Map<Integer, List<SubpartitionSlice>> subpartitionSlices,
            List<BlockingInputInfo> nonBroadcastInputInfos,
            List<BlockingInputInfo> broadcastInputInfos,
            List<IndexRange> subpartitionSliceRanges) {
        final Map<IntermediateDataSetID, JobVertexInputInfo> vertexInputInfos = new HashMap<>();
        for (BlockingInputInfo inputInfo : nonBroadcastInputInfos) {
            List<ExecutionVertexInputInfo> executionVertexInputInfos =
                    createdExecutionVertexInputInfosForNonBroadcast(
                            inputInfo,
                            subpartitionSliceRanges,
                            subpartitionSlices.get(inputInfo.getInputTypeNumber()));
            vertexInputInfos.put(
                    inputInfo.getResultId(), new JobVertexInputInfo(executionVertexInputInfos));
        }

        for (BlockingInputInfo inputInfo : broadcastInputInfos) {
            List<ExecutionVertexInputInfo> executionVertexInputInfos =
                    createdExecutionVertexInputInfosForBroadcast(
                            inputInfo, subpartitionSliceRanges.size());
            vertexInputInfos.put(
                    inputInfo.getResultId(), new JobVertexInputInfo(executionVertexInputInfos));
        }

        return vertexInputInfos;
    }

    private static boolean hasSameIntraInputKeyCorrelation(
            Map<Integer, List<BlockingInputInfo>> inputGroups) {
        return inputGroups.values().stream()
                .allMatch(
                        inputs ->
                                inputs.stream()
                                                .map(BlockingInputInfo::isIntraInputKeyCorrelated)
                                                .distinct()
                                                .count()
                                        == 1);
    }

    private int getMaxSubpartitionSliceRangePerTask(
            List<BlockingInputInfo> nonBroadcastInputInfos) {
        return MAX_NUM_SUBPARTITIONS_PER_TASK_CONSUME / getMaxNumPartitions(nonBroadcastInputInfos);
    }
}
