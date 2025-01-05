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

import org.apache.flink.runtime.executiongraph.IndexRange;
import org.apache.flink.runtime.executiongraph.JobVertexInputInfo;
import org.apache.flink.runtime.executiongraph.VertexInputInfoComputationUtils;
import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;
import org.apache.flink.runtime.scheduler.adaptivebatch.BlockingInputInfo;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.apache.flink.runtime.scheduler.adaptivebatch.util.SubpartitionSlice.createSubpartitionSlice;
import static org.apache.flink.runtime.scheduler.adaptivebatch.util.VertexParallelismAndInputInfosDeciderUtils.createdExecutionVertexInputInfosForNonBroadcast;
import static org.apache.flink.runtime.scheduler.adaptivebatch.util.VertexParallelismAndInputInfosDeciderUtils.isLegalParallelism;
import static org.apache.flink.runtime.scheduler.adaptivebatch.util.VertexParallelismAndInputInfosDeciderUtils.tryComputeSubpartitionSliceRange;
import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkState;

/** Helper class that computes VertexInputInfo for pointwise input. */
public class PointwiseVertexInputInfoComputer {

    private static final Logger LOG =
            LoggerFactory.getLogger(PointwiseVertexInputInfoComputer.class);

    private final long dataVolumePerTask;

    public PointwiseVertexInputInfoComputer(long dataVolumePerTask) {
        this.dataVolumePerTask = dataVolumePerTask;
    }

    /**
     * Computes the input information for a job vertex based on the provided blocking input
     * information and parallelism.
     *
     * @param inputInfos List of blocking input information for the job vertex.
     * @param parallelism Parallelism of the job vertex.
     * @return A map of intermediate data set IDs to their corresponding job vertex input
     *     information.
     */
    public Map<IntermediateDataSetID, JobVertexInputInfo> compute(
            List<BlockingInputInfo> inputInfos, int parallelism) {
        checkArgument(
                inputInfos.stream().noneMatch(BlockingInputInfo::areInterInputsKeysCorrelated));
        Map<IntermediateDataSetID, JobVertexInputInfo> vertexInputInfos = new HashMap<>();
        for (BlockingInputInfo inputInfo : inputInfos) {
            vertexInputInfos.put(
                    inputInfo.getResultId(),
                    computeVertexInputInfo(inputInfo, parallelism, dataVolumePerTask));
        }
        return vertexInputInfos;
    }

    /**
     * Decide parallelism and input infos, which will make the data be evenly distributed to
     * downstream subtasks for POINTWISE, such that different downstream subtasks consume roughly
     * the same amount of data.
     *
     * <p>Assume that `inputInfo` has two partitions, each partition has three subpartitions, their
     * data bytes are: {0->[1,2,1], 1->[2,1,2]}, and the expected parallelism is 3. The calculation
     * process is as follows: <br>
     * 1. Create subpartition slices for input which is composed of several subpartitions. The
     * created slice list and its data bytes are: [1,2,1,2,1,2] <br>
     * 2. Distribute the subpartition slices array into n balanced parts (described by `IndexRange`,
     * named SubpartitionSliceRanges) based on data volume: [0,1],[2,3],[4,5] <br>
     * 3. Reorganize the distributed results into a mapping of partition range to subpartition
     * range: {0 -> [0,1]}, {0->[2,2],1->[0,0]}, {1->[1,2]}. <br>
     * The final result is the `SubpartitionGroup` that each of the three parallel tasks need to
     * subscribe.
     *
     * @param inputInfo The information of consumed blocking results
     * @param parallelism The parallelism of the job vertex
     * @return the vertex input info
     */
    static JobVertexInputInfo computeVertexInputInfo(
            BlockingInputInfo inputInfo, int parallelism, long dataVolumePerTask) {
        List<SubpartitionSlice> subpartitionSlices = createSubpartitionSlices(inputInfo);

        // Node: SubpartitionSliceRanges does not represent the real index of the subpartitions, but
        // the location of that subpartition in all subpartitions, as we aggregate all subpartitions
        // into a one-digit array to calculate.
        Optional<List<IndexRange>> optionalSubpartitionSliceRanges =
                tryComputeSubpartitionSliceRange(
                        parallelism,
                        parallelism,
                        subpartitionSlices.size(),
                        dataVolumePerTask,
                        Map.of(inputInfo.getInputTypeNumber(), subpartitionSlices));

        if (optionalSubpartitionSliceRanges.isEmpty()) {
            LOG.info(
                    "Filed to decide parallelism in balanced way for input {}, fallback to computePartitionOrSubpartitionRangesEvenlySum",
                    inputInfo.getResultId());
            return VertexInputInfoComputationUtils.computeVertexInputInfoForPointwise(
                    inputInfo.getNumPartitions(),
                    parallelism,
                    inputInfo::getNumSubpartitions,
                    true);
        }

        List<IndexRange> subpartitionSliceRanges = optionalSubpartitionSliceRanges.get();

        checkState(isLegalParallelism(subpartitionSliceRanges.size(), parallelism, parallelism));

        // Create vertex input info based on the subpartition slice and ranges.
        return createJobVertexInputInfo(inputInfo, subpartitionSliceRanges, subpartitionSlices);
    }

    private static List<SubpartitionSlice> createSubpartitionSlices(BlockingInputInfo inputInfo) {
        List<SubpartitionSlice> subpartitionSlices = new ArrayList<>();
        if (inputInfo.isIntraInputKeyCorrelated()) {
            // If the input has intra-input correlation, we need to ensure all subpartitions
            // in the same partition index are assigned to the same downstream concurrent task.
            for (int i = 0; i < inputInfo.getNumPartitions(); ++i) {
                IndexRange partitionRange = new IndexRange(i, i);
                IndexRange subpartitionRange =
                        new IndexRange(0, inputInfo.getNumSubpartitions(i) - 1);
                subpartitionSlices.add(
                        createSubpartitionSlice(
                                partitionRange,
                                subpartitionRange,
                                inputInfo.getNumBytesProduced(partitionRange, subpartitionRange)));
            }
        } else {
            for (int i = 0; i < inputInfo.getNumPartitions(); ++i) {
                IndexRange partitionRange = new IndexRange(i, i);
                for (int j = 0; j < inputInfo.getNumSubpartitions(i); ++j) {
                    IndexRange subpartitionRange = new IndexRange(j, j);
                    subpartitionSlices.add(
                            createSubpartitionSlice(
                                    partitionRange,
                                    subpartitionRange,
                                    inputInfo.getNumBytesProduced(
                                            partitionRange, subpartitionRange)));
                }
            }
        }
        return subpartitionSlices;
    }

    private static JobVertexInputInfo createJobVertexInputInfo(
            BlockingInputInfo inputInfo,
            List<IndexRange> subpartitionSliceRanges,
            List<SubpartitionSlice> subpartitionSlices) {
        return new JobVertexInputInfo(
                createdExecutionVertexInputInfosForNonBroadcast(
                        inputInfo, subpartitionSliceRanges, subpartitionSlices));
    }
}
