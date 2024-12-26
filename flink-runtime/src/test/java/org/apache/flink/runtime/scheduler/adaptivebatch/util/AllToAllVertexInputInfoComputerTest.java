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

import org.apache.flink.runtime.executiongraph.ExecutionVertexInputInfo;
import org.apache.flink.runtime.executiongraph.IndexRange;
import org.apache.flink.runtime.executiongraph.JobVertexInputInfo;
import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.scheduler.adaptivebatch.BlockingInputInfo;
import org.apache.flink.runtime.scheduler.adaptivebatch.VertexInputInfoComputerTestUtil;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link AllToAllVertexInputInfoComputer}. */
class AllToAllVertexInputInfoComputerTest {

    @Test
    void testComputeAllInputExistIntraInputKeyCorrelation() {
        AllToAllVertexInputInfoComputer computer = getDefaultAllToAllVertexInputInfoComputer();
        List<BlockingInputInfo> inputInfos = new ArrayList<>();
        List<BlockingInputInfo> leftInputInfos =
                createBlockingInputInfos(1, 1, true, 10, List.of(0));
        List<BlockingInputInfo> rightInputInfos =
                createBlockingInputInfos(2, 1, true, 10, List.of());
        inputInfos.addAll(leftInputInfos);
        inputInfos.addAll(rightInputInfos);
        Map<IntermediateDataSetID, JobVertexInputInfo> vertexInputs =
                computer.compute(new JobVertexID(), inputInfos, 10, 1, 10);
        List<Map<IndexRange, IndexRange>> targetConsumedSubpartitionGroups =
                List.of(
                        Map.of(new IndexRange(0, 9), new IndexRange(0, 0)),
                        Map.of(new IndexRange(0, 9), new IndexRange(1, 1)),
                        Map.of(new IndexRange(0, 9), new IndexRange(2, 2)));
        checkJobVertexInputInfo(inputInfos, vertexInputs, 3, targetConsumedSubpartitionGroups);
    }

    @Test
    void testComputeOneSkewedInputNotExistIntraInputKeyCorrelation() {
        AllToAllVertexInputInfoComputer computer = getDefaultAllToAllVertexInputInfoComputer();
        List<BlockingInputInfo> inputInfos = new ArrayList<>();
        List<BlockingInputInfo> leftInputInfos =
                createBlockingInputInfos(1, 1, false, 10, List.of(0));
        List<BlockingInputInfo> rightInputInfos =
                createBlockingInputInfos(2, 1, true, 10, List.of());
        inputInfos.addAll(leftInputInfos);
        inputInfos.addAll(rightInputInfos);
        Map<IntermediateDataSetID, JobVertexInputInfo> vertexInputs =
                computer.compute(new JobVertexID(), inputInfos, 10, 1, 10);
        List<Map<IndexRange, IndexRange>> leftTargetConsumedSubpartitionGroups =
                List.of(
                        Map.of(new IndexRange(0, 1), new IndexRange(0, 0)),
                        Map.of(new IndexRange(2, 3), new IndexRange(0, 0)),
                        Map.of(new IndexRange(4, 5), new IndexRange(0, 0)),
                        Map.of(new IndexRange(6, 7), new IndexRange(0, 0)),
                        Map.of(new IndexRange(8, 9), new IndexRange(0, 0)),
                        Map.of(new IndexRange(0, 9), new IndexRange(1, 1)),
                        Map.of(new IndexRange(0, 9), new IndexRange(2, 2)));
        checkJobVertexInputInfo(
                leftInputInfos, vertexInputs, 7, leftTargetConsumedSubpartitionGroups);
        List<Map<IndexRange, IndexRange>> rightTargetConsumedSubpartitionGroups =
                List.of(
                        Map.of(new IndexRange(0, 9), new IndexRange(0, 0)),
                        Map.of(new IndexRange(0, 9), new IndexRange(0, 0)),
                        Map.of(new IndexRange(0, 9), new IndexRange(0, 0)),
                        Map.of(new IndexRange(0, 9), new IndexRange(0, 0)),
                        Map.of(new IndexRange(0, 9), new IndexRange(0, 0)),
                        Map.of(new IndexRange(0, 9), new IndexRange(1, 1)),
                        Map.of(new IndexRange(0, 9), new IndexRange(2, 2)));
        checkJobVertexInputInfo(
                rightInputInfos, vertexInputs, 7, rightTargetConsumedSubpartitionGroups);
    }

    @Test
    void testComputeAllSkewedInputNotExistIntraInputKeyCorrelation() {
        AllToAllVertexInputInfoComputer computer = getDefaultAllToAllVertexInputInfoComputer();
        List<BlockingInputInfo> inputInfos = new ArrayList<>();
        List<BlockingInputInfo> leftInputInfos =
                createBlockingInputInfos(1, 1, false, 5, List.of(0));
        List<BlockingInputInfo> rightInputInfos =
                createBlockingInputInfos(2, 1, false, 5, List.of(0));
        inputInfos.addAll(leftInputInfos);
        inputInfos.addAll(rightInputInfos);
        Map<IntermediateDataSetID, JobVertexInputInfo> vertexInputs =
                computer.compute(new JobVertexID(), inputInfos, 10, 1, 20);
        List<Map<IndexRange, IndexRange>> targetConsumedSubpartitionGroups =
                List.of(
                        Map.of(new IndexRange(0, 1), new IndexRange(0, 0)),
                        Map.of(new IndexRange(2, 3), new IndexRange(0, 0)),
                        Map.of(new IndexRange(4, 5), new IndexRange(0, 0)),
                        Map.of(new IndexRange(6, 7), new IndexRange(0, 0)),
                        Map.of(new IndexRange(8, 9), new IndexRange(0, 0)),
                        Map.of(new IndexRange(0, 9), new IndexRange(1, 1)),
                        Map.of(new IndexRange(0, 9), new IndexRange(2, 2)));
        checkJobVertexInputInfo(inputInfos, vertexInputs, 7, targetConsumedSubpartitionGroups);
    }

    private void checkJobVertexInputInfo(
            List<BlockingInputInfo> inputInfos,
            Map<IntermediateDataSetID, JobVertexInputInfo> vertexInputInfoMap,
            int targetParallelism,
            List<Map<IndexRange, IndexRange>> targetConsumedSubpartitionGroups) {
        JobVertexInputInfo vertexInputInfo =
                checkAndGetJobVertexInputInfo(inputInfos, vertexInputInfoMap);
        List<ExecutionVertexInputInfo> executionVertexInputInfos =
                vertexInputInfo.getExecutionVertexInputInfos();
        assertThat(executionVertexInputInfos.size()).isEqualTo(targetParallelism);
        for (int i = 0; i < targetParallelism; i++) {
            assertThat(executionVertexInputInfos.get(i).getConsumedSubpartitionGroups())
                    .isEqualTo(targetConsumedSubpartitionGroups.get(i));
        }
    }

    private JobVertexInputInfo checkAndGetJobVertexInputInfo(
            List<BlockingInputInfo> inputInfos,
            Map<IntermediateDataSetID, JobVertexInputInfo> vertexInputInfoMap) {
        List<JobVertexInputInfo> vertexInputInfos =
                inputInfos.stream()
                        .map(inputInfo -> vertexInputInfoMap.get(inputInfo.getResultId()))
                        .collect(Collectors.toList());
        assertThat(vertexInputInfos.size()).isEqualTo(inputInfos.size());
        JobVertexInputInfo baseVertexInputInfo = vertexInputInfos.get(0);
        for (int i = 1; i < vertexInputInfos.size(); i++) {
            assertThat(vertexInputInfos.get(i)).isEqualTo(baseVertexInputInfo);
        }
        return baseVertexInputInfo;
    }

    private static List<BlockingInputInfo> createBlockingInputInfos(
            int typeNumber,
            int numInputInfos,
            boolean existIntraInputKeyCorrelation,
            double skewedFactor,
            List<Integer> skewedSubpartitionIndex) {
        return VertexInputInfoComputerTestUtil.createBlockingInputInfos(
                typeNumber,
                numInputInfos,
                10,
                3,
                existIntraInputKeyCorrelation,
                true,
                1,
                skewedFactor,
                skewedSubpartitionIndex);
    }

    private static AllToAllVertexInputInfoComputer getDefaultAllToAllVertexInputInfoComputer() {
        return new AllToAllVertexInputInfoComputer(10, 4, 10);
    }
}
