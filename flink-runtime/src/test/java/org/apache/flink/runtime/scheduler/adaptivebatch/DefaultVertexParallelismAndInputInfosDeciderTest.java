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

import org.apache.flink.configuration.BatchExecutionOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.runtime.executiongraph.ExecutionVertexInputInfo;
import org.apache.flink.runtime.executiongraph.IndexRange;
import org.apache.flink.runtime.executiongraph.JobVertexInputInfo;
import org.apache.flink.runtime.executiongraph.ParallelismAndInputInfos;
import org.apache.flink.runtime.executiongraph.ResultPartitionBytes;
import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;
import org.apache.flink.runtime.jobgraph.JobVertexID;

import org.apache.flink.shaded.guava31.com.google.common.collect.Iterables;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.flink.util.Preconditions.checkState;
import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link DefaultVertexParallelismAndInputInfosDecider}. */
class DefaultVertexParallelismAndInputInfosDeciderTest {

    private static final long BYTE_256_MB = 256 * 1024 * 1024L;
    private static final long BYTE_512_MB = 512 * 1024 * 1024L;
    private static final long BYTE_1_GB = 1024 * 1024 * 1024L;
    private static final long BYTE_8_GB = 8 * 1024 * 1024 * 1024L;
    private static final long BYTE_1_TB = 1024 * 1024 * 1024 * 1024L;

    private static final int MAX_PARALLELISM = 100;
    private static final int MIN_PARALLELISM = 3;
    private static final int VERTEX_MAX_PARALLELISM = 256;
    private static final int DEFAULT_SOURCE_PARALLELISM = 10;
    private static final long DATA_VOLUME_PER_TASK = 1024 * 1024 * 1024L;

    @Test
    void testDecideParallelism() {
        BlockingResultInfo resultInfo1 = createFromBroadcastResult(BYTE_256_MB);
        BlockingResultInfo resultInfo2 = createFromNonBroadcastResult(BYTE_256_MB + BYTE_8_GB);

        int parallelism =
                createDeciderAndDecideParallelism(Arrays.asList(resultInfo1, resultInfo2));

        assertThat(parallelism).isEqualTo(9);
    }

    @Test
    void testInitiallyNormalizedParallelismIsLargerThanMaxParallelism() {
        BlockingResultInfo resultInfo1 = createFromBroadcastResult(BYTE_256_MB);
        BlockingResultInfo resultInfo2 = createFromNonBroadcastResult(BYTE_8_GB + BYTE_1_TB);

        int parallelism =
                createDeciderAndDecideParallelism(Arrays.asList(resultInfo1, resultInfo2));

        assertThat(parallelism).isEqualTo(MAX_PARALLELISM);
    }

    @Test
    void testInitiallyNormalizedParallelismIsSmallerThanMinParallelism() {
        BlockingResultInfo resultInfo1 = createFromBroadcastResult(BYTE_256_MB);
        BlockingResultInfo resultInfo2 = createFromNonBroadcastResult(BYTE_512_MB);

        int parallelism =
                createDeciderAndDecideParallelism(Arrays.asList(resultInfo1, resultInfo2));

        assertThat(parallelism).isEqualTo(MIN_PARALLELISM);
    }

    @Test
    void testNonBroadcastBytesCanNotDividedEvenly() {
        BlockingResultInfo resultInfo1 = createFromBroadcastResult(BYTE_512_MB);
        BlockingResultInfo resultInfo2 = createFromNonBroadcastResult(BYTE_256_MB + BYTE_8_GB);

        int parallelism =
                createDeciderAndDecideParallelism(Arrays.asList(resultInfo1, resultInfo2));

        assertThat(parallelism).isEqualTo(9);
    }

    @Test
    void testDecideParallelismWithMaxSubpartitionLimitation() {
        BlockingResultInfo resultInfo1 = new TestingBlockingResultInfo(false, 1L, 1024, 1024);
        BlockingResultInfo resultInfo2 = new TestingBlockingResultInfo(false, 1L, 512, 512);

        int parallelism =
                createDeciderAndDecideParallelism(
                        1, 100, BYTE_256_MB, Arrays.asList(resultInfo1, resultInfo2));
        assertThat(parallelism).isEqualTo(32);
    }

    @Test
    void testAllEdgesAllToAll() {
        AllToAllBlockingResultInfo resultInfo1 =
                createAllToAllBlockingResultInfo(
                        new long[] {10L, 15L, 13L, 12L, 1L, 10L, 8L, 20L, 12L, 17L});
        AllToAllBlockingResultInfo resultInfo2 =
                createAllToAllBlockingResultInfo(
                        new long[] {8L, 12L, 21L, 9L, 13L, 7L, 19L, 13L, 14L, 5L});
        ParallelismAndInputInfos parallelismAndInputInfos =
                createDeciderAndDecideParallelismAndInputInfos(
                        1, 10, 60L, Arrays.asList(resultInfo1, resultInfo2));

        assertThat(parallelismAndInputInfos.getParallelism()).isEqualTo(5);
        assertThat(parallelismAndInputInfos.getJobVertexInputInfos()).hasSize(2);

        List<IndexRange> subpartitionRanges =
                Arrays.asList(
                        new IndexRange(0, 1),
                        new IndexRange(2, 3),
                        new IndexRange(4, 6),
                        new IndexRange(7, 8),
                        new IndexRange(9, 9));
        checkAllToAllJobVertexInputInfo(
                parallelismAndInputInfos.getJobVertexInputInfos().get(resultInfo1.getResultId()),
                subpartitionRanges);
        checkAllToAllJobVertexInputInfo(
                parallelismAndInputInfos.getJobVertexInputInfos().get(resultInfo2.getResultId()),
                subpartitionRanges);
    }

    @Test
    void testAllEdgesAllToAllAndDecidedParallelismIsMaxParallelism() {
        AllToAllBlockingResultInfo resultInfo =
                createAllToAllBlockingResultInfo(
                        new long[] {10L, 15L, 13L, 12L, 1L, 10L, 8L, 20L, 12L, 17L});
        ParallelismAndInputInfos parallelismAndInputInfos =
                createDeciderAndDecideParallelismAndInputInfos(
                        1, 2, 10L, Collections.singletonList(resultInfo));

        assertThat(parallelismAndInputInfos.getParallelism()).isEqualTo(2);
        assertThat(parallelismAndInputInfos.getJobVertexInputInfos()).hasSize(1);
        checkAllToAllJobVertexInputInfo(
                Iterables.getOnlyElement(
                        parallelismAndInputInfos.getJobVertexInputInfos().values()),
                Arrays.asList(new IndexRange(0, 5), new IndexRange(6, 9)));
    }

    @Test
    void testAllEdgesAllToAllAndDecidedParallelismIsMinParallelism() {
        AllToAllBlockingResultInfo resultInfo =
                createAllToAllBlockingResultInfo(
                        new long[] {10L, 15L, 13L, 12L, 1L, 10L, 8L, 20L, 12L, 17L});
        ParallelismAndInputInfos parallelismAndInputInfos =
                createDeciderAndDecideParallelismAndInputInfos(
                        4, 10, 1000L, Collections.singletonList(resultInfo));

        assertThat(parallelismAndInputInfos.getParallelism()).isEqualTo(4);
        assertThat(parallelismAndInputInfos.getJobVertexInputInfos()).hasSize(1);
        checkAllToAllJobVertexInputInfo(
                Iterables.getOnlyElement(
                        parallelismAndInputInfos.getJobVertexInputInfos().values()),
                Arrays.asList(
                        new IndexRange(0, 1),
                        new IndexRange(2, 5),
                        new IndexRange(6, 7),
                        new IndexRange(8, 9)));
    }

    @Test
    void testFallBackToEvenlyDistributeSubpartitions() {
        AllToAllBlockingResultInfo resultInfo =
                createAllToAllBlockingResultInfo(
                        new long[] {10L, 1L, 10L, 1L, 10L, 1L, 10L, 1L, 10L, 1L});
        ParallelismAndInputInfos parallelismAndInputInfos =
                createDeciderAndDecideParallelismAndInputInfos(
                        8, 8, 10L, Collections.singletonList(resultInfo));

        assertThat(parallelismAndInputInfos.getParallelism()).isEqualTo(8);
        assertThat(parallelismAndInputInfos.getJobVertexInputInfos()).hasSize(1);
        checkAllToAllJobVertexInputInfo(
                Iterables.getOnlyElement(
                        parallelismAndInputInfos.getJobVertexInputInfos().values()),
                Arrays.asList(
                        new IndexRange(0, 0),
                        new IndexRange(1, 1),
                        new IndexRange(2, 2),
                        new IndexRange(3, 4),
                        new IndexRange(5, 5),
                        new IndexRange(6, 6),
                        new IndexRange(7, 7),
                        new IndexRange(8, 9)));
    }

    @Test
    void testAllEdgesAllToAllAndOneIsBroadcast() {
        AllToAllBlockingResultInfo resultInfo1 =
                createAllToAllBlockingResultInfo(
                        new long[] {10L, 15L, 13L, 12L, 1L, 10L, 8L, 20L, 12L, 17L});
        AllToAllBlockingResultInfo resultInfo2 =
                createAllToAllBlockingResultInfo(new long[] {10L}, true);

        ParallelismAndInputInfos parallelismAndInputInfos =
                createDeciderAndDecideParallelismAndInputInfos(
                        1, 10, 60L, Arrays.asList(resultInfo1, resultInfo2));

        assertThat(parallelismAndInputInfos.getParallelism()).isEqualTo(3);
        assertThat(parallelismAndInputInfos.getJobVertexInputInfos()).hasSize(2);

        checkAllToAllJobVertexInputInfo(
                parallelismAndInputInfos.getJobVertexInputInfos().get(resultInfo1.getResultId()),
                Arrays.asList(new IndexRange(0, 4), new IndexRange(5, 8), new IndexRange(9, 9)));
        checkAllToAllJobVertexInputInfo(
                parallelismAndInputInfos.getJobVertexInputInfos().get(resultInfo2.getResultId()),
                Arrays.asList(new IndexRange(0, 0), new IndexRange(0, 0), new IndexRange(0, 0)));
    }

    @Test
    void testAllEdgesBroadcast() {
        AllToAllBlockingResultInfo resultInfo1 =
                createAllToAllBlockingResultInfo(new long[] {10L}, true);
        AllToAllBlockingResultInfo resultInfo2 =
                createAllToAllBlockingResultInfo(new long[] {10L}, true);
        ParallelismAndInputInfos parallelismAndInputInfos =
                createDeciderAndDecideParallelismAndInputInfos(
                        1, 10, 60L, Arrays.asList(resultInfo1, resultInfo2));

        assertThat(parallelismAndInputInfos.getParallelism()).isOne();
        assertThat(parallelismAndInputInfos.getJobVertexInputInfos()).hasSize(2);

        checkAllToAllJobVertexInputInfo(
                parallelismAndInputInfos.getJobVertexInputInfos().get(resultInfo1.getResultId()),
                Collections.singletonList(new IndexRange(0, 0)));
        checkAllToAllJobVertexInputInfo(
                parallelismAndInputInfos.getJobVertexInputInfos().get(resultInfo2.getResultId()),
                Collections.singletonList(new IndexRange(0, 0)));
    }

    @Test
    void testHavePointwiseEdges() {
        AllToAllBlockingResultInfo resultInfo1 =
                createAllToAllBlockingResultInfo(
                        new long[] {10L, 15L, 13L, 12L, 1L, 10L, 8L, 20L, 12L, 17L});
        PointwiseBlockingResultInfo resultInfo2 =
                createPointwiseBlockingResultInfo(
                        new long[] {8L, 12L, 21L, 9L, 13L}, new long[] {7L, 19L, 13L, 14L, 5L});
        ParallelismAndInputInfos parallelismAndInputInfos =
                createDeciderAndDecideParallelismAndInputInfos(
                        1, 10, 60L, Arrays.asList(resultInfo1, resultInfo2));

        assertThat(parallelismAndInputInfos.getParallelism()).isEqualTo(4);
        assertThat(parallelismAndInputInfos.getJobVertexInputInfos()).hasSize(2);

        checkAllToAllJobVertexInputInfo(
                parallelismAndInputInfos.getJobVertexInputInfos().get(resultInfo1.getResultId()),
                Arrays.asList(
                        new IndexRange(0, 1),
                        new IndexRange(2, 4),
                        new IndexRange(5, 6),
                        new IndexRange(7, 9)));
        checkPointwiseJobVertexInputInfo(
                parallelismAndInputInfos.getJobVertexInputInfos().get(resultInfo2.getResultId()),
                Arrays.asList(
                        new IndexRange(0, 0),
                        new IndexRange(0, 0),
                        new IndexRange(1, 1),
                        new IndexRange(1, 1)),
                Arrays.asList(
                        new IndexRange(0, 1),
                        new IndexRange(2, 4),
                        new IndexRange(0, 1),
                        new IndexRange(2, 4)));
    }

    @Test
    void testParallelismAlreadyDecided() {
        final DefaultVertexParallelismAndInputInfosDecider decider =
                createDecider(MIN_PARALLELISM, MAX_PARALLELISM, DATA_VOLUME_PER_TASK);

        AllToAllBlockingResultInfo allToAllBlockingResultInfo =
                createAllToAllBlockingResultInfo(
                        new long[] {10L, 15L, 13L, 12L, 1L, 10L, 8L, 20L, 12L, 17L});
        ParallelismAndInputInfos parallelismAndInputInfos =
                decider.decideParallelismAndInputInfosForVertex(
                        new JobVertexID(),
                        Collections.singletonList(allToAllBlockingResultInfo),
                        3,
                        MIN_PARALLELISM,
                        MAX_PARALLELISM);

        assertThat(parallelismAndInputInfos.getParallelism()).isEqualTo(3);
        assertThat(parallelismAndInputInfos.getJobVertexInputInfos()).hasSize(1);

        checkAllToAllJobVertexInputInfo(
                Iterables.getOnlyElement(
                        parallelismAndInputInfos.getJobVertexInputInfos().values()),
                Arrays.asList(new IndexRange(0, 2), new IndexRange(3, 5), new IndexRange(6, 9)));
    }

    @Test
    void testSourceJobVertex() {
        ParallelismAndInputInfos parallelismAndInputInfos =
                createDeciderAndDecideParallelismAndInputInfos(
                        MIN_PARALLELISM,
                        MAX_PARALLELISM,
                        DATA_VOLUME_PER_TASK,
                        Collections.emptyList());

        assertThat(parallelismAndInputInfos.getParallelism()).isEqualTo(DEFAULT_SOURCE_PARALLELISM);
        assertThat(parallelismAndInputInfos.getJobVertexInputInfos()).isEmpty();
    }

    @Test
    void testDynamicSourceParallelismWithUpstreamInputs() {
        final DefaultVertexParallelismAndInputInfosDecider decider =
                createDecider(MIN_PARALLELISM, MAX_PARALLELISM, DATA_VOLUME_PER_TASK);

        AllToAllBlockingResultInfo allToAllBlockingResultInfo =
                createAllToAllBlockingResultInfo(
                        new long[] {10L, 15L, 13L, 12L, 1L, 10L, 8L, 20L, 12L, 17L});
        int dynamicSourceParallelism = 4;
        ParallelismAndInputInfos parallelismAndInputInfos =
                decider.decideParallelismAndInputInfosForVertex(
                        new JobVertexID(),
                        Collections.singletonList(allToAllBlockingResultInfo),
                        -1,
                        dynamicSourceParallelism,
                        MAX_PARALLELISM);

        assertThat(parallelismAndInputInfos.getParallelism()).isEqualTo(4);
        assertThat(parallelismAndInputInfos.getJobVertexInputInfos()).hasSize(1);

        checkAllToAllJobVertexInputInfo(
                Iterables.getOnlyElement(
                        parallelismAndInputInfos.getJobVertexInputInfos().values()),
                Arrays.asList(
                        new IndexRange(0, 1),
                        new IndexRange(2, 5),
                        new IndexRange(6, 7),
                        new IndexRange(8, 9)));
    }

    @Test
    void testEvenlyDistributeDataWithMaxSubpartitionLimitation() {
        long[] subpartitionBytes = new long[1024];
        Arrays.fill(subpartitionBytes, 1L);
        AllToAllBlockingResultInfo resultInfo =
                new AllToAllBlockingResultInfo(new IntermediateDataSetID(), 1024, 1024, false);
        for (int i = 0; i < 1024; ++i) {
            resultInfo.recordPartitionInfo(i, new ResultPartitionBytes(subpartitionBytes));
        }

        ParallelismAndInputInfos parallelismAndInputInfos =
                createDeciderAndDecideParallelismAndInputInfos(
                        1, 100, BYTE_256_MB, Collections.singletonList(resultInfo));

        assertThat(parallelismAndInputInfos.getParallelism()).isEqualTo(32);
        List<IndexRange> subpartitionRanges = new ArrayList<>();
        for (int i = 0; i < 32; ++i) {
            subpartitionRanges.add(new IndexRange(i * 32, (i + 1) * 32 - 1));
        }
        checkAllToAllJobVertexInputInfo(
                Iterables.getOnlyElement(
                        parallelismAndInputInfos.getJobVertexInputInfos().values()),
                new IndexRange(0, 1023),
                subpartitionRanges);
    }

    @Test
    void testComputeSourceParallelismUpperBound() {
        Configuration configuration = new Configuration();
        configuration.setInteger(
                BatchExecutionOptions.ADAPTIVE_AUTO_PARALLELISM_DEFAULT_SOURCE_PARALLELISM,
                DEFAULT_SOURCE_PARALLELISM);
        VertexParallelismAndInputInfosDecider vertexParallelismAndInputInfosDecider =
                DefaultVertexParallelismAndInputInfosDecider.from(MAX_PARALLELISM, configuration);
        assertThat(
                        vertexParallelismAndInputInfosDecider.computeSourceParallelismUpperBound(
                                new JobVertexID(), VERTEX_MAX_PARALLELISM))
                .isEqualTo(DEFAULT_SOURCE_PARALLELISM);
    }

    @Test
    void testComputeSourceParallelismUpperBoundFallback() {
        Configuration configuration = new Configuration();
        VertexParallelismAndInputInfosDecider vertexParallelismAndInputInfosDecider =
                DefaultVertexParallelismAndInputInfosDecider.from(MAX_PARALLELISM, configuration);
        assertThat(
                        vertexParallelismAndInputInfosDecider.computeSourceParallelismUpperBound(
                                new JobVertexID(), VERTEX_MAX_PARALLELISM))
                .isEqualTo(MAX_PARALLELISM);
    }

    @Test
    void testComputeSourceParallelismUpperBoundNotExceedMaxParallelism() {
        Configuration configuration = new Configuration();
        configuration.setInteger(
                BatchExecutionOptions.ADAPTIVE_AUTO_PARALLELISM_DEFAULT_SOURCE_PARALLELISM,
                VERTEX_MAX_PARALLELISM * 2);
        VertexParallelismAndInputInfosDecider vertexParallelismAndInputInfosDecider =
                DefaultVertexParallelismAndInputInfosDecider.from(MAX_PARALLELISM, configuration);
        assertThat(
                        vertexParallelismAndInputInfosDecider.computeSourceParallelismUpperBound(
                                new JobVertexID(), VERTEX_MAX_PARALLELISM))
                .isEqualTo(VERTEX_MAX_PARALLELISM);
    }

    @Test
    void testMedianFunction() {
        List<Long> nums1 = Arrays.asList(5L, 3L, 1L, 4L, 2L);
        assertThat(DefaultVertexParallelismAndInputInfosDecider.median(nums1)).isEqualTo(3L);

        List<Long> nums2 = Arrays.asList(5L, 3L, 1L, 4L);
        assertThat(DefaultVertexParallelismAndInputInfosDecider.median(nums2)).isEqualTo(3L);

        List<Long> nums3 = Collections.singletonList(7L);
        assertThat(DefaultVertexParallelismAndInputInfosDecider.median(nums3)).isEqualTo(7L);

        List<Long> nums4 = Arrays.asList(-3L, -1L, -2L, -4L);
        assertThat(DefaultVertexParallelismAndInputInfosDecider.median(nums4)).isEqualTo(1L);

        List<Long> nums5 = Arrays.asList(0L, 2L, 1L);
        assertThat(DefaultVertexParallelismAndInputInfosDecider.median(nums5)).isEqualTo(1L);

        List<Long> nums6 = Arrays.asList(-7L, -2L, -5L);
        assertThat(DefaultVertexParallelismAndInputInfosDecider.median(nums6)).isEqualTo(1L);

        List<Long> nums7 = Arrays.asList(0L, 0L, 0L, 0L);
        assertThat(DefaultVertexParallelismAndInputInfosDecider.median(nums7)).isEqualTo(1L);

        List<Long> nums8 = Arrays.asList(1L, 1L, 2L, 2L, 3L, 3L);
        assertThat(DefaultVertexParallelismAndInputInfosDecider.median(nums8)).isEqualTo(2L);

        List<Long> nums9 = Arrays.asList(1L, 2L, 3L, 4L);
        assertThat(DefaultVertexParallelismAndInputInfosDecider.median(nums9)).isEqualTo(2L);
    }

    @Test
    void testCreatedExecutionVertexInputInfos() {
        BlockingResultInfo blockingResultInfo =
                new AllToAllBlockingResultInfo(new IntermediateDataSetID(), 10, 3, false);
        List<IndexRange> combinedPartitionRanges = new ArrayList<>();
        List<IndexRange> splitPartitions = new ArrayList<>();
        Map<Integer, Integer> mapToSubpartitionIdx = new HashMap<>();
        splitPartitions.add(new IndexRange(0, 4));
        splitPartitions.add(new IndexRange(5, 9));
        splitPartitions.add(new IndexRange(0, 9));
        splitPartitions.add(new IndexRange(0, 9));
        splitPartitions.add(new IndexRange(0, 4));
        splitPartitions.add(new IndexRange(5, 9));
        combinedPartitionRanges.add(new IndexRange(0, 0));
        combinedPartitionRanges.add(new IndexRange(1, 5));
        mapToSubpartitionIdx.put(0, 0);
        mapToSubpartitionIdx.put(1, 0);
        mapToSubpartitionIdx.put(2, 1);
        mapToSubpartitionIdx.put(3, 2);
        mapToSubpartitionIdx.put(4, 3);
        mapToSubpartitionIdx.put(5, 3);
        List<ExecutionVertexInputInfo> executionVertexInputInfos =
                DefaultVertexParallelismAndInputInfosDecider.createdExecutionVertexInputInfos(
                        blockingResultInfo,
                        combinedPartitionRanges,
                        splitPartitions,
                        mapToSubpartitionIdx);
        assertThat(executionVertexInputInfos.size()).isEqualTo(2);
        assertThat(
                        executionVertexInputInfos
                                .get(0)
                                .getSubpartitionIndexRanges(new IndexRange(0, 4)))
                .isEqualTo(new IndexRange(0, 0));
        assertThat(
                        executionVertexInputInfos
                                .get(1)
                                .getSubpartitionIndexRanges(new IndexRange(5, 9)))
                .isEqualTo(new IndexRange(0, 0));
        assertThat(
                        executionVertexInputInfos
                                .get(1)
                                .getSubpartitionIndexRanges(new IndexRange(0, 9)))
                .isEqualTo(new IndexRange(1, 3));
    }

    @Test
    void testSplitSkewPartition() {
        Map<Integer, long[]> subPartitionBytesByPartitionIndex = new HashMap<>();
        long targetSize = 256L;

        subPartitionBytesByPartitionIndex.put(0, new long[] {200, 100});
        subPartitionBytesByPartitionIndex.put(1, new long[] {200, 100});
        subPartitionBytesByPartitionIndex.put(2, new long[] {200, 300});
        subPartitionBytesByPartitionIndex.put(3, new long[] {200, 256});
        subPartitionBytesByPartitionIndex.put(4, new long[] {200, 300});
        List<IndexRange> range1 =
                DefaultVertexParallelismAndInputInfosDecider.splitSkewPartition(
                        subPartitionBytesByPartitionIndex, 1, targetSize);
        List<IndexRange> range2 =
                DefaultVertexParallelismAndInputInfosDecider.splitSkewPartition(
                        subPartitionBytesByPartitionIndex, 0, targetSize);
        assertThat(range1.size()).isEqualTo(4);
        assertThat(range2.size()).isEqualTo(5);
    }

    @Test
    void testDecideParallelismAndEvenlyDistributeSkewedData() {
        Configuration configuration = new Configuration();
        configuration.set(
                BatchExecutionOptions.SKEWED_PARTITION_THRESHOLD_IN_BYTES,
                MemorySize.ofMebiBytes(8));
        configuration.set(BatchExecutionOptions.SKEWED_PARTITION_FACTOR, 2.0);
        MemorySize memorySize = new MemorySize(1200);
        configuration.set(
                BatchExecutionOptions.ADAPTIVE_AUTO_PARALLELISM_AVG_DATA_VOLUME_PER_TASK,
                memorySize);
        DefaultVertexParallelismAndInputInfosDecider vertexParallelismAndInputInfosDecider =
                DefaultVertexParallelismAndInputInfosDecider.from(MAX_PARALLELISM, configuration);

        AllToAllBlockingResultInfo leftResultInfo =
                new AllToAllBlockingResultInfo(
                        new IntermediateDataSetID(), 5, 4, false, false, new HashMap<>());
        AllToAllBlockingResultInfo rightResultInfo =
                new AllToAllBlockingResultInfo(
                        new IntermediateDataSetID(), 5, 4, false, false, new HashMap<>());

        leftResultInfo.markAsSkewed();
        leftResultInfo.recordPartitionInfo(
                0, new ResultPartitionBytes(new long[] {200, 2000, 200, 200}));
        leftResultInfo.recordPartitionInfo(
                1, new ResultPartitionBytes(new long[] {200, 2000, 200, 200}));
        leftResultInfo.recordPartitionInfo(
                2, new ResultPartitionBytes(new long[] {200, 2000, 200, 200}));
        leftResultInfo.recordPartitionInfo(
                3, new ResultPartitionBytes(new long[] {200, 2000, 200, 2000}));
        leftResultInfo.recordPartitionInfo(
                4, new ResultPartitionBytes(new long[] {200, 2000, 200, 2000}));

        rightResultInfo.markAsSkewed();
        rightResultInfo.recordPartitionInfo(
                0, new ResultPartitionBytes(new long[] {200, 200, 2000, 2000}));
        rightResultInfo.recordPartitionInfo(
                1, new ResultPartitionBytes(new long[] {200, 200, 2000, 2000}));
        rightResultInfo.recordPartitionInfo(
                2, new ResultPartitionBytes(new long[] {200, 200, 2000, 200}));
        rightResultInfo.recordPartitionInfo(
                3, new ResultPartitionBytes(new long[] {200, 200, 2000, 200}));
        rightResultInfo.recordPartitionInfo(
                4, new ResultPartitionBytes(new long[] {200, 200, 2000, 200}));

        List<BlockingResultInfo> blockingResultInfos = new ArrayList<>();
        blockingResultInfos.add(leftResultInfo);
        blockingResultInfos.add(rightResultInfo);
        ParallelismAndInputInfos parallelismAndInputInfos =
                vertexParallelismAndInputInfosDecider
                        .decideParallelismAndEvenlyDistributeSkewedData(
                                new JobVertexID(), blockingResultInfos, 5, 5, 10);
        assertThat(parallelismAndInputInfos.getParallelism()).isEqualTo(7);
    }

    private static void checkAllToAllJobVertexInputInfo(
            JobVertexInputInfo jobVertexInputInfo, List<IndexRange> subpartitionRanges) {
        checkAllToAllJobVertexInputInfo(
                jobVertexInputInfo, new IndexRange(0, 0), subpartitionRanges);
    }

    private static void checkAllToAllJobVertexInputInfo(
            JobVertexInputInfo jobVertexInputInfo,
            IndexRange indexRange,
            List<IndexRange> subpartitionRanges) {
        List<ExecutionVertexInputInfo> executionVertexInputInfos = new ArrayList<>();
        for (int i = 0; i < subpartitionRanges.size(); ++i) {
            executionVertexInputInfos.add(
                    new ExecutionVertexInputInfo(i, indexRange, subpartitionRanges.get(i)));
        }
        assertThat(jobVertexInputInfo.getExecutionVertexInputInfos())
                .containsExactlyInAnyOrderElementsOf(executionVertexInputInfos);
    }

    private static void checkPointwiseJobVertexInputInfo(
            JobVertexInputInfo jobVertexInputInfo,
            List<IndexRange> partitionRanges,
            List<IndexRange> subpartitionRanges) {
        assertThat(partitionRanges).hasSameSizeAs(subpartitionRanges);
        List<ExecutionVertexInputInfo> executionVertexInputInfos = new ArrayList<>();
        for (int i = 0; i < subpartitionRanges.size(); ++i) {
            executionVertexInputInfos.add(
                    new ExecutionVertexInputInfo(
                            i, partitionRanges.get(i), subpartitionRanges.get(i)));
        }
        assertThat(jobVertexInputInfo.getExecutionVertexInputInfos())
                .containsExactlyInAnyOrderElementsOf(executionVertexInputInfos);
    }

    static DefaultVertexParallelismAndInputInfosDecider createDecider(
            int minParallelism, int maxParallelism, long dataVolumePerTask) {
        return createDecider(
                minParallelism, maxParallelism, dataVolumePerTask, DEFAULT_SOURCE_PARALLELISM);
    }

    static DefaultVertexParallelismAndInputInfosDecider createDecider(
            int minParallelism,
            int maxParallelism,
            long dataVolumePerTask,
            int defaultSourceParallelism) {
        Configuration configuration = new Configuration();

        configuration.set(
                BatchExecutionOptions.ADAPTIVE_AUTO_PARALLELISM_MIN_PARALLELISM, minParallelism);
        configuration.set(
                BatchExecutionOptions.ADAPTIVE_AUTO_PARALLELISM_AVG_DATA_VOLUME_PER_TASK,
                new MemorySize(dataVolumePerTask));
        configuration.set(
                BatchExecutionOptions.ADAPTIVE_AUTO_PARALLELISM_DEFAULT_SOURCE_PARALLELISM,
                defaultSourceParallelism);

        return DefaultVertexParallelismAndInputInfosDecider.from(maxParallelism, configuration);
    }

    private static int createDeciderAndDecideParallelism(List<BlockingResultInfo> consumedResults) {
        return createDeciderAndDecideParallelism(
                MIN_PARALLELISM, MAX_PARALLELISM, DATA_VOLUME_PER_TASK, consumedResults);
    }

    private static int createDeciderAndDecideParallelism(
            int minParallelism,
            int maxParallelism,
            long dataVolumePerTask,
            List<BlockingResultInfo> consumedResults) {
        final DefaultVertexParallelismAndInputInfosDecider decider =
                createDecider(minParallelism, maxParallelism, dataVolumePerTask);
        return decider.decideParallelism(
                new JobVertexID(), consumedResults, minParallelism, maxParallelism);
    }

    private static ParallelismAndInputInfos createDeciderAndDecideParallelismAndInputInfos(
            int minParallelism,
            int maxParallelism,
            long dataVolumePerTask,
            List<BlockingResultInfo> consumedResults) {
        final DefaultVertexParallelismAndInputInfosDecider decider =
                createDecider(minParallelism, maxParallelism, dataVolumePerTask);
        return decider.decideParallelismAndInputInfosForVertex(
                new JobVertexID(), consumedResults, -1, minParallelism, maxParallelism);
    }

    private AllToAllBlockingResultInfo createAllToAllBlockingResultInfo(
            long[] aggregatedSubpartitionBytes) {
        return createAllToAllBlockingResultInfo(aggregatedSubpartitionBytes, false);
    }

    private AllToAllBlockingResultInfo createAllToAllBlockingResultInfo(
            long[] aggregatedSubpartitionBytes, boolean isBroadcast) {
        // For simplicity, we configure only one partition here, so the aggregatedSubpartitionBytes
        // is equivalent to the subpartition bytes of partition0
        AllToAllBlockingResultInfo resultInfo =
                new AllToAllBlockingResultInfo(
                        new IntermediateDataSetID(),
                        1,
                        aggregatedSubpartitionBytes.length,
                        isBroadcast);
        resultInfo.recordPartitionInfo(0, new ResultPartitionBytes(aggregatedSubpartitionBytes));
        return resultInfo;
    }

    private PointwiseBlockingResultInfo createPointwiseBlockingResultInfo(
            long[]... subpartitionBytesByPartition) {

        final Set<Integer> subpartitionNumSet =
                Arrays.stream(subpartitionBytesByPartition)
                        .map(array -> array.length)
                        .collect(Collectors.toSet());
        // all partitions have the same subpartition num
        checkState(subpartitionNumSet.size() == 1);
        int numSubpartitions = subpartitionNumSet.iterator().next();
        int numPartitions = subpartitionBytesByPartition.length;

        PointwiseBlockingResultInfo resultInfo =
                new PointwiseBlockingResultInfo(
                        new IntermediateDataSetID(), numPartitions, numSubpartitions);

        int partitionIndex = 0;
        for (long[] subpartitionBytes : subpartitionBytesByPartition) {
            resultInfo.recordPartitionInfo(
                    partitionIndex++, new ResultPartitionBytes(subpartitionBytes));
        }

        return resultInfo;
    }

    private static class TestingBlockingResultInfo implements BlockingResultInfo {

        private final boolean isBroadcast;
        private final long producedBytes;
        private final int numPartitions;
        private final int numSubpartitions;

        private TestingBlockingResultInfo(boolean isBroadcast, long producedBytes) {
            this(isBroadcast, producedBytes, MAX_PARALLELISM, MAX_PARALLELISM);
        }

        private TestingBlockingResultInfo(
                boolean isBroadcast, long producedBytes, int numPartitions, int numSubpartitions) {
            this.isBroadcast = isBroadcast;
            this.producedBytes = producedBytes;
            this.numPartitions = numPartitions;
            this.numSubpartitions = numSubpartitions;
        }

        @Override
        public IntermediateDataSetID getResultId() {
            return new IntermediateDataSetID();
        }

        @Override
        public boolean isBroadcast() {
            return isBroadcast;
        }

        @Override
        public boolean isPointwise() {
            return false;
        }

        @Override
        public int getNumPartitions() {
            return numPartitions;
        }

        @Override
        public int getNumSubpartitions(int partitionIndex) {
            return numSubpartitions;
        }

        @Override
        public long getNumBytesProduced() {
            return producedBytes;
        }

        @Override
        public long getNumBytesProduced(
                IndexRange partitionIndexRange, IndexRange subpartitionIndexRange) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void recordPartitionInfo(int partitionIndex, ResultPartitionBytes partitionBytes) {}

        @Override
        public void resetPartitionInfo(int partitionIndex) {}

        @Override
        public Map<Integer, long[]> getSubpartitionBytesByPartitionIndex() {
            return Collections.emptyMap();
        }

        @Override
        public void markAsSkewed() {}

        @Override
        public boolean isSkewed() {
            return false;
        }

        @Override
        public void markAsSplittable() {}

        @Override
        public boolean isSplittable() {
            return false;
        }
    }

    private static BlockingResultInfo createFromBroadcastResult(long producedBytes) {
        return new TestingBlockingResultInfo(true, producedBytes);
    }

    private static BlockingResultInfo createFromNonBroadcastResult(long producedBytes) {
        return new TestingBlockingResultInfo(false, producedBytes);
    }
}
