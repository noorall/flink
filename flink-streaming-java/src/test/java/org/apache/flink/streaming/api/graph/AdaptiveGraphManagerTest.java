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

package org.apache.flink.streaming.api.graph;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.graph.util.ImmutableStreamEdge;
import org.apache.flink.streaming.api.graph.util.ImmutableStreamGraph;
import org.apache.flink.streaming.api.graph.util.ImmutableStreamNode;
import org.apache.flink.streaming.api.graph.util.StreamEdgeUpdateRequestInfo;
import org.apache.flink.streaming.api.transformations.PartitionTransformation;
import org.apache.flink.streaming.api.transformations.StreamExchangeMode;
import org.apache.flink.streaming.runtime.partitioner.ForwardForConsecutiveHashPartitioner;
import org.apache.flink.streaming.runtime.partitioner.KeyGroupStreamPartitioner;
import org.apache.flink.streaming.runtime.partitioner.RescalePartitioner;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for {@link AdaptiveGraphManager}.
 *
 * <p>ATTENTION: This test is extremely brittle. Do NOT remove, add or re-order test cases.
 */
public class AdaptiveGraphManagerTest extends JobGraphGeneratorTestBase {
    @Override
    JobGraph createJobGraph(StreamGraph streamGraph) {
        return generateJobGraphInLazilyMode(streamGraph);
    }

    @Test
    void testCreateJobVertexLazily() {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStream<Tuple2<String, String>> input =
                env.fromData("a", "b", "c", "d", "e", "f")
                        .map(
                                new MapFunction<String, Tuple2<String, String>>() {

                                    @Override
                                    public Tuple2<String, String> map(String value) {
                                        return new Tuple2<>(value, value);
                                    }
                                });

        DataStream<Tuple2<String, String>> result =
                input.keyBy(0)
                        .map(
                                new MapFunction<Tuple2<String, String>, Tuple2<String, String>>() {

                                    @Override
                                    public Tuple2<String, String> map(
                                            Tuple2<String, String> value) {
                                        return value;
                                    }
                                });

        result.addSink(
                new SinkFunction<Tuple2<String, String>>() {
                    @Override
                    public void invoke(Tuple2<String, String> value) {}
                });
        StreamGraph streamGraph = env.getStreamGraph();

        AdaptiveGraphManager adaptiveGraphManager =
                new AdaptiveGraphManager(
                        Thread.currentThread().getContextClassLoader(),
                        streamGraph,
                        Runnable::run,
                        null);
        JobGraph jobGraph = adaptiveGraphManager.getJobGraph();
        List<JobVertex> jobVertices = jobGraph.getVerticesSortedTopologicallyFromSources();
        assertThat(jobVertices.size()).isEqualTo(1);
        while (!jobVertices.isEmpty()) {
            List<JobVertex> newJobVertices = new ArrayList<>();
            for (JobVertex jobVertex : jobVertices) {
                newJobVertices.addAll(adaptiveGraphManager.onJobVertexFinished(jobVertex.getID()));
            }
            jobVertices = newJobVertices;
        }
        assertThat(jobGraph.getVerticesSortedTopologicallyFromSources().size()).isEqualTo(2);
    }

    @Test
    void testTheCorrectnessOfJobGraph() {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStream<Tuple2<String, String>> input =
                env.fromData("a", "b", "c", "d", "e", "f")
                        .map(
                                new MapFunction<String, Tuple2<String, String>>() {

                                    @Override
                                    public Tuple2<String, String> map(String value) {
                                        return new Tuple2<>(value, value);
                                    }
                                });

        DataStream<Tuple2<String, String>> result =
                input.keyBy(0)
                        .map(
                                new MapFunction<Tuple2<String, String>, Tuple2<String, String>>() {

                                    @Override
                                    public Tuple2<String, String> map(
                                            Tuple2<String, String> value) {
                                        return value;
                                    }
                                });

        result.addSink(
                new SinkFunction<Tuple2<String, String>>() {
                    @Override
                    public void invoke(Tuple2<String, String> value) {}
                });
        StreamGraph streamGraph = env.getStreamGraph();
        JobGraph jobGraph1 = generateJobGraphInLazilyMode(streamGraph);
        JobGraph jobGraph2 = StreamingJobGraphGenerator.createJobGraph(streamGraph);
        assertThat(isJobGraphEquivalent(jobGraph1, jobGraph2)).isEqualTo(true);
    }

    @Test
    void testStreamGraphContext() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // fromElements -> Map -> Print
        DataStream<Integer> sourceDataStream = env.fromData(1, 2, 3);

        DataStream<Integer> partitionAfterSourceDataStream =
                new DataStream<>(
                        env,
                        new PartitionTransformation<>(
                                sourceDataStream.getTransformation(),
                                new RescalePartitioner<>(),
                                StreamExchangeMode.PIPELINED));

        DataStream<Integer> mapDataStream =
                partitionAfterSourceDataStream.map(value -> value).setParallelism(1);

        DataStream<Integer> mapDataStream2 =
                new DataStream<>(
                                env,
                                new PartitionTransformation<>(
                                        mapDataStream.getTransformation(),
                                        new ForwardForConsecutiveHashPartitioner<>(
                                                new KeyGroupStreamPartitioner<>(record -> 0L, 100)),
                                        StreamExchangeMode.PIPELINED))
                        .map(value -> value)
                        .setParallelism(1);

        DataStream<Integer> partitionAfterMapDataStream =
                new DataStream<>(
                        env,
                        new PartitionTransformation<>(
                                mapDataStream2.getTransformation(),
                                new RescalePartitioner<>(),
                                StreamExchangeMode.PIPELINED));

        partitionAfterMapDataStream.print().setParallelism(1);

        StreamGraph streamGraph = env.getStreamGraph();
        streamGraph.setDynamic(true);

        AdaptiveGraphManager adaptiveGraphManager =
                new AdaptiveGraphManager(
                        Thread.currentThread().getContextClassLoader(),
                        streamGraph,
                        Runnable::run,
                        null);

        JobGraph jobGraph = adaptiveGraphManager.getJobGraph();
        StreamGraphContext streamGraphContext = adaptiveGraphManager.getStreamGraphContext();

        List<JobVertex> jobVertices = jobGraph.getVerticesSortedTopologicallyFromSources();

        ImmutableStreamGraph immutableStreamGraph = streamGraphContext.getStreamGraph();
        ImmutableStreamNode targetNode =
                immutableStreamGraph.getStreamNode(
                        immutableStreamGraph
                                .getStreamNode(streamGraph.getSourceIDs().iterator().next())
                                .getOutEdges()
                                .get(0)
                                .getTargetId());
        ImmutableStreamEdge targetEdge = targetNode.getOutEdges().get(0);
        StreamEdgeUpdateRequestInfo streamEdgeUpdateRequestInfo =
                new StreamEdgeUpdateRequestInfo(
                                targetEdge.getId(),
                                targetEdge.getSourceId(),
                                targetEdge.getTargetId())
                        .outputPartitioner(new RescalePartitioner<>());
        assertThat(
                        streamGraphContext.modifyStreamEdge(
                                Collections.singletonList(streamEdgeUpdateRequestInfo)))
                .isEqualTo(true);
        while (!jobVertices.isEmpty()) {
            List<JobVertex> newJobVertices = new ArrayList<>();
            for (JobVertex jobVertex : jobVertices) {
                newJobVertices.addAll(adaptiveGraphManager.onJobVertexFinished(jobVertex.getID()));
            }
            jobVertices = newJobVertices;
        }
        List<JobVertex> sortedJobVertices =
                adaptiveGraphManager.getJobGraph().getVerticesSortedTopologicallyFromSources();
        assertThat(sortedJobVertices.size()).isEqualTo(4);
    }

    private static JobGraph generateJobGraphInLazilyMode(StreamGraph streamGraph) {
        AdaptiveGraphManager adaptiveGraphManager =
                new AdaptiveGraphManager(
                        Thread.currentThread().getContextClassLoader(),
                        streamGraph,
                        Runnable::run,
                        null);
        JobGraph jobGraph = adaptiveGraphManager.getJobGraph();
        List<JobVertex> jobVertices = jobGraph.getVerticesSortedTopologicallyFromSources();

        while (!jobVertices.isEmpty()) {
            List<JobVertex> newJobVertices = new ArrayList<>();
            for (JobVertex jobVertex : jobVertices) {
                newJobVertices.addAll(adaptiveGraphManager.onJobVertexFinished(jobVertex.getID()));
            }
            jobVertices = newJobVertices;
        }
        return jobGraph;
    }

    private static boolean isJobGraphEquivalent(JobGraph jobGraph1, JobGraph jobGraph2) {
        assertThat(jobGraph1.getJobConfiguration()).isEqualTo(jobGraph2.getJobConfiguration());
        assertThat(jobGraph1.getJobType()).isEqualTo(jobGraph2.getJobType());
        assertThat(jobGraph1.isDynamic()).isEqualTo(jobGraph2.isDynamic());
        assertThat(jobGraph1.isApproximateLocalRecoveryEnabled())
                .isEqualTo(jobGraph2.isApproximateLocalRecoveryEnabled());
        assertThat(jobGraph1.getSerializedExecutionConfig())
                .isEqualTo(jobGraph2.getSerializedExecutionConfig());
        assertThat(jobGraph1.getCheckpointingSettings().toString())
                .isEqualTo(jobGraph2.getCheckpointingSettings().toString());
        assertThat(jobGraph1.getSavepointRestoreSettings())
                .isEqualTo(jobGraph2.getSavepointRestoreSettings());
        assertThat(jobGraph1.getUserJars()).isEqualTo(jobGraph2.getUserJars());
        assertThat(jobGraph1.getUserArtifacts()).isEqualTo(jobGraph2.getUserArtifacts());
        assertThat(jobGraph1.getUserJarBlobKeys()).isEqualTo(jobGraph2.getUserJarBlobKeys());
        assertThat(jobGraph1.getClasspaths()).isEqualTo(jobGraph2.getClasspaths());
        assertThat(jobGraph1.getJobStatusHooks()).isEqualTo(jobGraph2.getJobStatusHooks());

        List<JobVertex> vertices1 = jobGraph1.getVerticesSortedTopologicallyFromSources();
        List<JobVertex> vertices2 = jobGraph2.getVerticesSortedTopologicallyFromSources();
        assertThat(vertices1.size()).isEqualTo(vertices2.size());
        for (int i = 1; i < vertices1.size(); i++) {
            JobVertex vertex1 = vertices1.get(i);
            JobVertex vertex2 = vertices2.get(i);
            assertThat(vertex1.getID()).isEqualTo(vertex2.getID());
            assertThat(vertex1.getInputs().size()).isEqualTo(vertex2.getInputs().size());
            assertThat(vertex1.getParallelism()).isEqualTo(vertex2.getParallelism());
            assertThat(vertex1.getMaxParallelism()).isEqualTo(vertex2.getMaxParallelism());
            assertThat(vertex1.getMinResources()).isEqualTo(vertex2.getMinResources());
            assertThat(vertex1.getPreferredResources()).isEqualTo(vertex2.getPreferredResources());
            assertThat(vertex1.getInvokableClassName()).isEqualTo(vertex2.getInvokableClassName());
            assertThat(vertex1.getName()).isEqualTo(vertex2.getName());
            assertThat(vertex1.getOperatorName()).isEqualTo(vertex2.getOperatorName());
            assertThat(vertex1.isSupportsConcurrentExecutionAttempts())
                    .isEqualTo(vertex2.isSupportsConcurrentExecutionAttempts());
            assertThat(vertex1.isAnyOutputBlocking()).isEqualTo(vertex2.isAnyOutputBlocking());
            assertThat(vertex1.isParallelismConfigured())
                    .isEqualTo(vertex2.isParallelismConfigured());
        }
        return true;
    }
}
