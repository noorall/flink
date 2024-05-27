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
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.util.Hardware;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.util.concurrent.ExecutorThreadFactory;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link AdaptiveJobGraphManager}. */
@SuppressWarnings("serial")
public class AdaptiveJobGraphManagerTest {

    @Test
    void testCreateJobVertexEagerly() {
        // --------- the program ---------

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
        final ExecutorService serializationExecutor =
                Executors.newFixedThreadPool(
                        Math.max(
                                1,
                                Math.min(
                                        Hardware.getNumberCPUCores(),
                                        streamGraph.getExecutionConfig().getParallelism())),
                        new ExecutorThreadFactory("flink-operator-serialization-io"));
        AdaptiveJobGraphManager adaptiveJobGraphGenerator;
        try {
            adaptiveJobGraphGenerator =
                    new AdaptiveJobGraphManager(
                            Thread.currentThread().getContextClassLoader(),
                            streamGraph,
                            serializationExecutor,
                            AdaptiveJobGraphManager.GenerateMode.EAGERLY);
            List<StreamNode> streamNodeList = new ArrayList<>();
            for (Integer nodeId : streamGraph.getSourceIDs()) {
                streamNodeList.add(streamGraph.getStreamNode(nodeId));
            }
            adaptiveJobGraphGenerator.createJobVerticesAndUpdateGraph(streamNodeList);

            assertThat(adaptiveJobGraphGenerator.isStreamGraphConversionFinished()).isEqualTo(true);
            assertThat(adaptiveJobGraphGenerator.getJobGraph().getNumberOfVertices()).isEqualTo(2);

        } finally {
            serializationExecutor.shutdown();
        }
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
        final ExecutorService serializationExecutor =
                Executors.newFixedThreadPool(
                        Math.max(
                                1,
                                Math.min(
                                        Hardware.getNumberCPUCores(),
                                        streamGraph.getExecutionConfig().getParallelism())),
                        new ExecutorThreadFactory("flink-operator-serialization-io"));
        AdaptiveJobGraphManager adaptiveJobGraphGenerator;
        try {
            adaptiveJobGraphGenerator =
                    new AdaptiveJobGraphManager(
                            Thread.currentThread().getContextClassLoader(),
                            streamGraph,
                            serializationExecutor,
                            AdaptiveJobGraphManager.GenerateMode.LAZILY);
            List<StreamNode> streamNodeList = new ArrayList<>();
            for (Integer nodeId : streamGraph.getSourceIDs()) {
                streamNodeList.add(streamGraph.getStreamNode(nodeId));
            }
            List<JobVertex> jobVertices =
                    adaptiveJobGraphGenerator.createJobVerticesAndUpdateGraph(streamNodeList);
            assertThat(adaptiveJobGraphGenerator.isStreamGraphConversionFinished())
                    .isEqualTo(false);
            assertThat(adaptiveJobGraphGenerator.getJobGraph().getNumberOfVertices()).isEqualTo(1);
            while (!adaptiveJobGraphGenerator.isStreamGraphConversionFinished()) {
                List<StreamEdge> streamEdges = new ArrayList<>();
                for (JobVertex jobVertex : jobVertices) {
                    streamEdges.addAll(
                            adaptiveJobGraphGenerator.findOutputEdgesByVertexId(jobVertex.getID()));
                }
                List<StreamNode> streamNodes = new ArrayList<>();
                for (StreamEdge streamEdge : streamEdges) {
                    StreamNode streamNode = streamGraph.getStreamNode(streamEdge.getTargetId());
                    streamNodes.add(streamNode);
                }
                jobVertices.addAll(
                        adaptiveJobGraphGenerator.createJobVerticesAndUpdateGraph(streamNodes));
            }
            assertThat(adaptiveJobGraphGenerator.isStreamGraphConversionFinished()).isEqualTo(true);
            assertThat(adaptiveJobGraphGenerator.getJobGraph().getNumberOfVertices()).isEqualTo(2);

        } finally {
            serializationExecutor.shutdown();
        }
    }
}
