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

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.CheckpointStorage;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.streaming.api.operators.InternalTimeServiceManager;
import org.apache.flink.streaming.api.operators.StreamOperatorFactory;
import org.apache.flink.streaming.api.transformations.StreamExchangeMode;
import org.apache.flink.streaming.runtime.partitioner.StreamPartitioner;
import org.apache.flink.util.InstantiationUtil;
import org.apache.flink.util.OutputTag;
import org.apache.flink.util.SerializedValue;
import org.apache.flink.util.concurrent.FutureUtils;

import java.io.IOException;
import java.io.Serializable;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.stream.Collectors;

public class StreamGraphConfig implements Serializable {

    private static final long serialVersionUID = 1L;

    private static final String VIRTUAL_PARTITION_NODES = "virtualPartitionNodes";
    private static final String VIRTUAL_SIDE_OUTPUT_NODES = "virtualSideOutputNodes";

    private final Configuration config = new Configuration();
    private SerializedValue<StateBackend> stateBackendSerializedValue;
    private SerializedValue<CheckpointStorage> storageSerializedValue;
    private SerializedValue<ExecutionConfig> configSerializedValue;
    private SerializedValue<Map<Integer, StreamNode>> streamNodesSerializedValue;
    private SerializedValue<InternalTimeServiceManager.Provider> providerSerializedValue;
    private final Configuration operatorFactoryConfig = new Configuration();

    public CompletableFuture<?> serializeStreamNodes(
            Map<Integer, StreamNode> toBeSerializedStreamNodes, Executor ioExecutor) {
        try {
            FutureUtils.ConjunctFuture<Collection<Void>> future =
                    FutureUtils.combineAll(
                            toBeSerializedStreamNodes.values().stream()
                                    .filter(node -> node.getOperatorFactory() != null)
                                    .map(
                                            node ->
                                                    CompletableFuture.runAsync(
                                                            () -> {
                                                                try {
                                                                    InstantiationUtil
                                                                            .writeObjectToConfig(
                                                                                    node
                                                                                            .getOperatorFactory(),
                                                                                    this
                                                                                            .operatorFactoryConfig,
                                                                                    String.valueOf(
                                                                                            node
                                                                                                    .getId()));
                                                                } catch (IOException e) {
                                                                    throw new RuntimeException(
                                                                            "Could not serialize stream nodes",
                                                                            e);
                                                                }
                                                            },
                                                            ioExecutor))
                                    .collect(Collectors.toList()));
            this.streamNodesSerializedValue = new SerializedValue<>(toBeSerializedStreamNodes);
            return future;
        } catch (IOException e) {
            throw new RuntimeException("Could not serialize stream nodes", e);
        }
    }

    public void serializeVirtualPartitionNodes(
            Map<Integer, Tuple3<Integer, StreamPartitioner<?>, StreamExchangeMode>>
                    toBeSerializedVirtualPartitionNodes) {
        try {
            InstantiationUtil.writeObjectToConfig(
                    toBeSerializedVirtualPartitionNodes.entrySet().stream()
                            .map(entry -> Tuple2.of(entry.getKey(), entry.getValue()))
                            .collect(Collectors.toList()),
                    this.config,
                    VIRTUAL_PARTITION_NODES);
        } catch (IOException e) {
            throw new RuntimeException("Could not serialize virtual Partition Nodes.", e);
        }
    }

    public void serializeStateBackends(StateBackend stateBackend) {
        try {
            this.stateBackendSerializedValue = new SerializedValue<>(stateBackend);
        } catch (IOException e) {
            throw new RuntimeException("Could not serialize state backend.", e);
        }
    }

    public void serializeCheckpointStorage(CheckpointStorage checkpointStorage) {
        try {
            this.storageSerializedValue = new SerializedValue<>(checkpointStorage);
        } catch (IOException e) {
            throw new RuntimeException("Could not serialize checkpoint storage.", e);
        }
    }

    public void serializeVirtualSideOutputNodes(
            Map<Integer, Tuple2<Integer, OutputTag>> virtualSideOutputNodes) {
        try {
            InstantiationUtil.writeObjectToConfig(
                    virtualSideOutputNodes, this.config, VIRTUAL_SIDE_OUTPUT_NODES);
        } catch (IOException e) {
            throw new RuntimeException("Could not serialize virtualSideOutputNodes.", e);
        }
    }

    public void serializeExecutionConfig(ExecutionConfig toBeSerializeExecutionConfig) {
        try {
            this.configSerializedValue = new SerializedValue<>(toBeSerializeExecutionConfig);
        } catch (IOException e) {
            throw new RuntimeException("Could not serialize execution config.", e);
        }
    }

    public void serializeTimeServiceProvider(
            InternalTimeServiceManager.Provider toBeSerializeTimerServiceProvider) {
        try {
            this.providerSerializedValue = new SerializedValue<>(toBeSerializeTimerServiceProvider);
        } catch (IOException e) {
            throw new RuntimeException("Could not serialize time service provider.", e);
        }
    }

    public InternalTimeServiceManager.Provider getTimeServiceProvider(ClassLoader cl) {
        try {
            return providerSerializedValue.deserializeValue(cl);
        } catch (Exception e) {
            throw new RuntimeException("Could not deserialize time service provider.", e);
        }
    }

    public ExecutionConfig getExecutionConfig(ClassLoader cl) {
        try {
            return configSerializedValue.deserializeValue(cl);
        } catch (Exception e) {
            throw new RuntimeException("Could not deserialize serializer config.", e);
        }
    }

    public SerializedValue<StateBackend> getStateBackendSerializedValue() {
        return stateBackendSerializedValue;
    }

    public SerializedValue<CheckpointStorage> getStorageSerializedValue() {
        return storageSerializedValue;
    }

    public SerializedValue<InternalTimeServiceManager.Provider> getProviderSerializedValue() {
        return providerSerializedValue;
    }

    public CompletableFuture<Map<Integer, StreamNode>> getStreamNodes(
            ClassLoader cl, Executor ioExecutor) {
        try {
            FutureUtils.ConjunctFuture<Collection<Tuple2<String, StreamOperatorFactory<?>>>>
                    future =
                            FutureUtils.combineAll(
                                    operatorFactoryConfig.keySet().stream()
                                            .map(
                                                    nodeId ->
                                                            CompletableFuture.supplyAsync(
                                                                    () -> {
                                                                        try {
                                                                            StreamOperatorFactory<?>
                                                                                    operatorFactory =
                                                                                            InstantiationUtil
                                                                                                    .readObjectFromConfig(
                                                                                                            operatorFactoryConfig,
                                                                                                            nodeId,
                                                                                                            cl);
                                                                            return Tuple2
                                                                                    .<String,
                                                                                            StreamOperatorFactory<
                                                                                                    ?>>
                                                                                            of(
                                                                                                    nodeId,
                                                                                                    operatorFactory);
                                                                        } catch (Exception e) {
                                                                            throw new RuntimeException(
                                                                                    "Could not deserialize stream node "
                                                                                            + nodeId,
                                                                                    e);
                                                                        }
                                                                    },
                                                                    ioExecutor))
                                            .collect(Collectors.toList()));
            Map<Integer, StreamNode> streamNodes = streamNodesSerializedValue.deserializeValue(cl);

            return future.thenApply(
                    results -> {
                        for (Tuple2<String, StreamOperatorFactory<?>> tuple2 : results) {
                            streamNodes
                                    .get(Integer.valueOf(tuple2.f0))
                                    .setOperatorFactory(tuple2.f1);
                        }

                        return streamNodes;
                    });
        } catch (Exception e) {
            throw new RuntimeException("Could not deserialize stream nodes.", e);
        }
    }

    public StateBackend getStateBackend(ClassLoader cl) {
        try {
            return stateBackendSerializedValue.deserializeValue(cl);
        } catch (Exception e) {
            throw new RuntimeException("Could not deserialize state backend.", e);
        }
    }

    public CheckpointStorage getCheckpointStorage(ClassLoader cl) {
        try {
            return storageSerializedValue.deserializeValue(cl);
        } catch (Exception e) {
            throw new RuntimeException("Could not deserialize checkpoint storage.", e);
        }
    }

    public Map<Integer, Tuple2<Integer, OutputTag>> getVirtualSideOutputNodes(ClassLoader cl) {
        try {
            return InstantiationUtil.readObjectFromConfig(
                    this.config, VIRTUAL_SIDE_OUTPUT_NODES, cl);
        } catch (Exception e) {
            throw new RuntimeException("Could not deserialize checkpoint storage.", e);
        }
    }

    public Map<Integer, Tuple3<Integer, StreamPartitioner<?>, StreamExchangeMode>>
            getVirtualPartitionNodes(ClassLoader cl) {
        try {
            List<Tuple2<Integer, Tuple3<Integer, StreamPartitioner<?>, StreamExchangeMode>>> list =
                    InstantiationUtil.readObjectFromConfig(
                            this.config, VIRTUAL_PARTITION_NODES, cl);
            Map<Integer, Tuple3<Integer, StreamPartitioner<?>, StreamExchangeMode>>
                    virtualPartitionNodes = new HashMap<>();
            list.forEach(tuple2 -> virtualPartitionNodes.put(tuple2.f0, tuple2.f1));

            return virtualPartitionNodes;
        } catch (Exception e) {
            throw new RuntimeException("Could not deserialize virtual partition nodes.", e);
        }
    }

    public int getNumberOfVertices() {
        return config.toMap().size();
    }
}
