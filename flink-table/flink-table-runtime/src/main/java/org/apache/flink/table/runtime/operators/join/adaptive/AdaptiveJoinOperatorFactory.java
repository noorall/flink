/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.runtime.operators.join.adaptive;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.streaming.api.graph.StreamEdge;
import org.apache.flink.streaming.api.graph.StreamNode;
import org.apache.flink.streaming.api.graph.multinput.StreamExecEdge;
import org.apache.flink.streaming.api.graph.multinput.StreamExecNode;
import org.apache.flink.streaming.api.operators.AbstractStreamOperatorFactory;
import org.apache.flink.streaming.api.operators.StreamOperator;
import org.apache.flink.streaming.api.operators.StreamOperatorFactory;
import org.apache.flink.streaming.api.operators.StreamOperatorParameters;
import org.apache.flink.table.planner.loader.PlannerModule;
import org.apache.flink.table.runtime.operators.join.FlinkJoinType;
import org.apache.flink.util.InstantiationUtil;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Adaptive join factory.
 *
 * <p>Note: This class will hold an {@link AdaptiveJoin} and serve as a proxy class to provide an
 * interface externally. Due to runtime access visibility constraints with the table-planner module,
 * the {@link AdaptiveJoin} object will be serialized during the Table Planner phase and will only
 * be lazily deserialized before the dynamic generation of the JobGraph.
 *
 * @param <OUT> The output type of the operator
 */
@Internal
public class AdaptiveJoinOperatorFactory<OUT> extends AbstractStreamOperatorFactory<OUT>
        implements AdaptiveJoin {
    private static final long serialVersionUID = 1L;

    private final byte[] adaptiveJoinSerialized;

    @Nullable private transient AdaptiveJoin adaptiveJoin;

    @Nullable private StreamOperatorFactory<OUT> finalFactory;

    public AdaptiveJoinOperatorFactory(byte[] adaptiveJoinSerialized) {
        this.adaptiveJoinSerialized = checkNotNull(adaptiveJoinSerialized);
    }

    @Override
    public StreamOperatorFactory<?> genOperatorFactory(
            ClassLoader classLoader, ReadableConfig config) {
        checkAndLazyInitialize();
        this.finalFactory =
                (StreamOperatorFactory<OUT>) adaptiveJoin.genOperatorFactory(classLoader, config);
        return this.finalFactory;
    }

    @Override
    public void generateOperatorFactory(
            ClassLoader classLoader,
            ReadableConfig config,
            StreamNode adaptiveJoinNode,
            StreamExecNode adaptiveJoinExecNode) {
        if (this.finalFactory != null) {
            return;
        }

        // [original left is build, current left is build]
        // 1. [true, false] original edge [left, right] [build, probe] -> [right, left] [build,
        // probe]
        // 2. [true, true] original edge [left, right] [build, probe] -> [left, right] [build,
        // probe]
        // 3. [false, false] original edge [left, right] [probe, build] -> [right, left] [build,
        // build]
        // 4. [false, true] original edge [left, right] [probe, build] -> [left, right] [build,
        // probe]
        if (shouldReorderInputs()) {
            generateStreamEdgeUpdateRequestInfosForInputsReordered(
                    adaptiveJoinNode, adaptiveJoinExecNode);
            generateStreamNodeUpdateRequestInfosForInputsReordered(adaptiveJoinNode);
        }

        if (shouldReorderProperties()) {
            Collections.reverse(adaptiveJoinNode.getInputProperties());
            adaptiveJoinExecNode.reverseInputProperties();
        }

        genOperatorFactory(classLoader, config);
    }

    private boolean shouldReorderProperties() {
        return !originalLeftIsBuild();
    }

    private void generateStreamEdgeUpdateRequestInfosForInputsReordered(
            StreamNode adaptiveJoinNode, StreamExecNode streamExecNode) {
        for (StreamEdge inEdge : adaptiveJoinNode.getInEdges()) {
            if (inEdge.getTypeNumber() == 1) {
                inEdge.setTypeNumber(2);
            } else if (inEdge.getTypeNumber() == 2) {
                inEdge.setTypeNumber(1);
            } else {
                throw new IllegalStateException();
            }
        }

        for (StreamExecEdge inEdge : streamExecNode.getInputEdges()) {
            if (inEdge.getTypeNumber() == 1) {
                inEdge.setTypeNumber(2);
            } else if (inEdge.getTypeNumber() == 2) {
                inEdge.setTypeNumber(1);
            } else {
                throw new IllegalStateException();
            }
        }

        Collections.reverse(adaptiveJoinNode.getInEdges());
        Collections.reverse(streamExecNode.getInputEdges());
    }

    private void generateStreamNodeUpdateRequestInfosForInputsReordered(StreamNode modifiedNode) {
        TypeSerializer<?>[] typeSerializers = modifiedNode.getTypeSerializersIn();
        Preconditions.checkState(
                typeSerializers.length == 2,
                String.format(
                        "Adaptive join currently only supports two "
                                + "inputs, but the join node [%s] has received %s inputs.",
                        modifiedNode.getId(), typeSerializers.length));
        TypeSerializer<?>[] swappedTypeSerializers = new TypeSerializer<?>[2];
        swappedTypeSerializers[0] = typeSerializers[1];
        swappedTypeSerializers[1] = typeSerializers[0];
        modifiedNode.setSerializersIn(swappedTypeSerializers);

        List<TypeInformation<?>> inTypeInfos = modifiedNode.getInTypeInfos();
        Preconditions.checkState(
                inTypeInfos.size() == 2,
                String.format(
                        "Adaptive join currently only supports two "
                                + "inputs, but the join node [%s] has received %s inputs.",
                        modifiedNode.getId(), inTypeInfos.size()));
        TypeInformation<?>[] swappedInTypeInfos = new TypeInformation<?>[2];
        swappedInTypeInfos[0] = inTypeInfos.get(1);
        swappedInTypeInfos[1] = inTypeInfos.get(0);
        modifiedNode.setInTypeInfos(Arrays.asList(swappedInTypeInfos));
    }

    @Override
    public FlinkJoinType getJoinType() {
        checkAndLazyInitialize();
        return adaptiveJoin.getJoinType();
    }

    @Override
    public void markAsBroadcastJoin(boolean canBeBroadcast, boolean leftIsBuild) {
        checkAndLazyInitialize();
        adaptiveJoin.markAsBroadcastJoin(canBeBroadcast, leftIsBuild);
    }

    @Override
    public boolean shouldReorderInputs() {
        checkAndLazyInitialize();
        return adaptiveJoin.shouldReorderInputs();
    }

    @Override
    public boolean leftIsBuild() {
        checkAndLazyInitialize();
        return adaptiveJoin.leftIsBuild();
    }

    @Override
    public boolean originalLeftIsBuild() {
        checkAndLazyInitialize();
        return adaptiveJoin.originalLeftIsBuild();
    }

    private void checkAndLazyInitialize() {
        if (this.adaptiveJoin == null) {
            lazyInitialize();
        }
    }

    @Override
    public <T extends StreamOperator<OUT>> T createStreamOperator(
            StreamOperatorParameters<OUT> parameters) {
        checkNotNull(
                finalFactory,
                String.format(
                        "The OperatorFactory of task [%s] have not been initialized.",
                        parameters.getContainingTask()));
        if (finalFactory instanceof AbstractStreamOperatorFactory) {
            ((AbstractStreamOperatorFactory<OUT>) finalFactory)
                    .setProcessingTimeService(processingTimeService);
        }
        StreamOperator<OUT> operator = finalFactory.createStreamOperator(parameters);
        return (T) operator;
    }

    @Override
    public Class<? extends StreamOperator> getStreamOperatorClass(ClassLoader classLoader) {
        throw new UnsupportedOperationException(
                "The method should not be invoked in the "
                        + "adaptive join operator for batch jobs.");
    }

    private void lazyInitialize() {
        if (!tryInitializeAdaptiveJoin(Thread.currentThread().getContextClassLoader())) {
            boolean isSuccess =
                    tryInitializeAdaptiveJoin(
                            PlannerModule.getInstance().getSubmoduleClassLoader());
            if (!isSuccess) {
                throw new RuntimeException(
                        "Failed to deserialize AdaptiveJoin instance. "
                                + "Please check whether the flink-table-planner-loader.jar is in the classpath.");
            }
        }
    }

    private boolean tryInitializeAdaptiveJoin(ClassLoader classLoader) {
        try {
            this.adaptiveJoin =
                    InstantiationUtil.deserializeObject(adaptiveJoinSerialized, classLoader);
        } catch (ClassNotFoundException | IOException e) {
            return false;
        }

        return true;
    }
}
