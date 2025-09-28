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

package org.apache.flink.table.runtime.strategy.multipleinput.wrapper;

import org.apache.flink.api.common.operators.ResourceSpec;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.core.memory.ManagedMemoryUseCase;
import org.apache.flink.streaming.api.operators.StreamOperatorFactory;
import org.apache.flink.table.planner.plan.nodes.exec.InputProperty;
import org.apache.flink.table.runtime.strategy.multipleinput.MultipleInputGroup;
import org.apache.flink.table.runtime.strategy.multipleinput.utils.AbstractWrappedNodeExactlyOnceVisitor;

import javax.annotation.Nullable;

import java.util.List;
import java.util.Map;

public interface WrappedNode<T> {

    // modify operation
    void addOutputEdge(WrappedEdge<?> outEdge);

    void addInputEdge(WrappedEdge<?> inEdge);

    void replaceInputEdge(int idx, WrappedEdge<?> inEdge);

    // base operation
    boolean isSameNode(WrappedNode<?> n);

    int getInputGroupSize();

    List<WrappedNode<?>> getInputs();

    List<WrappedEdge<?>> getInputEdges();

    List<InputProperty> getInputProperties();

    List<WrappedEdge<?>> getInputEdgesByPriority(int inputPriority);

    List<WrappedNode<?>> getOutputs();

    List<WrappedEdge<?>> getOutputEdges();

    int getNodeId();

    boolean isFrozen();

    T getNode();

    // use for stream node generator
    int getParallelism();

    int getMaxParallelism();

    boolean isParallelismConfigured();

    String getOperatorName();

    ResourceSpec getMinResources();

    ResourceSpec getPreferredResources();

    Map<ManagedMemoryUseCase, Integer> getManagedMemoryOperatorScopeUseCaseWeights();

    @Nullable
    StreamOperatorFactory<?> getOperatorFactory();

    List<TypeInformation<?>> getInTypeInfos();

    TypeInformation<?> getOutTypeInfo();

    // multi input group
    void setMultipleInputGroup(@Nullable MultipleInputGroup group);

    @Nullable
    MultipleInputGroup getMultipleInputGroup();

    boolean canInSameMultipleInputGroup(WrappedEdge<?> outEdge);

    boolean canBeRootOfMultipleInputGroup();

    boolean isChainableSource();

    boolean isSingleton();

    // others
    void accept(AbstractWrappedNodeExactlyOnceVisitor visitor);
}
