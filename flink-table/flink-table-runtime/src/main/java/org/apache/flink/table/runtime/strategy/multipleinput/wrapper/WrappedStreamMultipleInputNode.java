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
import org.apache.flink.table.runtime.strategy.multipleinput.node.BatchStreamMultipleInput;
import org.apache.flink.table.runtime.strategy.multipleinput.utils.AbstractWrappedNodeExactlyOnceVisitor;

import javax.annotation.Nullable;

import java.util.List;
import java.util.Map;

public class WrappedStreamMultipleInputNode implements WrappedNode<BatchStreamMultipleInput> {
    private final BatchStreamMultipleInput multipleInputNode;

    public WrappedStreamMultipleInputNode(BatchStreamMultipleInput multipleInputNode) {
        this.multipleInputNode = multipleInputNode;
    }

    @Override
    public void addOutputEdge(WrappedEdge<?> outEdge) {

    }

    @Override
    public void addInputEdge(WrappedEdge<?> inEdge) {

    }

    @Override
    public void replaceInputEdge(int idx, WrappedEdge<?> inEdge) {

    }

    @Override
    public boolean isSameNode(WrappedNode<?> n) {
        return false;
    }

    @Override
    public int getInputGroupSize() {
        return 0;
    }

    @Override
    public List<WrappedNode<?>> getInputs() {
        return List.of();
    }

    @Override
    public List<WrappedEdge<?>> getInputEdges() {
        return List.of();
    }

    @Override
    public List<InputProperty> getInputProperties() {
        return List.of();
    }

    @Override
    public List<WrappedEdge<?>> getInputEdgesByPriority(int inputPriority) {
        return List.of();
    }

    @Override
    public List<WrappedNode<?>> getOutputs() {
        return List.of();
    }

    @Override
    public List<WrappedEdge<?>> getOutputEdges() {
        return List.of();
    }

    @Override
    public int getNodeId() {
        return 0;
    }

    @Override
    public boolean isFrozen() {
        return false;
    }

    @Override
    public BatchStreamMultipleInput getNode() {
        return null;
    }

    @Override
    public int getParallelism() {
        return 0;
    }

    @Override
    public int getMaxParallelism() {
        return 0;
    }

    @Override
    public boolean isParallelismConfigured() {
        return false;
    }

    @Override
    public String getOperatorName() {
        return "";
    }

    @Override
    public ResourceSpec getMinResources() {
        return null;
    }

    @Override
    public ResourceSpec getPreferredResources() {
        return null;
    }

    @Override
    public Map<ManagedMemoryUseCase, Integer> getManagedMemoryOperatorScopeUseCaseWeights() {
        return Map.of();
    }

    @Nullable
    @Override
    public StreamOperatorFactory<?> getOperatorFactory() {
        return null;
    }

    @Override
    public List<TypeInformation<?>> getInTypeInfos() {
        return List.of();
    }

    @Override
    public TypeInformation<?> getOutTypeInfo() {
        return null;
    }

    @Override
    public void setMultipleInputGroup(@Nullable MultipleInputGroup group) {

    }

    @Nullable
    @Override
    public MultipleInputGroup getMultipleInputGroup() {
        return null;
    }

    @Override
    public boolean canInSameMultipleInputGroup(WrappedEdge<?> outEdge) {
        return false;
    }

    @Override
    public boolean canBeRootOfMultipleInputGroup() {
        return false;
    }

    @Override
    public boolean isChainableSource() {
        return false;
    }

    @Override
    public boolean isSingleton() {
        return false;
    }

    @Override
    public void accept(AbstractWrappedNodeExactlyOnceVisitor visitor) {
        throw new UnsupportedOperationException("unsupported operation");
    }
}
