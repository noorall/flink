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
import org.apache.flink.runtime.jobgraph.forwardgroup.ForwardGroup;
import org.apache.flink.runtime.jobgraph.forwardgroup.ForwardGroupComputeUtil;
import org.apache.flink.streaming.api.graph.util.ImmutableStreamEdge;
import org.apache.flink.streaming.api.graph.util.ImmutableStreamNode;
import org.apache.flink.streaming.api.operators.ChainingStrategy;
import org.apache.flink.streaming.api.operators.SourceOperatorFactory;
import org.apache.flink.streaming.api.operators.StreamOperatorFactory;
import org.apache.flink.table.planner.plan.nodes.exec.InputProperty;
import org.apache.flink.table.runtime.strategy.multipleinput.MultipleInputGroup;
import org.apache.flink.table.runtime.strategy.multipleinput.utils.AbstractWrappedNodeExactlyOnceVisitor;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;
import java.util.stream.Collectors;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

public class WrappedStreamNode implements WrappedNode<ImmutableStreamNode> {
    private final ImmutableStreamNode node;
    private final boolean isFrozen;
    private final ForwardGroup<?> forwardGroup;
    // TODO input priority neq input typeNumber
    private final Map<Integer, List<WrappedEdge<?>>> inputEdgesByPriority;
    private final List<WrappedEdge<?>> inputEdges;
    private final List<WrappedEdge<?>> outputEdges;

    private MultipleInputGroup group;

    public WrappedStreamNode(
            ImmutableStreamNode node,
            boolean isFrozen
    ) {
        this.node = node;
        this.isFrozen = isFrozen;
        this.forwardGroup = null;
        this.inputEdgesByPriority = new TreeMap<>();
        this.inputEdges = new ArrayList<>();
        this.outputEdges = new ArrayList<>();
    }

    @Override
    public void addOutputEdge(WrappedEdge<?> outEdge) {
        this.outputEdges.add(outEdge);
    }

    @Override
    public void addInputEdge(WrappedEdge<?> inEdge) {
        this.inputEdgesByPriority
                .computeIfAbsent(inEdge.getInputPriority(), ignored -> new ArrayList<>())
                .add(inEdge);
    }

    @Override
    public void replaceInputEdge(int index, WrappedEdge<?> inEdge) {
        checkArgument(index >= 0 && index < inputEdges.size());
        inputEdges.set(index, inEdge);
    }

    @Override
    public boolean isSameNode(WrappedNode<?> n) {
        return false;
    }

    @Override
    public int getInputGroupSize() {
        return inputEdgesByPriority.size();
    }

    @Override
    public List<WrappedNode<?>> getInputs() {
        return inputEdges.stream().map(WrappedEdge::getSource).collect(Collectors.toList());
    }

    @Override
    public List<WrappedEdge<?>> getInputEdgesByPriority(int inputPriority) {
        return Collections.unmodifiableList(inputEdgesByPriority.get(inputPriority));
    }

    @Override
    public List<WrappedEdge<?>> getInputEdges() {
        return inputEdgesByPriority
                .values()
                .stream()
                .flatMap(List::stream)
                .collect(Collectors.toList());
    }

    @Override
    public List<InputProperty> getInputProperties() {
        return node.getInputProperties();
    }

    @Override
    public List<WrappedNode<?>> getOutputs() {
        return outputEdges.stream().map(WrappedEdge::getTarget).collect(Collectors.toList());
    }

    @Override
    public List<WrappedEdge<?>> getOutputEdges() {
        return Collections.unmodifiableList(outputEdges);
    }

    @Override
    public int getNodeId() {
        return node.getId();
    }

    @Override
    public boolean isFrozen() {
        return isFrozen;
    }

    @Override
    public void accept(AbstractWrappedNodeExactlyOnceVisitor visitor) {
        visitor.visit(this);
    }

    @Override
    public ImmutableStreamNode getNode() {
        return node;
    }

    @Override
    public int getParallelism() {
        return node.getParallelism();
    }

    @Override
    public int getMaxParallelism() {
        return node.getMaxParallelism();
    }

    @Override
    public boolean isParallelismConfigured() {
        return node.isParallelismConfigured();
    }

    @Override
    public String getOperatorName() {
        return node.getOperatorName();
    }

    @Override
    public ResourceSpec getMinResources() {
        return node.getMinResources();
    }

    @Override
    public ResourceSpec getPreferredResources() {
        return node.getPreferredResources();
    }

    @Override
    public Map<ManagedMemoryUseCase, Integer> getManagedMemoryOperatorScopeUseCaseWeights() {
        return node.getManagedMemoryOperatorScopeUseCaseWeights();
    }

    @Nullable
    @Override
    public StreamOperatorFactory<?> getOperatorFactory() {
        return node.getOperatorFactory();
    }

    @Override
    public List<TypeInformation<?>> getInTypeInfos() {
        return node.getInTypeInfos();
    }

    @Override
    public TypeInformation<?> getOutTypeInfo() {
        return node.getOutTypeInfo();
    }

    @Override
    public void setMultipleInputGroup(@Nullable MultipleInputGroup group) {
        this.group = group;
    }

    @Nullable
    @Override
    public MultipleInputGroup getMultipleInputGroup() {
        return group;
    }

    @Override
    public boolean canInSameMultipleInputGroup(WrappedEdge<?> outEdge) {
        checkArgument(outEdge.getSource().equals(this));

        if (outEdge.isBroken()) {
            return false;
        }

        ImmutableStreamNode targetNode = (ImmutableStreamNode) outEdge.getTarget().getNode();
        if (!Objects.equals(node.getSlotSharingGroup(), targetNode.getSlotSharingGroup())) {
            return false;
        }

        if (!(((ImmutableStreamEdge) outEdge.getEdge()).isForwardEdge())) {
            return false;
        }

        return ForwardGroupComputeUtil.canTargetMergeIntoSourceForwardGroup(
                forwardGroup,
                ((WrappedStreamNode) outEdge.getTarget()).forwardGroup);
    }

    @Override
    public boolean canBeRootOfMultipleInputGroup() {
        return inputEdgesByPriority.size() >= 2;
    }

    @Override
    public boolean isChainableSource() {
        if (node.getOperatorFactory() == null
                || !(node.getOperatorFactory() instanceof SourceOperatorFactory)
                || node.getOutEdges().size() != 1) {
            return false;
        }
        ImmutableStreamNode targetNode = (ImmutableStreamNode) getOutputs().get(0).getNode();
        ChainingStrategy targetChainingStrategy = checkNotNull(targetNode.getOperatorFactory())
                .getChainingStrategy();
        return targetChainingStrategy == ChainingStrategy.HEAD_WITH_SOURCES;
    }

    @Override
    public boolean isSingleton() {
        return node.getMaxParallelism() == 1;
    }
}
