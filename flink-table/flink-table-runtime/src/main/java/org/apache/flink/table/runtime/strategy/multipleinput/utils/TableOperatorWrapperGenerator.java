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

package org.apache.flink.table.runtime.strategy.multipleinput.utils;

import org.apache.flink.api.common.operators.ResourceSpec;
import org.apache.flink.core.memory.ManagedMemoryUseCase;
import org.apache.flink.table.planner.plan.nodes.exec.InputProperty;
import org.apache.flink.table.runtime.operators.multipleinput.TableOperatorWrapper;
import org.apache.flink.table.runtime.operators.multipleinput.input.InputSpec;
import org.apache.flink.table.runtime.strategy.multipleinput.wrapper.WrappedEdge;
import org.apache.flink.table.runtime.strategy.multipleinput.wrapper.WrappedNode;

import org.apache.commons.lang3.tuple.Pair;

import java.util.ArrayList;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.flink.util.Preconditions.checkState;

/**
 * A generator that generates a {@link TableOperatorWrapper} graph from a graph of {@link
 * WrappedNode}.
 */
public class TableOperatorWrapperGenerator {

    // Wrapped inputs
    private final List<WrappedEdge<?>> inputEdges;

    /**
     * The tail (root) of multiple input group.
     */
    private final WrappedNode<?> tail;

    /** The read order corresponding to each transformation in {@link #inputEdges}. */
    private final int[] readOrders;

    /** The list of {@link WrappedEdge} together with their {@link InputSpec}. */
    private final List<Pair<WrappedEdge<?>, InputSpec>> inputAndInputSpecPairs;

    /**
     * The head (leaf) operator wrappers of the operator-graph in multiple input node.
     */
    private final List<TableOperatorWrapper<?, ?>> headWrappers;

    /**
     * The tail (root) operator wrapper of the operator-graph in multiple input node.
     */
    private TableOperatorWrapper<?, ?> tailWrapper;

    /** Map the visited transformation to its generated TableOperatorWrapper. */
    private final Map<WrappedNode<?>, TableOperatorWrapper<?, ?>> visitedNodes;

    /** The identifier for each sub operator in multiple input node. */
    private int identifierOfSubOp = 0;

    private int parallelism;
    private int maxParallelism;
    private boolean parallelismConfigured;
    private ResourceSpec minResources;
    private ResourceSpec preferredResources;

    /** Managed memory weight for batch operator in mebibyte. */
    private int managedMemoryWeight;

    public TableOperatorWrapperGenerator(
            List<WrappedEdge<?>> inputEdges,
            WrappedNode<?> tail,
            int[] readOrders) {
        this.inputEdges = inputEdges;
        this.tail = tail;
        this.readOrders = readOrders;
        this.inputAndInputSpecPairs = new ArrayList<>();
        this.headWrappers = new ArrayList<>();
        this.visitedNodes = new IdentityHashMap<>();

        this.parallelism = -1;
        this.maxParallelism = -1;
    }

    public void generate() {
        tailWrapper = visit(tail);
        checkState(inputEdges.size() == inputAndInputSpecPairs.size());
        calculateManagedMemoryFraction();
    }

    public List<Pair<WrappedEdge<?>, InputSpec>> getInputEdgesAndInputSpecPairs() {
        return inputAndInputSpecPairs;
    }

    public List<TableOperatorWrapper<?, ?>> getHeadWrappers() {
        return headWrappers;
    }

    public TableOperatorWrapper<?, ?> getTailWrapper() {
        return tailWrapper;
    }

    public int getParallelism() {
        return parallelism;
    }

    public int getMaxParallelism() {
        return maxParallelism;
    }

    public ResourceSpec getMinResources() {
        return minResources;
    }

    public ResourceSpec getPreferredResources() {
        return preferredResources;
    }

    public int getManagedMemoryWeight() {
        return managedMemoryWeight;
    }

    private TableOperatorWrapper<?, ?> visit(WrappedNode<?> node) {
        calcParallelismAndResource(node);

        return visitedNodes.computeIfAbsent(node, this::visitTransformation);
    }

    private void calcParallelismAndResource(WrappedNode<?> node) {
        // do not check the parallelisms in multiple-input node are same,
        // because we should consider the following case:
        // Source1(100 parallelism) -> Calc(100 parallelism) -\
        //                                                     -> union -> join -> ...
        // Source2(50 parallelism)  -> Calc(50 parallelism) -/
        parallelism = Math.max(parallelism, node.getParallelism());
        parallelismConfigured |= node.isParallelismConfigured();

        int currentMaxParallelism = node.getMaxParallelism();
        if (maxParallelism < 0) {
            maxParallelism = currentMaxParallelism;
        } else {
            checkState(
                    currentMaxParallelism < 0 || maxParallelism == currentMaxParallelism,
                    "Max parallelism of a transformation in MultipleInput node is different from others. This is a bug.");
        }

        if (minResources == null) {
            minResources = node.getMinResources();
            preferredResources = node.getPreferredResources();
            managedMemoryWeight =
                    node
                            .getManagedMemoryOperatorScopeUseCaseWeights()
                            .getOrDefault(ManagedMemoryUseCase.OPERATOR, 0);
        } else {
            minResources = minResources.merge(node.getMinResources());
            preferredResources = preferredResources.merge(node.getPreferredResources());
            managedMemoryWeight +=
                    node
                            .getManagedMemoryOperatorScopeUseCaseWeights()
                            .getOrDefault(ManagedMemoryUseCase.OPERATOR, 0);
        }
    }

    private TableOperatorWrapper<?, ?> visitTransformation(WrappedNode<?> node) {
        int inputGroupSize = node.getInputGroupSize();
        if (inputGroupSize == 1) {
            return visitOneInputNode(node);
        } else if (inputGroupSize >= 2) {
            return visitMultipleInputNode(node);
        } else {
            throw new RuntimeException("Unsupported WrappedNode: " + node);
        }
    }

    private TableOperatorWrapper<?, ?> visitMultipleInputNode(WrappedNode<?> node) {
        TableOperatorWrapper<?, ?> wrapper = new TableOperatorWrapper<>(
                node.getOperatorFactory(),
                genSubOperatorName(node),
                node.getInTypeInfos(),
                node.getOutTypeInfo()
        );
        boolean processed = false;
        List<Integer> inputProperties = node
                .getInputProperties()
                .stream()
                .map(InputProperty::getPriority)
                .distinct()
                .sorted()
                .collect(Collectors.toList());
        for (int i = 0; i < inputProperties.size(); i++) {
            List<WrappedEdge<?>> inputEdges = node.getInputEdgesByPriority(inputProperties.get(i));
            processed |= processUnion(inputEdges, wrapper, i + 1);
        }

        if (processed) {
            headWrappers.add(wrapper);
        }

        return wrapper;
    }

    private TableOperatorWrapper<?, ?> visitOneInputNode(
            WrappedNode<?> node) {
        TableOperatorWrapper<?, ?> wrapper =
                new TableOperatorWrapper<>(
                        node.getOperatorFactory(),
                        genSubOperatorName(node),
                        node.getInTypeInfos(),
                        node.getOutTypeInfo());
        // outputOpInputId always 1 here
        if (processUnion(node.getInputEdges(), wrapper, 1)) {
            headWrappers.add(wrapper);
        }
        return wrapper;
    }

    private boolean processUnion(
            List<WrappedEdge<?>> inputEdges,
            TableOperatorWrapper<?, ?> wrapper,
            int outputOpInputId
    ) {
        int numberOfHeadInput = 0;
        for (WrappedEdge<?> input : inputEdges) {
            int inputIdx = this.inputEdges.indexOf(input);
            if (inputIdx >= 0) {
                numberOfHeadInput++;
                processInput(input, inputIdx, wrapper, outputOpInputId);
            } else {
                TableOperatorWrapper<?, ?> inputWrapper = visit(input.getSource());
                wrapper.addInput(inputWrapper, outputOpInputId);
            }
        }

        return numberOfHeadInput > 0;
    }

    private void processInput(
            WrappedEdge<?> input,
            int inputIdx,
            TableOperatorWrapper<?, ?> outputWrapper,
            int outputOpInputId) {
        int inputId = inputAndInputSpecPairs.size() + 1;
        InputSpec inputSpec =
                new InputSpec(inputId, readOrders[inputIdx], outputWrapper, outputOpInputId);
        inputAndInputSpecPairs.add(Pair.of(input, inputSpec));
    }

    /** calculate managed memory fraction for each operator wrapper. */
    private void calculateManagedMemoryFraction() {
        for (Map.Entry<WrappedNode<?>, TableOperatorWrapper<?, ?>> entry :
                visitedNodes.entrySet()) {
            double fraction = 0;
            if (managedMemoryWeight != 0) {
                fraction =
                        entry.getKey()
                                .getManagedMemoryOperatorScopeUseCaseWeights()
                                .getOrDefault(ManagedMemoryUseCase.OPERATOR, 0)
                                * 1.0
                                / this.managedMemoryWeight;
            }
            entry.getValue().setManagedMemoryFraction(fraction);
        }
    }

    private String genSubOperatorName(WrappedNode<?> node) {
        return "SubOp" + (identifierOfSubOp++) + "_" + node.getOperatorName();
    }

    public boolean isParallelismConfigured() {
        return parallelismConfigured;
    }
}
