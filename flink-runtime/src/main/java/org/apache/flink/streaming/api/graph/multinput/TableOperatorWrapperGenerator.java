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

package org.apache.flink.streaming.api.graph.multinput;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.operators.ResourceSpec;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.core.memory.ManagedMemoryUseCase;
import org.apache.flink.streaming.api.graph.StreamNode;
import org.apache.flink.streaming.api.graph.multinput.input.InputSpec;

import java.util.ArrayList;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * A generator that generates a {@link TableOperatorWrapper} graph from a graph of {@link
 * Transformation}.
 */
@Internal
public class TableOperatorWrapperGenerator {

    /** Original input transformations for . */
    private final List<StreamExecNode> inputTransforms;

    /** The tail (root) transformation of the transformation-graph in . */
    private final StreamExecNode tailTransform;

    /** The read order corresponding to each transformation in {@link #inputTransforms}. */
    private final int[] readOrders;

    /** The list of {@link Transformation} together with their {@link InputSpec}. */
    private final List<Tuple3<InputSpec, TypeInformation<?>, StreamExecEdge>>
            inputTransformAndInputSpecPairs;

    /** The head (leaf) operator wrappers of the operator-graph in . */
    private final List<TableOperatorWrapper<?, ?>> headWrappers;

    /** The tail (root) operator wrapper of the operator-graph in . */
    private TableOperatorWrapper<?, ?> tailWrapper;

    /** Map the visited transformation to its generated TableOperatorWrapper. */
    private final Map<StreamExecNode, TableOperatorWrapper<?, ?>> visitedTransforms;

    /** The identifier for each sub operator in . */
    private int identifierOfSubOp = 0;

    private int parallelism;
    private boolean parallelismConfigured;
    private int maxParallelism;
    private ResourceSpec minResources;
    private ResourceSpec preferredResources;

    /** Managed memory weight for batch operator in mebibyte. */
    private int managedMemoryWeight;

    public TableOperatorWrapperGenerator(
            List<StreamExecEdge> inputEdges, StreamExecNode tailTransform, int[] readOrders) {
        this.inputTransforms =
                inputEdges.stream().map(StreamExecEdge::getSource).collect(Collectors.toList());
        this.tailTransform = tailTransform;
        this.readOrders = readOrders;
        this.inputTransformAndInputSpecPairs = new ArrayList<>();
        this.headWrappers = new ArrayList<>();
        this.visitedTransforms = new IdentityHashMap<>();

        this.parallelism = -1;
        this.maxParallelism = -1;
    }

    public void generate() {
        tailWrapper = visit(tailTransform);
        checkState(inputTransforms.size() == inputTransformAndInputSpecPairs.size());
        calculateManagedMemoryFraction();
    }

    public List<Tuple3<InputSpec, TypeInformation<?>, StreamExecEdge>>
            getInputTransformAndInputSpecPairs() {
        return inputTransformAndInputSpecPairs;
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

    private TableOperatorWrapper<?, ?> visit(StreamExecNode transform) {
        calcParallelismAndResource(checkNotNull(transform.getStreamNode()));

        return visitedTransforms.computeIfAbsent(transform, this::visitTransformation);
    }

    private void calcParallelismAndResource(StreamNode transform) {
        // do not check the parallelisms in multiple-input node are same,
        // because we should consider the following case:
        // Source1(100 parallelism) -> Calc(100 parallelism) -\
        //                                                     -> union -> join -> ...
        // Source2(50 parallelism)  -> Calc(50 parallelism) -/
        parallelism = Math.max(parallelism, transform.getParallelism());
        parallelismConfigured |= transform.isParallelismConfigured();

        int currentMaxParallelism = transform.getMaxParallelism();
        if (maxParallelism < 0) {
            maxParallelism = currentMaxParallelism;
        } else {
            checkState(
                    currentMaxParallelism < 0 || maxParallelism == currentMaxParallelism,
                    "Max parallelism of a transformation in MultipleInput node is different from others. This is a bug.");
        }

        if (minResources == null) {
            minResources = transform.getMinResources();
            preferredResources = transform.getPreferredResources();
            managedMemoryWeight =
                    transform
                            .getManagedMemoryOperatorScopeUseCaseWeights()
                            .getOrDefault(ManagedMemoryUseCase.OPERATOR, 0);
        } else {
            minResources = minResources.merge(transform.getMinResources());
            preferredResources = preferredResources.merge(transform.getPreferredResources());
            managedMemoryWeight +=
                    transform
                            .getManagedMemoryOperatorScopeUseCaseWeights()
                            .getOrDefault(ManagedMemoryUseCase.OPERATOR, 0);
        }
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private TableOperatorWrapper<?, ?> visitTransformation(StreamExecNode transform) {
        Map<Integer, List<StreamExecEdge>> map =
                transform.getInputEdges().stream()
                        .collect(Collectors.groupingBy(StreamExecEdge::getTypeNumber));
        if (map.size() == 1) {
            return visitOneInputTransformation(transform, map.get(0));
        } else if (map.size() >= 2) {
            return visitMultipleInputTransformation(transform, map);
        } else {
            throw new RuntimeException("Unsupported Transformation: " + transform);
        }
    }

    private TableOperatorWrapper<?, ?> visitMultipleInputTransformation(
            StreamExecNode transform, Map<Integer, List<StreamExecEdge>> map) {
        StreamNode streamNode = transform.getStreamNode();
        TableOperatorWrapper<?, ?> wrapper =
                new TableOperatorWrapper<>(
                        streamNode.getOperatorFactory(),
                        genSubOperatorName(streamNode),
                        streamNode.getInTypeInfos(),
                        streamNode.getOutTypeInfo(),
                        map.entrySet().stream()
                                .collect(
                                        Collectors.toMap(
                                                Map.Entry::getKey,
                                                entry -> entry.getValue().size())));

        boolean process = false;
        for (int i = 0; i < map.keySet().size(); i++) {
            List<StreamExecEdge> inputEdges = map.get(i + 1);

            process |= processUnion(inputEdges, wrapper, i + 1);
        }

        if (process) {
            headWrappers.add(wrapper);
        }

        return wrapper;
    }

    private TableOperatorWrapper<?, ?> visitOneInputTransformation(
            StreamExecNode execNode, List<StreamExecEdge> inputEdges) {
        StreamNode streamNode = execNode.getStreamNode();
        TableOperatorWrapper<?, ?> wrapper =
                new TableOperatorWrapper<>(
                        streamNode.getOperatorFactory(),
                        genSubOperatorName(streamNode),
                        streamNode.getInTypeInfos(),
                        streamNode.getOutTypeInfo(),
                        Map.of(1, 1));

        if (processUnion(inputEdges, wrapper, 1)) {
            headWrappers.add(wrapper);
        }
        return wrapper;
    }

    private boolean processUnion(
            List<StreamExecEdge> inputs, TableOperatorWrapper<?, ?> wrapper, int id) {
        int numberOfHeadInput = 0;
        for (StreamExecEdge input : inputs) {
            int inputIdx = inputTransforms.indexOf(input.getSource());
            if (inputIdx >= 0) {
                numberOfHeadInput++;
                processInput(input, inputIdx, wrapper, id);
            } else {
                TableOperatorWrapper<?, ?> inputWrapper = visit(input.getSource());
                wrapper.addInput(inputWrapper, id);
            }
        }

        return numberOfHeadInput > 0;
    }

    private void processInput(
            StreamExecEdge inputEdge,
            int inputIdx,
            TableOperatorWrapper<?, ?> outputWrapper,
            int outputOpInputId) {
        int inputId = inputTransformAndInputSpecPairs.size() + 1;
        InputSpec inputSpec =
                new InputSpec(inputId, readOrders[inputIdx], outputWrapper, outputOpInputId);

        inputTransformAndInputSpecPairs.add(
                Tuple3.of(inputSpec, inputEdge.getSource().getOutTypeInfo(), inputEdge));
    }

    /** calculate managed memory fraction for each operator wrapper. */
    private void calculateManagedMemoryFraction() {
        for (Map.Entry<StreamExecNode, TableOperatorWrapper<?, ?>> entry :
                visitedTransforms.entrySet()) {
            double fraction = 0;
            if (managedMemoryWeight != 0) {
                fraction =
                        entry.getKey()
                                        .getStreamNode()
                                        .getManagedMemoryOperatorScopeUseCaseWeights()
                                        .getOrDefault(ManagedMemoryUseCase.OPERATOR, 0)
                                * 1.0
                                / this.managedMemoryWeight;
            }
            entry.getValue().setManagedMemoryFraction(fraction);
        }
    }

    private String genSubOperatorName(StreamNode transformation) {
        return "SubOp" + (identifierOfSubOp++) + "_" + transformation.getOperatorName();
    }

    public boolean isParallelismConfigured() {
        return parallelismConfigured;
    }
}
