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

package org.apache.flink.table.runtime.strategy.multipleinput.node;

import org.apache.flink.api.common.operators.ResourceSpec;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.streaming.api.graph.StreamGraphContext;
import org.apache.flink.streaming.api.graph.StreamNode;
import org.apache.flink.streaming.api.operators.ChainingStrategy;
import org.apache.flink.streaming.api.operators.StreamOperatorFactory;
import org.apache.flink.streaming.runtime.io.MultipleInputSelectionHandler;
import org.apache.flink.streaming.runtime.tasks.MultipleInputStreamTask;
import org.apache.flink.table.planner.plan.nodes.exec.InputProperty;
import org.apache.flink.table.runtime.operators.multipleinput.BatchMultipleInputStreamOperatorFactory;
import org.apache.flink.table.runtime.operators.multipleinput.input.InputSpec;
import org.apache.flink.table.runtime.strategy.multipleinput.transformation.WrappedMultipleInputTransformation;
import org.apache.flink.table.runtime.strategy.multipleinput.utils.TableOperatorWrapperGenerator;
import org.apache.flink.table.runtime.strategy.multipleinput.wrapper.WrappedEdge;
import org.apache.flink.table.runtime.strategy.multipleinput.wrapper.WrappedNode;

import org.apache.commons.lang3.tuple.Pair;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.flink.table.runtime.strategy.multipleinput.utils.MultipleInputUtil.createTransformationDescription;
import static org.apache.flink.table.runtime.strategy.multipleinput.utils.MultipleInputUtil.createTransformationName;
import static org.apache.flink.table.runtime.strategy.multipleinput.utils.MultipleInputUtil.setManagedMemoryWeight;
import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

public class BatchStreamMultipleInput {
    private final WrappedNode<?> rootNode;
    private final List<WrappedNode<?>> memberNodes;
    private final List<WrappedEdge<?>> originalEdges;
    private final List<InputProperty> inputPriorities;
    private final String description;

    private List<WrappedEdge<?>> inputEdges;

    public BatchStreamMultipleInput(
            List<InputProperty> inputPriorities,
            WrappedNode<?> rootNode,
            List<WrappedNode<?>> memberNodes,
            List<WrappedEdge<?>> originalEdges,
            String description
    ) {
        this.rootNode = rootNode;
        this.memberNodes = memberNodes;
        this.inputPriorities = inputPriorities;
        this.originalEdges = originalEdges;
        this.description = description;
    }

    public void setInputEdges(List<WrappedEdge<?>> inputEdges) {
        checkNotNull(inputEdges, "inputEdges should not be null");
        this.inputEdges = inputEdges;
    }

    public List<WrappedNode<?>> getMemberNodes() {
        return Collections.unmodifiableList(memberNodes);
    }

    private List<WrappedEdge<?>> getInputEdges() {
        return Collections.unmodifiableList(inputEdges);
    }

    private List<InputProperty> getInputPriorities() {
        return Collections.unmodifiableList(inputPriorities);
    }

    private TypeInformation<?> getOutTypeInfo() {
        return rootNode.getOutTypeInfo();
    }

    private String getSlotSharingGroup() {
        StreamNode node = (StreamNode) rootNode.getNode();
        return node.getSlotSharingGroup();
    }

    public <OUT> StreamNode transToStreamNode(
            StreamGraphContext context
    ) {
        WrappedMultipleInputTransformation<OUT> transformation = translateToPlanInternal(context
                .getStreamGraph()
                .getConfiguration());
        return translateInternal(transformation, context);
    }

    private <OUT> WrappedMultipleInputTransformation<OUT> translateToPlanInternal(
            ReadableConfig config) {
        List<WrappedEdge<?>> inputEdges = getInputEdges();
        // TODO: check union ?
        final int[] readOrders = getInputPriorities()
                .stream()
                .map(InputProperty::getPriority)
                .mapToInt(i -> i)
                .toArray();

        StreamOperatorFactory<OUT> operatorFactory;
        int parallelism;
        int maxParallelism;
        long memoryBytes;
        ResourceSpec minResources;
        ResourceSpec preferredResources;

        final TableOperatorWrapperGenerator generator = new TableOperatorWrapperGenerator(
                inputEdges,
                rootNode,
                readOrders);
        generator.generate();

        final List<Pair<WrappedEdge<?>, InputSpec>> inputEdgesAndInputSpecPairs = generator.getInputEdgesAndInputSpecPairs();
        operatorFactory = new BatchMultipleInputStreamOperatorFactory(
                inputEdgesAndInputSpecPairs.stream().map(Pair::getValue).collect(
                        Collectors.toList()),
                generator.getHeadWrappers(),
                generator.getTailWrapper());

        parallelism = generator.getParallelism();
        maxParallelism = generator.getMaxParallelism();
        final int memoryWeight = generator.getManagedMemoryWeight();
        memoryBytes = (long) memoryWeight << 20;

        minResources = generator.getMinResources();
        preferredResources = generator.getPreferredResources();

        inputEdges = inputEdgesAndInputSpecPairs
                .stream()
                .map(Pair::getKey)
                .collect(Collectors.toList());

        final WrappedMultipleInputTransformation<OUT> multipleInputTransform = new WrappedMultipleInputTransformation<>(
                createTransformationName(config),
                operatorFactory,
                (TypeInformation<OUT>) getOutTypeInfo(),
                parallelism,
                generator.isParallelismConfigured()
        );

        multipleInputTransform.setDescription(createTransformationDescription(config));
        if (maxParallelism > 0) {
            multipleInputTransform.setMaxParallelism(maxParallelism);
        }

        for (WrappedEdge<?> input : inputEdges) {
            multipleInputTransform.addWrappedInput(input);
        }

        // set resources
        if (minResources != null && preferredResources != null) {
            multipleInputTransform.setResources(minResources, preferredResources);
        }

        multipleInputTransform.setDescription(createTransformationDescription(config));
        setManagedMemoryWeight(multipleInputTransform, memoryBytes);

        // set chaining strategy for source chaining
        multipleInputTransform.setChainingStrategy(ChainingStrategy.HEAD_WITH_SOURCES);

        return multipleInputTransform;
    }

    private <OUT> StreamNode translateInternal(
            final WrappedMultipleInputTransformation<OUT> transformation,
            StreamGraphContext context
    ) {
        checkNotNull(transformation);
        checkNotNull(context);
        final List<WrappedEdge<?>> inputEdges = transformation.getWrappedInputs();
        checkArgument(
                !inputEdges.isEmpty(),
                "Empty inputs for MultipleInputTransformation. Did you forget to add inputs?");
        MultipleInputSelectionHandler.checkSupportedInputCount(inputEdges.size());
        // TODO process id
        final int transformationId = transformation.getId();
        StreamNode streamNode = new StreamNode(
                transformationId,
                getSlotSharingGroup(),
                transformation.getCoLocationGroupKey(),
                transformation.getOperatorFactory(),
                transformation.getName(),
                MultipleInputStreamTask.class,
                transformation.getInputTypes(),
                transformation.getOutputType(),
                transformation.getInputProperties());
        streamNode.setAttribute(transformation.getAttribute());
        streamNode.setParallelism(
                transformation.getParallelism(),
                transformation.isParallelismConfigured());
        streamNode.setMaxParallelism(transformation.getMaxParallelism());
        // TODO process input edges
        streamNode.setSupportsConcurrentExecutionAttempts(
                transformation.isSupportsConcurrentExecutionAttempts());
        return streamNode;
    }
}
