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

package org.apache.flink.table.runtime.strategy.multipleinput;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.graph.util.ImmutableStreamEdge;
import org.apache.flink.streaming.api.graph.util.ImmutableStreamGraph;
import org.apache.flink.streaming.api.graph.util.ImmutableStreamNode;
import org.apache.flink.table.planner.plan.nodes.exec.InputProperty;
import org.apache.flink.table.runtime.strategy.multipleinput.node.BatchStreamMultipleInput;
import org.apache.flink.table.runtime.strategy.multipleinput.utils.AbstractWrappedNodeExactlyOnceVisitor;
import org.apache.flink.table.runtime.strategy.multipleinput.utils.InputOrderCalculator;
import org.apache.flink.table.runtime.strategy.multipleinput.utils.MultipleInputUtil;
import org.apache.flink.table.runtime.strategy.multipleinput.wrapper.WrappedEdge;
import org.apache.flink.table.runtime.strategy.multipleinput.wrapper.WrappedNode;
import org.apache.flink.table.runtime.strategy.multipleinput.wrapper.WrappedStreamEdge;
import org.apache.flink.table.runtime.strategy.multipleinput.wrapper.WrappedStreamMultipleInputNode;
import org.apache.flink.table.runtime.strategy.multipleinput.wrapper.WrappedStreamNode;
import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Queue;
import java.util.Set;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

public class MultipleInputNodeCreationProcessor {
    private static final Logger LOG =
            LoggerFactory.getLogger(MultipleInputNodeCreationProcessor.class);

    public static MultipleInputNodeCreateResult process(
            ImmutableStreamGraph streamGraph,
            Set<Integer> frozenNodes) {
        // root -> sinks
        List<WrappedNode<?>> rootWrappers = wrapStreamNodes(streamGraph, frozenNodes);

        // TODO: frozenNodes带来的边界问题：
        // 1. frozenNodes不能被放入multipleInput算子
        // 2. frozenNodes的output边无法被增删，我们只能通过修改edge的属性来实现，（例如将target修改为multipleInput算子）
        MultipleInputPriorityConflictResolver resolver =
                new MultipleInputPriorityConflictResolver(
                        rootWrappers, frozenNodes,
                        InputProperty.DamBehavior.BLOCKING);
        resolver.detectAndResolve();

        List<WrappedNode<?>> orderedWrappers = reversedTopologicalSort(rootWrappers);
        createMultipleInputGroups(orderedWrappers);

        // TODO: 处理边界问题
        optimizeMultiInputGroups(orderedWrappers);

        List<WrappedNode<?>> newRootNodes = createMultipleInputNodes(rootWrappers);

        return generateMultipleInputNodeResult(newRootNodes);
    }

    private static List<WrappedNode<?>> wrapStreamNodes(
            ImmutableStreamGraph streamGraph, Set<Integer> frozenNodes) {
        List<ImmutableStreamNode> topologicallyFromSources =
                streamGraph.getStreamNodesSortedTopologicallyFromSources();

        Map<Integer, WrappedNode<?>> wrappedNodeMap = new HashMap<>();
        List<WrappedNode<?>> roots = new ArrayList<>();

        for (ImmutableStreamNode node : topologicallyFromSources) {
            // 对于frozenNode如何处理？
            boolean isFrozen = frozenNodes.contains(node.getId());
            WrappedNode<?> targetNode = new WrappedStreamNode(node, isFrozen);
            for (ImmutableStreamEdge inEdge : node.getInEdges()) {
                WrappedNode<?> sourceNode = checkNotNull(wrappedNodeMap.get(inEdge.getSourceId()));
                WrappedEdge<?> edge = new WrappedStreamEdge(
                        inEdge,
                        sourceNode,
                        targetNode,
                        isFrozen);
                sourceNode.addOutputEdge(edge);
                targetNode.addInputEdge(edge);
            }
            wrappedNodeMap.put(node.getId(), targetNode);
            if (streamGraph.getSinkIDs().contains(node.getId())) {
                roots.add(targetNode);
            }
        }

        return roots;
    }

    private static List<WrappedNode<?>> reversedTopologicalSort(List<WrappedNode<?>> rootWrappers) {
        List<WrappedNode<?>> result = new ArrayList<>();
        Queue<WrappedNode<?>> queue = new LinkedList<>(rootWrappers);
        Map<WrappedNode<?>, Integer> visitCountMap = new HashMap<>();

        while (!queue.isEmpty()) {
            WrappedNode<?> wrapper = queue.poll();
            result.add(wrapper);
            for (WrappedNode<?> inputWrapper : wrapper.getInputs()) {
                int visitCount =
                        visitCountMap.compute(inputWrapper, (k, v) -> v == null ? 1 : v + 1);
                if (visitCount == inputWrapper.getOutputEdges().size()) {
                    queue.offer(inputWrapper);
                }
            }
        }

        return result;
    }

    private static void createMultipleInputGroups(List<WrappedNode<?>> orderedWrappers) {
        for (WrappedNode<?> wrapper : orderedWrappers) {
            if (!canBeMultipleInputNodeMember(wrapper)) {
                LOG.info("!canBeMultipleInputNodeMember {} ", wrapper.getNode());
                continue;
            }

            Optional<MultipleInputGroup> outputGroup = canBeInSameGroupWithOutputs(wrapper);
            if (outputGroup.isPresent()) {
                outputGroup.get().addMember(wrapper);
                continue;
            }

            if (canBeRootOfMultipleInputGroup(wrapper)) {
                wrapper.setMultipleInputGroup(new MultipleInputGroup(wrapper));
            }
        }
    }

    private static boolean canBeMultipleInputNodeMember(WrappedNode<?> wrapper) {
        // TODO: check if support multi input in multi input
        return !wrapper.getInputEdges().isEmpty() && !wrapper.isFrozen();
    }

    private static Optional<MultipleInputGroup> canBeInSameGroupWithOutputs(WrappedNode<?> wrapper) {
        List<WrappedEdge<?>> outputs = wrapper.getOutputEdges();
        if (outputs.isEmpty()) {
            return Optional.empty();
        }

        MultipleInputGroup outputGroup = outputs.get(0).getTarget().getMultipleInputGroup();
        if (outputGroup == null) {
            return Optional.empty();
        }

        for (WrappedEdge<?> output : outputs) {
            if (output.getTarget().getMultipleInputGroup() != outputGroup) {
                return Optional.empty();
            }
            if (!wrapper.canInSameMultipleInputGroup(output)) {
                return Optional.empty();
            }
        }

        return Optional.of(outputGroup);
    }

    private static boolean canBeRootOfMultipleInputGroup(WrappedNode<?> wrapper) {
        return wrapper.canBeRootOfMultipleInputGroup();
    }

    private static void optimizeMultiInputGroups(List<WrappedNode<?>> orderedWrappers) {
        for (int i = orderedWrappers.size() - 1; i >= 0; i--) {
            WrappedNode<?> wrapper = orderedWrappers.get(i);
            MultipleInputGroup group = wrapper.getMultipleInputGroup();
            if (group == null) {
                continue;
            }
            if (!isEntranceOfMultipleInputGroup(wrapper)) {
                continue;
            }

            boolean shouldRemove = false;
            if (wrapper.getInputGroupSize() == 1) {
                // optimization 1. for one-input operators we'll remove it unless its input
                // is an exchange or a FLIP-27 source, this is mainly to avoid the following
                // pattern:
                // non-chainable source -> calc --\
                //                                 join ->
                // non-chainable source -> calc --/
                // if we move two calcs into the multiple input group rooted at the join, we're
                // directly shuffling large amount of records from the source without filtering
                // by the calc
                List<WrappedNode<?>> inputs = wrapper.getInputs();
                shouldRemove = !inputs.stream().allMatch(WrappedNode::isChainableSource);
            }
            // optimization 2. for singleton operations (for example singleton global agg)
            // we're not including it into the multiple input node as we have to ensure that
            // the whole multiple input can only have 1 parallelism.
            // continuous singleton operations connected by forwarding shuffle will be dealt
            // together with optimization 2
            shouldRemove |= wrapper.isSingleton();

            if (shouldRemove) {
                LOG.info("### remove node {}", wrapper.getNode());
                wrapper.getMultipleInputGroup().removeMember(wrapper);
            }
        }

        for (WrappedNode<?> wrapper : orderedWrappers) {
            MultipleInputGroup group = wrapper.getMultipleInputGroup();
            if (group == null) {
                // we only consider nodes currently in a multiple input group
                continue;
            }
            if (wrapper != group.getRoot()) {
                // we only consider nodes at the root of the multiple input
                continue;
            }
            if (group.getMemberSize() == 1) {
                // optimization 3. we clean up multiple input groups with only 1 member,
                // unless one of its input is a FLIP-27 source (for maximizing source chaining),
                // however unions do not apply to this optimization because they're not real
                // operators
                if (wrapper.getInputs().stream().noneMatch(WrappedNode::isChainableSource)) {
                    wrapper.getMultipleInputGroup().removeRoot();
                    LOG.info("### remove multiple input group with root {}", wrapper.getNode());
                } else {
                    LOG.info(
                            "### allow multiple input group with root {} with single member",
                            wrapper.getNode());
                }
                continue;
            }
            if (wrapper.getInputs().size() == 1) {
                // optimization 4. operators with only 1 input are not allowed to be the root,
                // as their chaining will be handled by operator chains.
                LOG.info(
                        "### remove multiple input group with root {} as it has only one input",
                        wrapper.getNode());
                wrapper.getMultipleInputGroup().removeRoot();
            }
        }
    }

    private static boolean isEntranceOfMultipleInputGroup(WrappedNode<?> wrapper) {
        Preconditions.checkNotNull(
                wrapper.getMultipleInputGroup(),
                "Exec node wrapper does not have a multiple input group. This is a bug.");
        for (WrappedNode<?> inputWrapper : wrapper.getInputs()) {
            if (inputWrapper.getMultipleInputGroup() == wrapper.getMultipleInputGroup()) {
                return false;
            }
        }
        return true;
    }

    private static List<WrappedNode<?>> createMultipleInputNodes(List<WrappedNode<?>> rootWrappers) {
        List<WrappedNode<?>> result = new ArrayList<>();
        Map<WrappedNode<?>, WrappedNode<?>> visitedMap = new HashMap<>();
        for (WrappedNode<?> rootWrapper : rootWrappers) {
            result.add(getMultipleInputNode(rootWrapper, visitedMap));
        }
        return result;
    }

    private static WrappedNode<?> getMultipleInputNode(
            WrappedNode<?> wrapper,
            Map<WrappedNode<?>, WrappedNode<?>> visitedMap) {
        if (visitedMap.containsKey(wrapper)) {
            return wrapper;
        }

        List<WrappedEdge<?>> inputEdges = wrapper.getInputEdges();
        for (int i = 0; i < inputEdges.size(); i++) {
            WrappedEdge<?> inputEdge = inputEdges.get(i);
            WrappedNode<?> input = inputEdges.get(i).getSource();

            WrappedNode<?> multipleInputNode = getMultipleInputNode(input, visitedMap);

            WrappedEdge<?> wrappedEdge = new WrappedStreamEdge(
                    (ImmutableStreamEdge) inputEdge.getEdge(),
                    multipleInputNode,
                    wrapper,
                    inputEdge.isFrozen()
            );
            wrapper.replaceInputEdge(i, wrappedEdge);
        }

        WrappedNode<?> ret;
        MultipleInputGroup group = wrapper.getMultipleInputGroup();
        if (group != null && wrapper == group.getRoot()) {
            ret = createMultipleInputNode(group, visitedMap);
        } else {
            ret = wrapper;
        }
        visitedMap.put(wrapper, ret);
        return ret;
    }

    private static WrappedNode<?> createMultipleInputNode(
            MultipleInputGroup group,
            Map<WrappedNode<?>, WrappedNode<?>> visitedMap) {
        List<Tuple3<WrappedNode<?>, InputProperty, WrappedEdge<?>>> inputs = new ArrayList<>();
        List<WrappedNode<?>> members = group.getMembers();
        for (WrappedNode<?> member : members) {
            List<WrappedNode<?>> memberInputs = member.getInputs();
            for (int i = 0; i < memberInputs.size(); i++) {
                WrappedNode<?> memberInput = memberInputs.get(i);
                if (members.contains(memberInput)) {
                    continue;
                }
                checkState(
                        visitedMap.containsKey(memberInput),
                        "Input of a multiple input member is not visited. This is a bug.");
                WrappedNode<?> inputNode = visitedMap.get(memberInput);
                InputProperty inputProperty = member.getInputProperties().get(i);
                WrappedEdge<?> edge = member.getInputEdges().get(i);
                inputs.add(Tuple3.of(inputNode, inputProperty, edge));
            }
        }
        return createBatchMultipleInputNode(group, inputs);
    }

    private static WrappedNode<?> createBatchMultipleInputNode(
            MultipleInputGroup group,
            List<Tuple3<WrappedNode<?>, InputProperty, WrappedEdge<?>>> inputs) {
        Set<WrappedNode<?>> inputSet = new HashSet<>();
        for (Tuple3<WrappedNode<?>, InputProperty, WrappedEdge<?>> tuple3 : inputs) {
            inputSet.add(tuple3.f0);
        }
        WrappedNode<?> rootNode = group.getRoot();
        InputOrderCalculator calculator = new InputOrderCalculator(
                rootNode, inputSet,
                InputProperty.DamBehavior.BLOCKING);
        Map<WrappedNode<?>, Integer> inputOrderMap = calculator.calculate();

        // then create input reals and edges with the input orders
        List<WrappedNode<?>> inputNodes = new ArrayList<>();
        List<InputProperty> inputProperties = new ArrayList<>();
        List<WrappedEdge<?>> originalEdges = new ArrayList<>();
        for (Tuple3<WrappedNode<?>, InputProperty, WrappedEdge<?>> tuple3 : inputs) {
            WrappedNode<?> inputNode = tuple3.f0;
            InputProperty originalInputProperty = tuple3.f1;
            WrappedEdge<?> originalEdge = tuple3.f2;
            inputNodes.add(inputNode);
            inputProperties.add(
                    InputProperty.builder()
                            .requiredDistribution(originalInputProperty.getRequiredDistribution())
                            .damBehavior(originalInputProperty.getDamBehavior())
                            .priority(inputOrderMap.get(inputNode))
                            .build());
            originalEdges.add(originalEdge);
        }

        String description = MultipleInputUtil.getMultipleInputDescription(
                rootNode,
                inputSet,
                new ArrayList<>());

        BatchStreamMultipleInput multipleInput = new BatchStreamMultipleInput(
                inputProperties,
                rootNode,
                group.getMembers(),
                originalEdges,
                description
        );

        checkState(inputNodes.size() == originalEdges.size());
        List<WrappedEdge<?>> inputEdges = new ArrayList<>(inputNodes.size());

        WrappedNode<?> wrappedMultipleInput = new WrappedStreamMultipleInputNode(multipleInput);
        for (int i = 0; i < inputNodes.size(); i++) {
            WrappedEdge<?> originalInputEdge = originalEdges.get(i);
            WrappedNode<?> source = inputNodes.get(i);
            inputEdges.add(new WrappedStreamEdge(
                    (ImmutableStreamEdge) originalInputEdge.getEdge(),
                    source,
                    wrappedMultipleInput,
                    originalInputEdge.isFrozen()));
        }
        multipleInput.setInputEdges(inputEdges);

        return wrappedMultipleInput;
    }

    private static MultipleInputNodeCreateResult generateMultipleInputNodeResult(List<WrappedNode<?>> rootWrappers) {
        final MultipleInputNodeCreateResult result = new MultipleInputNodeCreateResult();
        AbstractWrappedNodeExactlyOnceVisitor visitor = new AbstractWrappedNodeExactlyOnceVisitor() {
            @Override
            protected void visitNode(WrappedNode<?> node) {
                if (node instanceof WrappedStreamMultipleInputNode) {
                    result.addCreatedNode(node);
                    BatchStreamMultipleInput originalNode = (BatchStreamMultipleInput) node.getNode();
                    for (WrappedNode<?> member : originalNode.getMemberNodes()) {
                        result.addRemovedNode(member);
                    }
                }
                visitInputs(node);
            }
        };
        rootWrappers.forEach(n -> n.accept(visitor));
        return result;
    }
}
