/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.api.graph.multinput;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.operators.ResourceSpec;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.ExecutionOptions;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.core.memory.ManagedMemoryUseCase;
import org.apache.flink.runtime.jobgraph.JobType;
import org.apache.flink.runtime.jobgraph.forwardgroup.ForwardGroupComputeUtil;
import org.apache.flink.runtime.jobgraph.forwardgroup.StreamNodeForwardGroup;
import org.apache.flink.streaming.api.graph.StreamEdge;
import org.apache.flink.streaming.api.graph.StreamGraph;
import org.apache.flink.streaming.api.graph.StreamNode;
import org.apache.flink.streaming.api.graph.StreamingJobGraphGenerator;
import org.apache.flink.streaming.api.graph.multinput.input.InputSpec;
import org.apache.flink.streaming.api.operators.ChainingStrategy;
import org.apache.flink.streaming.api.operators.SourceOperatorFactory;
import org.apache.flink.streaming.api.operators.StreamOperatorFactory;
import org.apache.flink.streaming.api.transformations.AbstractMultipleInputTransformation;
import org.apache.flink.streaming.api.transformations.MultipleInputTransformation;
import org.apache.flink.streaming.runtime.io.MultipleInputSelectionHandler;
import org.apache.flink.streaming.runtime.partitioner.ForwardPartitioner;
import org.apache.flink.table.planner.plan.nodes.exec.InputProperty;
import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Queue;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.flink.streaming.api.graph.SimpleTransformationTranslator.configure;
import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

@Internal
public class MultipleInputNodeCreationProcessor {

    private static final Logger LOG =
            LoggerFactory.getLogger(MultipleInputNodeCreationProcessor.class);

    private static List<StreamNode> filterInputPropertiesUnKnownNodes(
            StreamGraph streamGraph, Set<StreamNode> frozenNodes) {
        return streamGraph.getStreamNodes().stream()
                .filter(node -> !frozenNodes.contains(node))
                .filter(node -> node.getInputProperties().isEmpty() && !node.getInEdges().isEmpty())
                .collect(Collectors.toList());
    }

    private static final UpdatedResult EMPTY_RESULT =
            new UpdatedResult(
                    Collections.emptyMap(), Collections.emptySet(), Collections.emptySet());

    @Internal
    public static class UpdatedResult {
        private final Map<Integer, Collection<Integer>> newNodeIdToOriginalNodes;
        private final Set<StreamEdge> innerEdges;
        private final Set<StreamNode> innerNodes;

        public UpdatedResult(
                Map<Integer, Collection<Integer>> newNodeIdToOriginalNodes,
                Set<StreamEdge> innerEdges,
                Set<StreamNode> innerNodes) {
            this.newNodeIdToOriginalNodes = checkNotNull(newNodeIdToOriginalNodes);
            this.innerEdges = checkNotNull(innerEdges);
            this.innerNodes = checkNotNull(innerNodes);
        }

        public Map<Integer, Collection<Integer>> getNewNodeIdToOriginalNodes() {
            return Collections.unmodifiableMap(newNodeIdToOriginalNodes);
        }

        public boolean isUpdated() {
            return !newNodeIdToOriginalNodes.isEmpty();
        }

        public void merge(UpdatedResult updatedResult) {
            this.newNodeIdToOriginalNodes.putAll(updatedResult.newNodeIdToOriginalNodes);
            this.innerEdges.addAll(updatedResult.innerEdges);
            this.innerNodes.addAll(updatedResult.innerNodes);
        }

        public Set<StreamEdge> getInnerEdges() {
            return innerEdges;
        }

        public Set<StreamNode> getInnerNodes() {
            return innerNodes;
        }
    }

    public static UpdatedResult process(
            ClassLoader userClassLoader,
            StreamGraph streamGraph,
            Set<StreamNode> frozenNodes,
            Map<Integer, StreamNodeForwardGroup> streamNodeIdToForwardGroupMap) {
        boolean isStreaming = streamGraph.getJobType() == JobType.STREAMING;
        if (isStreaming) {
            LOG.info("Multiple input currently is only supported in BATCH mode.");
            return EMPTY_RESULT;
        }

        List<StreamNode> nodes = filterInputPropertiesUnKnownNodes(streamGraph, frozenNodes);
        if (!nodes.isEmpty()) {
            LOG.info(
                    "Exists some nodes {} input properties are known, can not to create multiple input nodes.",
                    nodes);
            return EMPTY_RESULT;
        }

        StreamExecGraph execGraph = new StreamExecGraph(streamGraph, frozenNodes);

        if (!isStreaming) {
            // As multiple input nodes use function call to deliver records between sub-operators,
            // we cannot rely on network buffers to buffer records not yet ready to be read,
            // so only BLOCKING dam behavior is safe here.
            // If conflict is detected under this stricter constraint,
            // we add a PIPELINED exchange to mark that its input and output node cannot be merged
            // into the same multiple input node
            InputPriorityConflictResolver resolver =
                    new InputPriorityConflictResolver(
                            execGraph, InputProperty.DamBehavior.BLOCKING);
            resolver.detectAndResolve();
        }

        List<StreamNodeWrapper> rootWrappers = wrapExecNodes(execGraph);
        // sort all nodes in topological order, sinks come first and sources come last
        List<StreamNodeWrapper> orderedWrappers = topologicalSort(rootWrappers);
        // group nodes into multiple input groups
        createMultipleInputGroups(
                execGraph, orderedWrappers, frozenNodes, streamNodeIdToForwardGroupMap);
        // apply optimizations to remove unnecessary nodes out of multiple input groups
        optimizeMultipleInputGroups(
                execGraph,
                streamGraph,
                orderedWrappers,
                streamNodeIdToForwardGroupMap,
                frozenNodes);

        // create the real multiple input nodes
        UpdatedResult result = new UpdatedResult(new HashMap<>(), new HashSet<>(), frozenNodes);

        // first process node near to sources
        Map<StreamExecNode, StreamExecNode> originalRootNodeToMultiInputNode = new HashMap<>();
        for (int i = orderedWrappers.size() - 1; i >= 0; i--) {
            StreamNodeWrapper wrapper = orderedWrappers.get(i);
            if (wrapper.group != null && wrapper == wrapper.group.root) {
                result.merge(
                        createMultipleInputNode(
                                userClassLoader,
                                streamGraph,
                                execGraph,
                                wrapper.group,
                                originalRootNodeToMultiInputNode,
                                streamNodeIdToForwardGroupMap));
            }
        }
        return result;
    }

    // --------------------------------------------------------------------------------
    // Wrapping and Sorting
    // --------------------------------------------------------------------------------

    private static List<StreamNodeWrapper> wrapExecNodes(StreamExecGraph streamExecGraph) {
        Map<StreamExecNode, StreamNodeWrapper> wrapperMap = new HashMap<>();
        AbstractStreamExecNodeExactlyOnceVisitor visitor =
                new AbstractStreamExecNodeExactlyOnceVisitor() {
                    @Override
                    protected void visitNode(StreamExecNode node) {
                        if (streamExecGraph.getFrozenNodes().contains(node)) {
                            return;
                        }

                        StreamNodeWrapper wrapper =
                                wrapperMap.computeIfAbsent(node, k -> new StreamNodeWrapper(node));
                        for (StreamExecEdge inputEdge : node.getInputEdges()) {
                            StreamExecNode inputNode = inputEdge.getSource();
                            int typeNumber = inputEdge.getTypeNumber();
                            StreamNodeWrapper inputWrapper =
                                    wrapperMap.computeIfAbsent(
                                            inputNode, k -> new StreamNodeWrapper(inputNode));
                            StreamEdgeWrapper edgeWrapper =
                                    new StreamEdgeWrapper(inputEdge, inputWrapper, wrapper);
                            wrapper.inputs
                                    .computeIfAbsent(typeNumber, k -> new ArrayList<>())
                                    .add(edgeWrapper);
                            inputWrapper.outputs.add(edgeWrapper);
                        }
                        visitInputs(node);
                    }
                };
        streamExecGraph.getRoots().forEach(visitor::visit);

        List<StreamNodeWrapper> rootWrappers = new ArrayList<>();
        for (StreamExecNode root : streamExecGraph.getRoots()) {
            StreamNodeWrapper rootWrapper = wrapperMap.get(root);
            Preconditions.checkNotNull(rootWrapper, "Root node is not wrapped. This is a bug.");
            rootWrappers.add(rootWrapper);
        }
        return rootWrappers;
    }

    private static List<StreamNodeWrapper> topologicalSort(List<StreamNodeWrapper> rootWrappers) {
        List<StreamNodeWrapper> result = new ArrayList<>();
        Queue<StreamNodeWrapper> queue = new LinkedList<>(rootWrappers);
        Map<StreamNodeWrapper, Integer> visitCountMap = new HashMap<>();

        while (!queue.isEmpty()) {
            StreamNodeWrapper wrapper = queue.poll();
            result.add(wrapper);
            for (List<StreamEdgeWrapper> inputWrappers : wrapper.inputs.values()) {
                for (StreamEdgeWrapper inputEdgeWrapper : inputWrappers) {
                    StreamNodeWrapper inputWrapper = inputEdgeWrapper.sourceWrapper;
                    int visitCount =
                            visitCountMap.compute(inputWrapper, (k, v) -> v == null ? 1 : v + 1);
                    if (visitCount == inputWrapper.outputs.size()) {
                        queue.offer(inputWrapper);
                    }
                }
            }
        }

        return result;
    }

    // --------------------------------------------------------------------------------
    // Multiple Input Groups Creating
    // --------------------------------------------------------------------------------

    private static void createMultipleInputGroups(
            StreamExecGraph execGraph,
            List<StreamNodeWrapper> orderedWrappers,
            Set<StreamNode> frozenNodes,
            Map<Integer, StreamNodeForwardGroup> streamNodeIdToForwardGroupMap) {
        // wrappers are checked in topological order from sinks to sources
        for (StreamNodeWrapper wrapper : orderedWrappers) {
            // we skip nodes which cannot be a member of a multiple input node
            if (!canBeMultipleInputNodeMember(wrapper, frozenNodes)) {
                LOG.info("!canBeMultipleInputNodeMember {} ", wrapper.streamNode);
                continue;
            }

            checkNotNull(wrapper.streamNode.getStreamNode());

            // we first try to assign this wrapper into the same group with its outputs
            MultipleInputGroup outputGroup =
                    canBeInSameGroupWithOutputs(wrapper, execGraph, streamNodeIdToForwardGroupMap);
            if (outputGroup != null) {
                LOG.info("Can be in same group with output {}", wrapper.streamNode);
                outputGroup.addMember(wrapper);
                continue;
            }

            // we then try to create a new multiple input group with this node as the root
            if (canBeRootOfMultipleInputGroup(wrapper)) {
                LOG.info("Can be root {}", wrapper.streamNode);
                wrapper.group = new MultipleInputGroup(wrapper);
            } else {
                LOG.info("Can not be root {}", wrapper.streamNode);
            }

            // all our attempts failed, this node will not be in a multiple input node
        }
    }

    private static boolean canBeMultipleInputNodeMember(
            StreamNodeWrapper wrapper, Set<StreamNode> frozenStreamNodes) {
        // sources cannot be a member of multiple input node
        return wrapper.streamNode.getStreamNode() != null
                && !wrapper.streamNode.getInputEdges().isEmpty()
                && !frozenStreamNodes.contains(wrapper.streamNode)
                && !wrapper.streamNode.getStreamNode().isMultiInputNode();
    }

    /**
     * A node can only be assigned into the same multiple input group of its outputs if all outputs
     * have a group and are the same.
     *
     * @return the {@link MultipleInputGroup} of the outputs if all outputs have a group and are the
     *     same, null otherwise
     */
    private static MultipleInputGroup canBeInSameGroupWithOutputs(
            StreamNodeWrapper wrapper,
            StreamExecGraph execGraph,
            Map<Integer, StreamNodeForwardGroup> streamNodeIdToForwardGroupMap) {
        if (wrapper.outputs.isEmpty()) {
            LOG.info("{} outputs is empty", wrapper.streamNode);
            return null;
        }

        if (wrapper.outputs.stream()
                .anyMatch(
                        edgeWrapper ->
                                edgeWrapper.targetWrapper.streamNode.getStreamNode() == null)) {
            LOG.info("{} out node is null", wrapper.streamNode);
            return null;
        }

        MultipleInputGroup outputGroup = wrapper.outputs.get(0).targetWrapper.group;
        if (outputGroup == null) {
            LOG.info("{} out group is null", wrapper.streamNode);
            return null;
        }

        for (StreamEdgeWrapper edgeWrapper : wrapper.outputs) {
            StreamNodeWrapper outputWrapper = edgeWrapper.targetWrapper;
            if (outputWrapper.group != outputGroup) {
                LOG.info(
                        "{} not equals to out group {}",
                        wrapper.streamNode,
                        outputWrapper.streamNode);
                return null;
            }

            if (!Objects.equals(
                    outputWrapper.streamNode.getStreamNode().getSlotSharingGroup(),
                    wrapper.streamNode.getStreamNode().getSlotSharingGroup())) {
                LOG.info(
                        "{} can not be same slot sharing group {}",
                        wrapper.streamNode,
                        outputWrapper.streamNode);
                return null;
            }

            for (StreamEdgeWrapper streamEdgeWrapper : wrapper.outputs) {

                if (streamEdgeWrapper.streamEdge.isBroken()) {
                    LOG.info("{} can not be include in", streamEdgeWrapper.streamEdge);
                    return null;
                }

                if (!(execGraph.getStreamEdge(streamEdgeWrapper.streamEdge).getPartitioner()
                                instanceof ForwardPartitioner
                        && ForwardGroupComputeUtil.canTargetMergeIntoSourceForwardGroup(
                                streamNodeIdToForwardGroupMap.get(
                                        wrapper.streamNode.getStreamNode().getId()),
                                streamNodeIdToForwardGroupMap.get(
                                        outputWrapper.streamNode.getStreamNode().getId())))) {
                    LOG.info(
                            "{} not forward to {}, the original partitioner is {}, and the source fgroup is {}, the target fgroup is {}",
                            wrapper.streamNode,
                            outputWrapper.streamNode.getStreamNode(),
                            execGraph
                                    .getStreamEdge(streamEdgeWrapper.streamEdge)
                                    .getPartitioner()
                                    .getClass(),
                            streamNodeIdToForwardGroupMap.get(
                                    wrapper.streamNode.getStreamNode().getId()),
                            streamNodeIdToForwardGroupMap.get(
                                    outputWrapper.streamNode.getStreamNode().getId()));
                    return null;
                }
            }
        }

        return outputGroup;
    }

    private static boolean canBeRootOfMultipleInputGroup(StreamNodeWrapper wrapper) {
        // A node with more than one input can be the root, and one-input operator can also be root
        // if the upstream node only contain Calc&HashJoin&HashAgg and operator fusion codegen
        // enabled
        LOG.info("{} inputs are {}.", wrapper.streamNode, wrapper.inputs);
        return wrapper.inputs.size() >= 2;
    }

    // --------------------------------------------------------------------------------
    // Multiple Input Groups Optimizing
    // --------------------------------------------------------------------------------

    private static void optimizeMultipleInputGroups(
            StreamExecGraph execGraph,
            StreamGraph streamGraph,
            List<StreamNodeWrapper> orderedWrappers,
            Map<Integer, StreamNodeForwardGroup> streamNodeIdToForwardGroupMap,
            Set<StreamNode> frozenNodes) {
        // wrappers are checked in topological order from sources to sinks
        for (int i = orderedWrappers.size() - 1; i >= 0; i--) {
            StreamNodeWrapper wrapper = orderedWrappers.get(i);
            MultipleInputGroup group = wrapper.group;
            if (group == null) {
                // we only consider nodes currently in a multiple input group
                continue;
            }
            if (!isEntranceOfMultipleInputGroup(wrapper)) {
                // we're not removing a node from the middle of a multiple input group
                continue;
            }

            boolean shouldRemove = false;
            if (wrapper.inputs.size() == 1) {
                // optimization 2. for one-input operators we'll remove it unless its input
                // is an exchange or a FLIP-27 source, this is mainly to avoid the following
                // pattern:
                // non-chainable source -> calc --\
                //                                 join ->
                // non-chainable source -> calc --/
                // if we move two calcs into the multiple input group rooted at the join, we're
                // directly shuffling large amount of records from the source without filtering
                // by the calc
                List<StreamEdgeWrapper> inputWrappers = wrapper.inputs.values().iterator().next();

                shouldRemove =
                        inputWrappers.stream()
                                .anyMatch(
                                        input ->
                                                !frozenNodes.contains(
                                                                input.sourceWrapper.streamNode
                                                                        .getStreamNode())
                                                        && StreamingJobGraphGenerator.isChainable(
                                                                execGraph.getStreamEdge(
                                                                        input.streamEdge),
                                                                streamGraph)
                                                        && !isChainableWithSource(
                                                                input.sourceWrapper.streamNode
                                                                        .getStreamNode(),
                                                                wrapper.streamNode.getStreamNode(),
                                                                streamGraph,
                                                                streamNodeIdToForwardGroupMap));
            }

            // optimization 3. for singleton operations (for example singleton global agg)
            // we're not including it into the multiple input node as we have to ensure that
            // the whole multiple input can only have 1 parallelism.
            // continuous singleton operations connected by forwarding shuffle will be dealt
            // together with optimization 3
            shouldRemove |= wrapper.streamNode.getStreamNode().getMaxParallelism() == 1;

            if (shouldRemove) {
                LOG.info("### remove node {}", wrapper.streamNode);
                wrapper.group.removeMember(wrapper);
            }
        }

        // wrappers are checked in topological order from sinks to sources
        for (StreamNodeWrapper wrapper : orderedWrappers) {
            MultipleInputGroup group = wrapper.group;
            if (group == null) {
                // we only consider nodes currently in a multiple input group
                continue;
            }
            if (wrapper != wrapper.group.root) {
                // we only consider nodes at the root of the multiple input
                continue;
            }

            if (group.members.size() == 1) {
                // optimization 4. we clean up multiple input groups with only 1 member,
                // unless one of its input is a FLIP-27 source (for maximizing source chaining),
                // however unions do not apply to this optimization because they're not real
                // operators
                if (wrapper.inputs.values().stream()
                        .flatMap(List::stream)
                        .noneMatch(
                                inputWrapper ->
                                        !frozenNodes.contains(
                                                        inputWrapper.sourceWrapper.streamNode
                                                                .getStreamNode())
                                                && isChainableWithSource(
                                                        inputWrapper.sourceWrapper.streamNode
                                                                .getStreamNode(),
                                                        wrapper.streamNode.getStreamNode(),
                                                        streamGraph,
                                                        streamNodeIdToForwardGroupMap))) {

                    LOG.info(
                            "### remove node {} for op4, which has inputs {}",
                            wrapper.streamNode,
                            wrapper.streamNode.getInputEdges());
                    if (wrapper.streamNode.getStreamNode() != null) {
                        List<StreamNode> noFrozenSources =
                                wrapper.streamNode.getStreamNode().getInEdges().stream()
                                        .map(StreamEdge::getSourceId)
                                        .map(streamGraph::getStreamNode)
                                        .filter(s -> !frozenNodes.contains(s))
                                        .collect(Collectors.toList());

                        List<StreamNode> fronzenNodes =
                                wrapper.streamNode.getStreamNode().getInEdges().stream()
                                        .map(StreamEdge::getSourceId)
                                        .map(streamGraph::getStreamNode)
                                        .filter(frozenNodes::contains)
                                        .collect(Collectors.toList());

                        LOG.info(
                                "{} has no FrozenSources are {}",
                                wrapper.streamNode.getStreamNode(),
                                noFrozenSources);
                        LOG.info(
                                "{} has fronzen sources are {}",
                                wrapper.streamNode.getStreamNode(),
                                fronzenNodes);
                    }
                    wrapper.group.removeRoot();
                } else {
                    LOG.info(
                            "### {} could be single multi input member because the input is chainable source",
                            wrapper.streamNode);
                }
                continue;
            }

            if (wrapper.inputs.size() == 1) {
                // optimization 6. operators with only 1 input are not allowed to be the root,
                // as their chaining will be handled by operator chains.
                LOG.info("remove root for {}", wrapper.streamNode);
                wrapper.group.removeRoot();
            }
        }
    }

    private static boolean isEntranceOfMultipleInputGroup(StreamNodeWrapper wrapper) {
        Preconditions.checkNotNull(
                wrapper.group,
                "Exec node wrapper does not have a multiple input group. This is a bug.");
        for (List<StreamEdgeWrapper> edgeWrappers : wrapper.inputs.values()) {
            for (StreamEdgeWrapper edgeWrapper : edgeWrappers) {
                StreamNodeWrapper inputWrapper = edgeWrapper.sourceWrapper;
                if (inputWrapper.group == wrapper.group) {
                    // one of the input is in the same group, so this node is not the entrance of
                    // the
                    // group
                    return false;
                }
            }
        }
        return true;
    }

    @VisibleForTesting
    static boolean isChainableWithSource(
            @Nullable StreamNode upstreamNode,
            StreamNode currentNode,
            StreamGraph streamGraph,
            Map<Integer, StreamNodeForwardGroup> streamNodeIdToForwardGroupMap) {
        return upstreamNode != null
                && upstreamNode.getOperatorFactory() instanceof SourceOperatorFactory
                && streamGraph.getStreamEdges(upstreamNode.getId(), currentNode.getId()).stream()
                        .allMatch(e -> e.getPartitioner() instanceof ForwardPartitioner)
                && ForwardGroupComputeUtil.canTargetMergeIntoSourceForwardGroup(
                        streamNodeIdToForwardGroupMap.get(upstreamNode.getId()),
                        streamNodeIdToForwardGroupMap.get(currentNode.getId()));
    }

    // --------------------------------------------------------------------------------
    // Multiple Input Nodes Creating
    // --------------------------------------------------------------------------------

    private static UpdatedResult createMultipleInputNode(
            ClassLoader userClassLoader,
            StreamGraph streamGraph,
            StreamExecGraph execGraph,
            MultipleInputGroup group,
            Map<StreamExecNode, StreamExecNode> originalRootNodeToMultiInputNode,
            Map<Integer, StreamNodeForwardGroup> streamNodeIdToForwardGroupMap) {
        // calculate the inputs of the multiple input node
        List<Tuple3<StreamExecNode, InputProperty, StreamExecEdge>> inputs = new ArrayList<>();
        List<StreamNodeForwardGroup> forwardGroups = new ArrayList<>();
        Set<StreamNode> upStreamSources = new HashSet<>();
        for (StreamNodeWrapper member : group.members) {
            Map<Integer, InputProperty> inputPropertiesByTypeNumber =
                    member.streamNode.getStreamNode().getInputPropertiesByTypeNumber();
            for (List<StreamEdgeWrapper> inputWrapperList : member.inputs.values()) {
                for (int i = 0; i < inputWrapperList.size(); i++) {
                    StreamEdgeWrapper edgeWrapper = inputWrapperList.get(i);
                    StreamNodeWrapper memberInput = edgeWrapper.sourceWrapper;
                    if (group.members.contains(memberInput)) {
                        continue;
                    }
                    StreamExecEdge edge = edgeWrapper.streamEdge;
                    // maybe update edge because the source of edge may be updated
                    if (originalRootNodeToMultiInputNode.containsKey(edge.getSource())) {
                        edge.setSource(originalRootNodeToMultiInputNode.get(edge.getSource()));
                        memberInput.streamNode = edge.getSource();
                    }

                    InputProperty inputProperty =
                            inputPropertiesByTypeNumber.get(edge.getTypeNumber());
                    inputs.add(Tuple3.of(memberInput.streamNode, inputProperty, edge));

                    StreamNode upstreamNode = memberInput.streamNode.getStreamNode();
                    if (upstreamNode != null
                            && upstreamNode.getOperatorFactory() instanceof SourceOperatorFactory
                            && streamGraph
                                    .getStreamEdges(
                                            upstreamNode.getId(),
                                            member.streamNode.getStreamNode().getId())
                                    .stream()
                                    .allMatch(
                                            e ->
                                                    e.getPartitioner()
                                                            instanceof ForwardPartitioner)) {
                        upStreamSources.add(upstreamNode);
                    }
                }
            }

            member.streamNode
                    .getStreamNode()
                    .getOperatorFactory()
                    .generateOperatorFactory(
                            userClassLoader,
                            streamGraph.getJobConfiguration(),
                            member.streamNode.getStreamNode(),
                            member.streamNode);
            LOG.info("MultiInput contains {}", member.streamNode);
            if (streamNodeIdToForwardGroupMap.get(member.streamNode.getStreamNode().getId())
                    != null) {
                forwardGroups.add(
                        streamNodeIdToForwardGroupMap.get(
                                member.streamNode.getStreamNode().getId()));
            }
        }

        for (int i = 0; i < forwardGroups.size() - 1; i++) {
            StreamNodeForwardGroup group1 = forwardGroups.get(i);
            StreamNodeForwardGroup group2 = forwardGroups.get(i + 1);
            mergeForwardGroups(group1, group2, streamNodeIdToForwardGroupMap);
        }

        StreamNodeForwardGroup targetGroup = null;
        if (!forwardGroups.isEmpty()) {
            targetGroup = forwardGroups.get(0);
            for (StreamNode upStreamSource : upStreamSources) {
                LOG.info(
                        "Merge forward group with upStreamSource {} with p {} and maxP {}",
                        upStreamSource,
                        upStreamSource.getParallelism(),
                        upStreamSource.getMaxParallelism());
                if (mergeForwardGroups(
                        streamNodeIdToForwardGroupMap.get(upStreamSource.getId()),
                        targetGroup,
                        streamNodeIdToForwardGroupMap)) {
                    LOG.info("merge forward group success");
                } else {
                    LOG.info("merge forward group failed");
                }
            }
        }

        StreamNode batchMultipleInputNode =
                createBatchMultipleInputNode(streamGraph, execGraph, group.root.streamNode, inputs);
        LOG.info("Creat dynamic multiInput node [{}]", batchMultipleInputNode);
        for (StreamEdge outEdge : group.root.streamNode.getStreamNode().getOutEdges()) {
            execGraph.replaceEdge(
                    outEdge,
                    streamGraph.addEdge(
                            batchMultipleInputNode.getId(),
                            outEdge.getTargetId(),
                            outEdge.getTypeNumber(),
                            outEdge.getPartitioner(),
                            outEdge.getExchangeMode(),
                            outEdge.getIntermediateDatasetIdToProduce()));
        }

        // update exec graph to make sure the downstream multi-input creation is correct
        originalRootNodeToMultiInputNode.put(
                group.root.streamNode, new StreamExecNode(batchMultipleInputNode));

        Set<StreamNode> toBeRemovedNodes =
                group.members.stream()
                        .map(wrapper -> wrapper.streamNode.getStreamNode())
                        .collect(Collectors.toSet());
        Set<Integer> toBeRemovedNodeIds =
                toBeRemovedNodes.stream().map(StreamNode::getId).collect(Collectors.toSet());
        if (targetGroup != null) {
            streamNodeIdToForwardGroupMap.put(batchMultipleInputNode.getId(), targetGroup);
        }

        return new UpdatedResult(
                Map.of(batchMultipleInputNode.getId(), toBeRemovedNodeIds),
                streamGraph.removeStreamNodesAndAllInputOutputEdges(toBeRemovedNodeIds),
                toBeRemovedNodes);
    }

    // && mergeForwardGroups(
    //                                wrapper.streamNode.getStreamNode().getId(),
    //                                outputWrapper.streamNode.getStreamNode().getId(),
    //                                streamNodeIdToForwardGroupMap)
    private static boolean mergeForwardGroups(
            StreamNodeForwardGroup sourceForwardGroup,
            StreamNodeForwardGroup forwardGroupToMerge,
            Map<Integer, StreamNodeForwardGroup> streamNodeIdToForwardGroupMap) {
        if (sourceForwardGroup == null || forwardGroupToMerge == null) {
            return false;
        }
        if (!sourceForwardGroup.mergeForwardGroup(forwardGroupToMerge)) {
            return false;
        }
        // Update streamNodeIdToForwardGroupMap.
        forwardGroupToMerge
                .getVertexIds()
                .forEach(nodeId -> streamNodeIdToForwardGroupMap.put(nodeId, sourceForwardGroup));
        return true;
    }

    private static StreamNode createBatchMultipleInputNode(
            StreamGraph streamGraph,
            StreamExecGraph execGraph,
            StreamExecNode streamExecNode,
            List<Tuple3<StreamExecNode, InputProperty, StreamExecEdge>> inputs) {
        // first calculate the input orders using InputPriorityConflictResolver
        Set<StreamExecNode> inputSet = new HashSet<>();
        for (Tuple3<StreamExecNode, InputProperty, StreamExecEdge> tuple3 : inputs) {
            inputSet.add(tuple3.f0);
        }
        InputOrderCalculator calculator =
                new InputOrderCalculator(
                        streamExecNode, inputSet, InputProperty.DamBehavior.BLOCKING);
        Map<StreamExecNode, Integer> inputOrderMap = calculator.calculate();

        // then create input rels and edges with the input orders
        String slotSharingGroup = streamExecNode.getStreamNode().getSlotSharingGroup();
        List<InputProperty> inputProperties = new ArrayList<>();
        List<StreamExecEdge> originalEdges = new ArrayList<>();
        Map<StreamExecEdge, InputProperty> inputEdgeToInputPropertyMap = new HashMap<>();
        for (Tuple3<StreamExecNode, InputProperty, StreamExecEdge> tuple3 : inputs) {
            StreamExecNode inputNode = tuple3.f0;
            InputProperty originalInputEdge = tuple3.f1;
            StreamExecEdge edge = tuple3.f2;
            InputProperty newProperty =
                    InputProperty.builder()
                            .requiredDistribution(originalInputEdge.getRequiredDistribution())
                            .damBehavior(originalInputEdge.getDamBehavior())
                            .priority(inputOrderMap.get(inputNode))
                            .build();
            inputProperties.add(newProperty);

            originalEdges.add(edge);
            inputEdgeToInputPropertyMap.put(edge, newProperty);
        }

        return translateToPlanInternal(
                originalEdges,
                streamExecNode,
                inputProperties,
                inputEdgeToInputPropertyMap,
                streamGraph,
                execGraph,
                slotSharingGroup);
    }

    protected static <OUT> StreamNode translateToPlanInternal(
            List<StreamExecEdge> inputEdges,
            StreamExecNode outputTransform,
            List<InputProperty> inputProperties,
            Map<StreamExecEdge, InputProperty> inputEdgeToInputPropertyMap,
            StreamGraph streamGraph,
            StreamExecGraph execGraph,
            String slotSharingGroup) {

        final int[] readOrders =
                inputProperties.stream().map(InputProperty::getPriority).mapToInt(i -> i).toArray();

        StreamOperatorFactory<OUT> operatorFactory;
        int parallelism;
        int maxParallelism;
        long memoryBytes;
        ResourceSpec minResources = null;
        ResourceSpec preferredResources = null;

        final TableOperatorWrapperGenerator generator =
                new TableOperatorWrapperGenerator(inputEdges, outputTransform, readOrders);
        generator.generate();

        // NOTE: the order is important
        final List<Tuple3<InputSpec, TypeInformation<?>, StreamExecEdge>>
                inputTransformAndInputSpecPairs = generator.getInputTransformAndInputSpecPairs();
        operatorFactory =
                new BatchMultipleInputStreamOperatorFactory(
                        inputTransformAndInputSpecPairs.stream()
                                .map(tuple3 -> tuple3.f0)
                                .collect(Collectors.toList()),
                        generator.getHeadWrappers(),
                        generator.getTailWrapper());
        List<InputProperty> propertyList =
                inputTransformAndInputSpecPairs.stream()
                        .map(tuple3 -> tuple3.f2)
                        .map(inputEdgeToInputPropertyMap::get)
                        .collect(Collectors.toList());

        parallelism = generator.getParallelism();
        maxParallelism = generator.getMaxParallelism();
        final int memoryWeight = generator.getManagedMemoryWeight();
        memoryBytes = (long) memoryWeight << 20;

        minResources = generator.getMinResources();
        preferredResources = generator.getPreferredResources();

        TypeInformation<OUT> outTypeInfo =
                (TypeInformation<OUT>) outputTransform.getStreamNode().getOutTypeInfo();
        final MultipleInputTransformation<OUT> multipleInputTransform =
                new MultipleInputTransformation<>(
                        createTransformationName(streamGraph.getJobConfiguration())
                                + (streamGraph.getMaxNodeId() + 1),
                        operatorFactory,
                        outTypeInfo,
                        parallelism,
                        generator.isParallelismConfigured(),
                        propertyList);
        multipleInputTransform.setDescription(
                createTransformationDescription(streamGraph.getJobConfiguration()));
        if (maxParallelism > 0) {
            multipleInputTransform.setMaxParallelism(maxParallelism);
        }

        // set resources
        if (minResources != null && preferredResources != null) {
            multipleInputTransform.setResources(minResources, preferredResources);
        }

        multipleInputTransform.setDescription(
                createTransformationDescription(streamGraph.getJobConfiguration()));
        setManagedMemoryWeight(multipleInputTransform, memoryBytes);

        // set chaining strategy for source chaining
        multipleInputTransform.setChainingStrategy(ChainingStrategy.HEAD_WITH_SOURCES);
        multipleInputTransform.setTransformationId(streamGraph.getMaxNodeId() + 1);

        return translateInternal(
                multipleInputTransform,
                streamGraph,
                slotSharingGroup,
                inputTransformAndInputSpecPairs.stream()
                        .map(
                                tuple3 ->
                                        new Tuple2<TypeInformation<?>, StreamEdge>(
                                                tuple3.f1, execGraph.getStreamEdge(tuple3.f2)))
                        .collect(Collectors.toList()));
    }

    public static <T> void setManagedMemoryWeight(
            Transformation<T> transformation, long memoryBytes) {
        if (memoryBytes > 0) {
            final int weightInMebibyte = Math.max(1, (int) (memoryBytes >> 20));
            final Optional<Integer> previousWeight =
                    transformation.declareManagedMemoryUseCaseAtOperatorScope(
                            ManagedMemoryUseCase.OPERATOR, weightInMebibyte);
            if (previousWeight.isPresent()) {
                throw new RuntimeException(
                        "Managed memory weight has been set, this should not happen.");
            }
        }
    }

    protected static String createTransformationName(ReadableConfig config) {
        return "A MultipleInputTransformation";
    }

    protected static String createTransformationDescription(ReadableConfig config) {
        return "A MultipleInputTransformation Description";
    }

    private static <OUT> StreamNode translateInternal(
            final AbstractMultipleInputTransformation<OUT> transformation,
            StreamGraph streamGraph,
            String slotSharingGroup,
            List<Tuple2<TypeInformation<?>, StreamEdge>> inTypeInfos) {
        checkNotNull(transformation);

        checkArgument(
                !inTypeInfos.isEmpty(),
                "Empty inputs for MultipleInputTransformation. Did you forget to add inputs?");
        MultipleInputSelectionHandler.checkSupportedInputCount(inTypeInfos.size());

        final int transformationId = transformation.getId();
        final ExecutionConfig executionConfig = streamGraph.getExecutionConfig();

        StreamNode streamNode =
                streamGraph.addMultipleInputOperator(
                        transformationId,
                        slotSharingGroup,
                        transformation.getCoLocationGroupKey(),
                        transformation.getOperatorFactory(),
                        // because we do not add real input transformations to multiInput
                        // transformation, here we should set actual input type infos
                        inTypeInfos.stream().map(tuple2 -> tuple2.f0).collect(Collectors.toList()),
                        transformation.getOutputType(),
                        transformation.getName(),
                        transformation.getInputProperties());
        streamNode.setMultiInputNode(false);
        streamGraph.setAttribute(transformationId, transformation.getAttribute());
        streamGraph.setParallelism(
                transformationId,
                transformation.getParallelism(),
                transformation.isParallelismConfigured());
        streamGraph.setMaxParallelism(transformationId, transformation.getMaxParallelism());

        int typeNumber = 1;
        for (Tuple2<TypeInformation<?>, StreamEdge> tuple2 : inTypeInfos) {
            StreamEdge edge = tuple2.f1;
            edge.setNewTargetWithTypeNumber(transformationId, typeNumber);
            streamNode.addInEdge(edge);
            typeNumber++;
        }

        LOG.info("Try to configure MultipleInputTransformation {}", transformation);
        configure(
                transformation,
                streamGraph,
                streamGraph.getJobConfiguration().get(ExecutionOptions.BUFFER_TIMEOUT_ENABLED)
                        ? streamGraph
                                .getJobConfiguration()
                                .get(ExecutionOptions.BUFFER_TIMEOUT)
                                .toMillis()
                        : ExecutionOptions.DISABLED_NETWORK_BUFFER_TIMEOUT);
        streamGraph.setSupportsConcurrentExecutionAttempts(
                transformationId, transformation.isSupportsConcurrentExecutionAttempts());
        return streamNode;
    }

    static String getMultipleInputDescription(
            StreamNode rootNode,
            List<StreamNode> inputNodes,
            List<InputProperty> inputProperties,
            StreamGraph streamGraph) {
        String members =
                ExecNodePlanDumper.treeToString(rootNode, inputNodes, true, streamGraph)
                        .replace("\n", "\\n");
        StringBuilder sb = new StringBuilder();
        sb.append("MultipleInput(");
        List<String> readOrders =
                inputProperties.stream()
                        .map(InputProperty::getPriority)
                        .map(Object::toString)
                        .collect(Collectors.toList());
        boolean hasDiffReadOrder = readOrders.stream().distinct().count() > 1;
        if (hasDiffReadOrder) {
            sb.append("readOrder=[").append(String.join(",", readOrders)).append("], ");
        }
        sb.append("members=[\\n").append(members).append("]");
        sb.append(")");
        return sb.toString();
    }

    // --------------------------------------------------------------------------------
    // Helper Classes
    // --------------------------------------------------------------------------------

    private static class StreamNodeWrapper {
        private StreamExecNode streamNode;
        private final Map<Integer, List<StreamEdgeWrapper>> inputs;
        private final List<StreamEdgeWrapper> outputs;
        private MultipleInputGroup group;

        private StreamNodeWrapper(StreamExecNode streamNode) {
            this.streamNode = streamNode;
            this.inputs = new HashMap<>();
            this.outputs = new ArrayList<>();
            this.group = null;
        }
    }

    private static class StreamEdgeWrapper {
        private final StreamExecEdge streamEdge;
        private final StreamNodeWrapper sourceWrapper;
        private final StreamNodeWrapper targetWrapper;

        private StreamEdgeWrapper(
                StreamExecEdge streamEdge,
                StreamNodeWrapper sourceWrapper,
                StreamNodeWrapper targetWrapper) {
            this.streamEdge = streamEdge;
            this.sourceWrapper = sourceWrapper;
            this.targetWrapper = targetWrapper;
        }
    }

    private static class MultipleInputGroup {
        // We use list instead of set here to ensure that the inputs of a multiple input node
        // will not change order. Although order changes do not affect the correctness of the
        // query, it does affect plan test cases.
        private final List<StreamNodeWrapper> members;

        private StreamNodeWrapper root;

        private MultipleInputGroup(StreamNodeWrapper root) {
            this.members = new ArrayList<>();
            members.add(root);
            this.root = root;
        }

        private void addMember(StreamNodeWrapper wrapper) {
            Preconditions.checkState(
                    wrapper.group == null,
                    "The given exec node wrapper is already in a multiple input group. This is a bug.");
            members.add(wrapper);
            wrapper.group = this;
        }

        private void removeMember(StreamNodeWrapper wrapper) {
            if (wrapper == root) {
                removeRoot();
            } else {
                Preconditions.checkState(
                        members.remove(wrapper),
                        "The given exec node wrapper does not exist in the multiple input group. This is a bug.");
                wrapper.group = null;
            }
        }

        private void removeRoot() {
            Preconditions.checkNotNull(
                    root, "Multiple input group does not have a root. This is a bug.");
            Set<StreamNodeWrapper> sameGroupInputWrappers = new HashSet<>();
            for (List<StreamEdgeWrapper> inputWrappers : root.inputs.values()) {
                for (StreamEdgeWrapper inputWrapper : inputWrappers) {
                    if (members.contains(inputWrapper.sourceWrapper)) {
                        sameGroupInputWrappers.add(inputWrapper.sourceWrapper);
                    }
                }
            }
            Preconditions.checkState(
                    sameGroupInputWrappers.size() < 2,
                    "There are two or more inputs of the root remaining in the multiple input group. This is a bug.");

            members.remove(root);
            root.group = null;
            if (sameGroupInputWrappers.isEmpty()) {
                root = null;
            } else {
                root = sameGroupInputWrappers.iterator().next();
            }
        }
    }
}
