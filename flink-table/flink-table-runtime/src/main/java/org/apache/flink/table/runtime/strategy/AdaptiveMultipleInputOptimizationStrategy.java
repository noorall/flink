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

package org.apache.flink.table.runtime.strategy;

import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.runtime.scheduler.adaptivebatch.OperatorsFinished;
import org.apache.flink.runtime.scheduler.adaptivebatch.StreamGraphOptimizationStrategy;
import org.apache.flink.streaming.api.graph.StreamGraphContext;
import org.apache.flink.streaming.api.graph.StreamNode;
import org.apache.flink.streaming.api.graph.util.ImmutableStreamEdge;
import org.apache.flink.streaming.api.graph.util.StreamEdgeUpdateRequestInfo;
import org.apache.flink.table.runtime.strategy.multipleinput.MultipleInputNodeCreateResult;
import org.apache.flink.table.runtime.strategy.multipleinput.MultipleInputNodeCreationProcessor;
import org.apache.flink.table.runtime.strategy.multipleinput.node.BatchStreamMultipleInput;
import org.apache.flink.table.runtime.strategy.multipleinput.wrapper.WrappedEdge;
import org.apache.flink.table.runtime.strategy.multipleinput.wrapper.WrappedNode;
import org.apache.flink.table.runtime.strategy.multipleinput.wrapper.WrappedStreamEdge;
import org.apache.flink.table.runtime.strategy.multipleinput.wrapper.WrappedStreamMultipleInputNode;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.flink.util.Preconditions.checkState;

public class AdaptiveMultipleInputOptimizationStrategy implements StreamGraphOptimizationStrategy {
    ReadableConfig streamGraphConfig;

    @Override
    public void initialize(StreamGraphContext context) {
        streamGraphConfig = context.getStreamGraph().getConfiguration();
    }

    @Override
    public boolean onOperatorsFinished(
            OperatorsFinished operatorsFinished,
            StreamGraphContext context) throws Exception {
        return false;
    }

    private boolean applyAdaptiveMultipleInputOptimizationStrategy(StreamGraphContext context) {
        MultipleInputNodeCreateResult result = MultipleInputNodeCreationProcessor.process(
                context.getStreamGraph(),
                context.getFrozenNodeIds());
        if (result.isEmpty()) {
            return false;
        }
        List<WrappedNode<?>> removedNodes = result.getRemovedNodes();
        List<WrappedNode<?>> createdNodes = result.getCreatedNodes();
        checkState(!removedNodes.isEmpty() && !createdNodes.isEmpty());

        List<Integer> nodesToRemove = removedNodes
                .stream()
                .map(WrappedNode::getNodeId)
                .collect(Collectors.toList());
        List<StreamNode> nodesToAdd = new ArrayList<>();
        List<StreamEdgeUpdateRequestInfo> edgesToUpdate = new ArrayList<>();
        for (WrappedNode<?> node : createdNodes) {
            checkState(node instanceof WrappedStreamMultipleInputNode);
            BatchStreamMultipleInput nodeToAdd = (BatchStreamMultipleInput) node.getNode();
            nodesToAdd.add(nodeToAdd.transToStreamNode(context));
            edgesToUpdate.addAll(generateStreamEdgeUpdateInfos(node));
        }
        return true;
    }

    private List<StreamEdgeUpdateRequestInfo> generateStreamEdgeUpdateInfos(WrappedNode<?> node) {
        checkState(node instanceof WrappedStreamMultipleInputNode);
        List<StreamEdgeUpdateRequestInfo> result = new ArrayList<>();
        for (WrappedEdge<?> inEdge : node.getInputEdges()) {
            checkState(inEdge instanceof WrappedStreamEdge);
            ImmutableStreamEdge originEdge = (ImmutableStreamEdge) inEdge.getEdge();
            if (originEdge.getTargetId() != node.getNodeId()) {
                checkState(inEdge.isFrozen());
                result.add(new StreamEdgeUpdateRequestInfo(
                        originEdge.getEdgeId(),
                        originEdge.getSourceId(),
                        originEdge.getTargetId()).withNewTargetId(node.getNodeId()));
            }
        }
        for (WrappedEdge<?> outEdge : node.getOutputEdges()) {
            checkState(outEdge instanceof WrappedStreamEdge);
            ImmutableStreamEdge originEdge = (ImmutableStreamEdge) outEdge.getEdge();
            if (originEdge.getSourceId() != node.getNodeId()) {
                checkState(outEdge.isFrozen());
                result.add(new StreamEdgeUpdateRequestInfo(
                        originEdge.getEdgeId(),
                        originEdge.getSourceId(),
                        originEdge.getTargetId()).withNewSourceId(node.getNodeId()));
            }
        }
        return result;
    }
}
