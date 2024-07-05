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

package org.apache.flink.streaming.api.graph;

import org.apache.flink.runtime.jobgraph.forwardgroup.StreamNodeForwardGroup;
import org.apache.flink.streaming.runtime.partitioner.ForwardPartitioner;
import org.apache.flink.streaming.runtime.partitioner.RescalePartitioner;
import org.apache.flink.streaming.runtime.partitioner.StreamPartitioner;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

public class StreamGraphManagerContext {
    private static final Logger LOG = LoggerFactory.getLogger(StreamGraphManagerContext.class);

    private final StreamGraph streamGraph;
    private final Map<Integer, StreamNodeForwardGroup> forwardGroupsByEndpointNodeIdCache;
    private final Map<Integer, Integer> frozenNodeToStartNodeMap;
    private final Map<Integer, Map<StreamEdge, NonChainedOutput>> opIntermediateOutputsCaches;

    public StreamGraphManagerContext(
            Map<Integer, StreamNodeForwardGroup> forwardGroupsByEndpointNodeIdCache,
            StreamGraph streamGraph,
            Map<Integer, Integer> frozenNodeToStartNodeMap,
            Map<Integer, Map<StreamEdge, NonChainedOutput>> opIntermediateOutputsCaches) {
        this.forwardGroupsByEndpointNodeIdCache = forwardGroupsByEndpointNodeIdCache;
        this.streamGraph = streamGraph;
        this.frozenNodeToStartNodeMap = frozenNodeToStartNodeMap;
        this.opIntermediateOutputsCaches = opIntermediateOutputsCaches;
    }

    public boolean modifyStreamEdge(List<StreamEdgeUpdateRequestInfo> requestInfos) {
        for (StreamEdgeUpdateRequestInfo requestInfo : requestInfos) {
            if (!modifyStreamEdgeValidate(requestInfo)) {
                return false;
            }
        }

        for (StreamEdgeUpdateRequestInfo requestInfo : requestInfos) {
            Integer sourceNodeId = requestInfo.getSourceId();
            Integer targetNodeId = requestInfo.getTargetId();
            StreamEdge targetEdge =
                    getStreamEdge(sourceNodeId, targetNodeId, requestInfo.getEdgeId());
            StreamPartitioner<?> newPartitioner = requestInfo.getOutputPartitioner();
            if (newPartitioner != null) {
                modifyOutputPartitioner(targetEdge, newPartitioner);
            }
        }

        return true;
    }

    // TODO: implement this function
    public boolean modifyStreamNode(StreamNodeUpdateRequestInfo requestInfo) {
        return true;
    }

    private boolean modifyStreamEdgeValidate(StreamEdgeUpdateRequestInfo requestInfo) {
        StreamEdge targetEdge =
                getStreamEdge(
                        requestInfo.getSourceId(),
                        requestInfo.getTargetId(),
                        requestInfo.getEdgeId());
        if (targetEdge == null) {
            LOG.info("Unable to find target streamEdge {}.", requestInfo.getEdgeId());
            return false;
        }
        Integer sourceNodeId = targetEdge.getSourceId();
        Integer targetNodeId = targetEdge.getTargetId();
        if (frozenNodeToStartNodeMap.containsKey(targetNodeId)) {
            LOG.info(
                    "The downstream vertex has been generated and the edge {} cannot be modified!",
                    requestInfo.getEdgeId());
            return false;
        }
        StreamPartitioner<?> newPartitioner = requestInfo.getOutputPartitioner();
        if (newPartitioner != null) {
            if (targetEdge.getPartitioner().getClass().equals(ForwardPartitioner.class)) {
                return false;
            }
            if (streamGraph.isDynamic()
                    && newPartitioner instanceof ForwardPartitioner
                    && !canMergeForwardGroups(sourceNodeId, targetNodeId)) {
                requestInfo.outputPartitioner(new RescalePartitioner<>());
                LOG.info(
                        "The ForwardPartitioner of StreamEdge with Id {} has been rolled back to RescalePartitioner.",
                        requestInfo.getEdgeId());
            }
        }
        return true;
    }

    private void modifyOutputPartitioner(
            StreamEdge targetEdge, StreamPartitioner<?> newPartitioner) {
        if (newPartitioner == null || targetEdge == null) {
            return;
        }
        Integer sourceNodeId = targetEdge.getSourceId();
        Integer targetNodeId = targetEdge.getTargetId();
        if (newPartitioner instanceof ForwardPartitioner
                && !StreamingJobGraphGenerator.isChainable(targetEdge, streamGraph)) {
            targetEdge.setPartitioner(new RescalePartitioner<>());
        } else {
            targetEdge.setPartitioner(newPartitioner);
        }
        LOG.info(
                "The partitioner of StreamEdge with Id {} expects set to be {} and is finally set to {}.",
                targetEdge.getId(),
                newPartitioner.getClass(),
                targetEdge.getPartitioner().getClass());
        if (streamGraph.isDynamic() && targetEdge.getPartitioner() instanceof ForwardPartitioner) {
            mergeForwardGroups(
                    sourceNodeId, targetNodeId, forwardGroupsByEndpointNodeIdCache, streamGraph);
        }
        Map<StreamEdge, NonChainedOutput> opIntermediateOutputs =
                opIntermediateOutputsCaches.get(sourceNodeId);
        NonChainedOutput output =
                opIntermediateOutputs != null ? opIntermediateOutputs.get(targetEdge) : null;
        if (output != null) {
            output.setPartitioner(targetEdge.getPartitioner());
        }
    }

    private boolean canMergeForwardGroups(Integer sourceNodeId, Integer targetNodeId) {
        StreamNodeForwardGroup sourceForwardGroup =
                forwardGroupsByEndpointNodeIdCache.get(sourceNodeId);
        StreamNodeForwardGroup targetForwardGroup =
                forwardGroupsByEndpointNodeIdCache.get(targetNodeId);
        if (sourceForwardGroup == null || targetForwardGroup == null) {
            return false;
        }
        return (!targetForwardGroup.isParallelismDecided()
                        || (sourceForwardGroup.isParallelismDecided()
                                && targetForwardGroup.getParallelism()
                                        == sourceForwardGroup.getParallelism()))
                && (!targetForwardGroup.isMaxParallelismDecided()
                        || (sourceForwardGroup.isMaxParallelismDecided())
                                && sourceForwardGroup.getMaxParallelism()
                                        >= targetForwardGroup.getMaxParallelism());
    }

    public static void mergeForwardGroups(
            Integer sourceNodeId,
            Integer targetNodeId,
            Map<Integer, StreamNodeForwardGroup> forwardGroupsByEndpointNodeIdCache,
            StreamGraph streamGraph) {
        StreamNodeForwardGroup sourceForwardGroup =
                forwardGroupsByEndpointNodeIdCache.get(sourceNodeId);
        StreamNodeForwardGroup targetForwardGroup =
                forwardGroupsByEndpointNodeIdCache.get(targetNodeId);
        if (sourceForwardGroup == null || targetForwardGroup == null) {
            return;
        }

        sourceForwardGroup.mergeForwardGroup(targetForwardGroup);

        targetForwardGroup
                .getStartNodeIds()
                .forEach(
                        startNodeId ->
                                forwardGroupsByEndpointNodeIdCache.put(
                                        startNodeId, sourceForwardGroup));

        if (sourceForwardGroup.isParallelismDecided()) {
            targetForwardGroup
                    .getChainedNodeIdMap()
                    .forEach(
                            (startNodeId, chainedNodeIds) -> {
                                chainedNodeIds.stream()
                                        .map(streamGraph::getStreamNode)
                                        .forEach(
                                                streamNode -> {
                                                    streamNode.setParallelism(
                                                            sourceForwardGroup.getParallelism(),
                                                            true);
                                                });
                            });
        }

        if (sourceForwardGroup.isMaxParallelismDecided()) {
            targetForwardGroup
                    .getChainedNodeIdMap()
                    .forEach(
                            (startNodeId, chainedNodeIds) -> {
                                chainedNodeIds.stream()
                                        .map(streamGraph::getStreamNode)
                                        .forEach(
                                                streamNode -> {
                                                    streamNode.setMaxParallelism(
                                                            sourceForwardGroup.getMaxParallelism());
                                                });
                            });
        }
    }

    private StreamEdge getStreamEdge(Integer sourceId, Integer targetId, String edgeId) {
        for (StreamEdge edge : streamGraph.getStreamEdges(sourceId, targetId)) {
            if (edge.getId().equals(edgeId)) {
                return edge;
            }
        }
        return null;
    }
}
