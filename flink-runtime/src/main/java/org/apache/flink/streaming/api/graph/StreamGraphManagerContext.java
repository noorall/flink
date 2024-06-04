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
import org.apache.flink.streaming.runtime.partitioner.StreamPartitioner;

import java.util.Map;

public class StreamGraphManagerContext {
    private final StreamGraph streamGraph;
    private final Map<Integer, StreamNodeForwardGroup> forwardGroupsByStartNodeIdCache;
    private final Map<Integer, Integer> frozenNodeToStartNodeMap;
    private final Map<Integer, Map<StreamEdge, NonChainedOutput>> opIntermediateOutputsCaches;

    public StreamGraphManagerContext(
            Map<Integer, StreamNodeForwardGroup> forwardGroupsByStartNodeIdCache,
            StreamGraph streamGraph,
            Map<Integer, Integer> frozenNodeToStartNodeMap,
            Map<Integer, Map<StreamEdge, NonChainedOutput>> opIntermediateOutputsCaches) {
        this.forwardGroupsByStartNodeIdCache = forwardGroupsByStartNodeIdCache;
        this.streamGraph = streamGraph;
        this.frozenNodeToStartNodeMap = frozenNodeToStartNodeMap;
        this.opIntermediateOutputsCaches = opIntermediateOutputsCaches;
    }

    public boolean modifyStreamEdge(StreamEdgeUpdateRequestInfo requestInfo) {
        StreamEdge targetEdge =
                getStreamEdge(
                        requestInfo.getSourceId(),
                        requestInfo.getTargetId(),
                        requestInfo.getEdgeId());
        if (targetEdge == null) {
            return false;
        }
        Integer sourceNodeId = targetEdge.getSourceId();
        Integer targetNodeId = targetEdge.getTargetId();
        if (frozenNodeToStartNodeMap.containsKey(targetNodeId)) {
            return false;
        }
        StreamPartitioner<?> newPartitioner = requestInfo.getOutputPartitioner();
        if (requestInfo.getOutputPartitioner() != null) {
            if (targetEdge.getPartitioner() instanceof ForwardPartitioner) {
                return false;
            }
            if (streamGraph.isDynamic()
                    && newPartitioner instanceof ForwardPartitioner
                    && !mergeForwardGroups(sourceNodeId, targetNodeId)) {
                return false;
            }
            targetEdge.setPartitioner(newPartitioner);
            Integer startNodeId = frozenNodeToStartNodeMap.get(sourceNodeId);
            NonChainedOutput output =
                    (startNodeId != null)
                            ? opIntermediateOutputsCaches.get(startNodeId).get(targetEdge)
                            : null;
            if (output != null) {
                output.setPartitioner(newPartitioner);
            }
        }
        return true;
    }

    // TODO: implement this function
    public boolean modifyStreamNode(StreamNodeUpdateRequestInfo requestInfo) {
        return true;
    }

    private boolean mergeForwardGroups(Integer sourceNodeId, Integer targetNodeId) {
        StreamNodeForwardGroup sourceForwardGroup =
                forwardGroupsByStartNodeIdCache.get(sourceNodeId);
        StreamNodeForwardGroup targetForwardGroup =
                forwardGroupsByStartNodeIdCache.get(targetNodeId);
        if (sourceForwardGroup == null || targetForwardGroup == null) {
            return false;
        }
        if ((!targetForwardGroup.isParallelismDecided()
                        || (sourceForwardGroup.isParallelismDecided()
                                && targetForwardGroup.getParallelism()
                                        == sourceForwardGroup.getParallelism()))
                && (!targetForwardGroup.isMaxParallelismDecided()
                        || (sourceForwardGroup.isMaxParallelismDecided())
                                && sourceForwardGroup.getMaxParallelism()
                                        >= targetForwardGroup.getMaxParallelism())) {
            sourceForwardGroup.mergeForwardGroup(targetForwardGroup);

            targetForwardGroup
                    .getStartNodeIds()
                    .forEach(
                            startNodeId ->
                                    forwardGroupsByStartNodeIdCache.put(
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
                                                                sourceForwardGroup
                                                                        .getMaxParallelism());
                                                    });
                                });
            }
            return true;
        }
        return false;
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
