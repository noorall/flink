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
import org.apache.flink.streaming.api.graph.StreamEdge;
import org.apache.flink.streaming.api.graph.StreamGraph;
import org.apache.flink.streaming.api.graph.StreamNode;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.flink.util.Preconditions.checkNotNull;

@Internal
public class StreamExecGraph {

    private final List<StreamExecNode> roots = new ArrayList<>();
    private final Set<StreamExecNode> frozenNodes = new HashSet<>();
    private final Map<StreamNode, StreamExecNode> nodeMapping = new HashMap<>();
    private final Map<StreamExecEdge, StreamEdge> edgeMapping = new HashMap<>();
    private final Map<StreamEdge, List<StreamExecEdge>> execEdgeMapping = new HashMap<>();

    public StreamExecGraph(StreamGraph streamGraph, Set<StreamNode> frozenNodes) {
        List<StreamNode> topologicallyFromSources =
                streamGraph.getStreamNodesSortedTopologicallyFromSources();

        for (StreamNode streamNode : topologicallyFromSources) {
            List<StreamExecEdge> execEdges = new ArrayList<>();
            for (StreamEdge inEdge : streamNode.getInEdges()) {
                StreamExecEdge execEdge =
                        new StreamExecEdge(
                                nodeMapping.get(streamGraph.getStreamNode(inEdge.getSourceId())),
                                inEdge.getTypeNumber());
                execEdges.add(execEdge);

                edgeMapping.put(execEdge, inEdge);
                execEdgeMapping.computeIfAbsent(inEdge, k -> new ArrayList<>()).add(execEdge);
            }

            StreamExecNode streamExecNode =
                    frozenNodes.contains(streamNode)
                            ? new StreamExecNode(streamNode)
                            : new StreamExecNode(
                                    streamNode,
                                    execEdges,
                                    streamNode.getInputPropertiesByTypeNumber());
            nodeMapping.put(streamNode, streamExecNode);

            if (streamGraph.getSinkIDs().contains(streamNode.getId())) {
                roots.add(streamExecNode);
            }

            if (frozenNodes.contains(streamNode)) {
                this.frozenNodes.add(streamExecNode);
            }
        }
    }

    public List<StreamExecNode> getRoots() {
        return roots;
    }

    public Set<StreamExecNode> getFrozenNodes() {
        return frozenNodes;
    }

    public void brokenEdge(StreamExecNode node, int indexInTarget) {
        StreamExecEdge oldEdge = node.getInputEdges().get(indexInTarget);
        StreamExecEdge newEdge = node.broken(indexInTarget);
        StreamEdge streamEdge = getStreamEdge(oldEdge);
        edgeMapping.put(newEdge, streamEdge);
        execEdgeMapping.computeIfAbsent(streamEdge, k -> new ArrayList<>()).add(newEdge);
    }

    public StreamEdge getStreamEdge(StreamExecEdge edge) {
        return checkNotNull(edgeMapping.get(edge));
    }

    public void replaceEdge(StreamEdge oldEdge, StreamEdge newEdge) {
        List<StreamExecEdge> execEdges = execEdgeMapping.get(oldEdge);
        execEdgeMapping.put(newEdge, execEdges);
        execEdgeMapping.remove(oldEdge);

        for (StreamExecEdge execEdge : execEdges) {
            edgeMapping.put(execEdge, newEdge);
        }
    }
}
