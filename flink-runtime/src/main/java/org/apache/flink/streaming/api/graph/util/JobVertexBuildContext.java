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

package org.apache.flink.streaming.api.graph.util;

import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.streaming.api.graph.StreamEdge;
import org.apache.flink.streaming.api.graph.StreamGraph;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class JobVertexBuildContext {

    private final StreamGraph streamGraph;

    /** The {@link OperatorChainInfo}s, key is the start node id of the chain. */
    private final Map<Integer, OperatorChainInfo> chainInfos;

    /** The {@link OperatorInfo}s, key is the id of the stream node. */
    private final Map<Integer, OperatorInfo> operatorInfosInorder;

    // The created JobVertex, the key is start node id
    private final Map<Integer, JobVertex> jobVerticesInorder;

    // the ids of nodes whose output result partition type should be set to BLOCKING
    private final Set<Integer> outputBlockingNodesID;

    // The order of StreamEdge connected to other vertices should be consistent with the order in
    // which JobEdge was created
    private final List<StreamEdge> physicalEdgesInOrder;

    // In progressive generation mode, this boolean only contains information from the current stage
    private boolean hasHybridResultPartition;

    public JobVertexBuildContext(StreamGraph streamGraph) {
        this.streamGraph = streamGraph;
        this.chainInfos = new HashMap<>();
        this.operatorInfosInorder = new LinkedHashMap<>();
        this.jobVerticesInorder = new LinkedHashMap<>();
        this.outputBlockingNodesID = new HashSet<>();
        this.physicalEdgesInOrder = new ArrayList<>();
        this.hasHybridResultPartition = false;
    }

    public void addChainInfo(Integer startNodeId, OperatorChainInfo chainInfo) {
        chainInfos.put(startNodeId, chainInfo);
    }

    public OperatorChainInfo getChainInfo(Integer startNodeId) {
        return chainInfos.get(startNodeId);
    }

    public Map<Integer, OperatorChainInfo> getChainInfos() {
        return chainInfos;
    }

    public OperatorInfo getOperatorInfo(Integer nodeId) {
        return operatorInfosInorder.get(nodeId);
    }

    public OperatorInfo getOrCreateOperatorInfo(Integer nodeId) {
        return operatorInfosInorder.computeIfAbsent(nodeId, key -> new OperatorInfo());
    }

    public Map<Integer, OperatorInfo> getOperatorInfos() {
        return operatorInfosInorder;
    }

    public StreamGraph getStreamGraph() {
        return streamGraph;
    }

    public boolean hasHybridResultPartition() {
        return hasHybridResultPartition;
    }

    public void setHasHybridResultPartition(boolean hasHybridResultPartition) {
        this.hasHybridResultPartition = hasHybridResultPartition;
    }

    public void addPhysicalEdgesInOrder(StreamEdge edge) {
        physicalEdgesInOrder.add(edge);
    }

    public List<StreamEdge> getPhysicalEdgesInOrder() {
        return physicalEdgesInOrder;
    }

    public void addOutputBlockingNode(Integer nodeId) {
        outputBlockingNodesID.add(nodeId);
    }

    public boolean isOutputBlockingNode(Integer nodeId) {
        return outputBlockingNodesID.contains(nodeId);
    }

    public void addJobVertex(Integer startNodeId, JobVertex jobVertex) {
        jobVerticesInorder.put(startNodeId, jobVertex);
    }

    public Map<Integer, JobVertex> getJobVertices() {
        return jobVerticesInorder;
    }

    public JobVertex getJobVertex(Integer startNodeId) {
        return jobVerticesInorder.get(startNodeId);
    }
}
