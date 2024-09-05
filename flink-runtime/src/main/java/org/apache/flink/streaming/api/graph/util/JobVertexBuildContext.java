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

import org.apache.flink.annotation.Internal;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.operators.coordination.OperatorCoordinator;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.api.graph.StreamEdge;
import org.apache.flink.streaming.api.graph.StreamGraph;
import org.apache.flink.util.SerializedValue;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Helper class encapsulates all necessary information and configurations required during the
 * construction of job vertices.
 */
@Internal
public class JobVertexBuildContext {

    private final StreamGraph streamGraph;

    /** The {@link OperatorChainInfo}s, key is the start node id of the chain. */
    private final Map<Integer, OperatorChainInfo> chainInfos;

    /** The {@link OperatorInfo}s, key is the id of the stream node. */
    private final Map<Integer, OperatorInfo> operatorInfosInorder;

    // This map's key represents the starting node id of each chain. Note that this includes not
    // only the usual head node of the chain but also the ids of chain sources which are used by
    // multi-input.
    private final Map<Integer, Map<Integer, StreamConfig>> chainedConfigs;

    // The created JobVertex, the key is start node id.
    private final Map<Integer, JobVertex> jobVerticesInorder;

    // Futures for the serialization of operator coordinators.
    private final Map<
                    JobVertexID,
                    List<CompletableFuture<SerializedValue<OperatorCoordinator.Provider>>>>
            coordinatorSerializationFuturesPerJobVertex;

    // The order of StreamEdge connected to other vertices should be consistent with the order in
    // which JobEdge was created.
    private final List<StreamEdge> physicalEdgesInOrder;

    // We use AtomicBoolean to track the existence of HybridResultPartition during the incremental
    // JobGraph generation process introduced by AdaptiveGraphManager. It is essential to globally
    // monitor changes to this variable, thus necessitating the use of a Boolean object instead of a
    // primitive boolean.
    private final AtomicBoolean hasHybridResultPartition;

    public JobVertexBuildContext(StreamGraph streamGraph, AtomicBoolean hasHybridResultPartition) {
        this.streamGraph = streamGraph;
        this.chainInfos = new LinkedHashMap<>();
        this.operatorInfosInorder = new LinkedHashMap<>();
        this.jobVerticesInorder = new LinkedHashMap<>();
        this.physicalEdgesInOrder = new ArrayList<>();
        this.hasHybridResultPartition = hasHybridResultPartition;
        this.coordinatorSerializationFuturesPerJobVertex = new HashMap<>();
        this.chainedConfigs = new HashMap<>();
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

    public OperatorInfo createAndGetOperatorInfo(Integer nodeId) {
        OperatorInfo operatorInfo = new OperatorInfo();
        operatorInfosInorder.put(nodeId, operatorInfo);
        return operatorInfo;
    }

    public Map<Integer, OperatorInfo> getOperatorInfos() {
        return operatorInfosInorder;
    }

    public StreamGraph getStreamGraph() {
        return streamGraph;
    }

    public boolean hasHybridResultPartition() {
        return hasHybridResultPartition.get();
    }

    public void setHasHybridResultPartition(boolean hasHybridResultPartition) {
        this.hasHybridResultPartition.set(hasHybridResultPartition);
    }

    public void addPhysicalEdgesInOrder(StreamEdge edge) {
        physicalEdgesInOrder.add(edge);
    }

    public List<StreamEdge> getPhysicalEdgesInOrder() {
        return physicalEdgesInOrder;
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

    public void putCoordinatorSerializationFutures(
            JobVertexID vertexId,
            List<CompletableFuture<SerializedValue<OperatorCoordinator.Provider>>>
                    serializationFutures) {
        coordinatorSerializationFuturesPerJobVertex.put(vertexId, serializationFutures);
    }

    public Map<JobVertexID, List<CompletableFuture<SerializedValue<OperatorCoordinator.Provider>>>>
            getCoordinatorSerializationFuturesPerJobVertex() {
        return coordinatorSerializationFuturesPerJobVertex;
    }

    public Map<Integer, Map<Integer, StreamConfig>> getChainedConfigs() {
        return chainedConfigs;
    }

    public Map<Integer, StreamConfig> getOrCreateChainedConfig(Integer streamNodeId) {
        return chainedConfigs.computeIfAbsent(streamNodeId, key -> new HashMap<>());
    }
}
