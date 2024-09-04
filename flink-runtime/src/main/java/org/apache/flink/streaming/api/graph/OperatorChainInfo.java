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

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.jobgraph.InputOutputFormatContainer;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.operators.coordination.OperatorCoordinator;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * A private class to help maintain the information of an operator chain during the recursive call
 * in {@link StreamingJobGraphGenerator# createChain(Integer, int, OperatorChainInfo, Map)}.
 */
public class OperatorChainInfo {
    private final Integer startNodeId;
    private final Map<Integer, List<Tuple2<byte[], byte[]>>> chainedOperatorHashes;
    private final Map<Integer, ChainedSourceInfo> chainedSources;
    private final List<OperatorCoordinator.Provider> coordinatorProviders;
    private final StreamGraph streamGraph;
    private final List<StreamNode> chainedNodes;
    private final List<StreamEdge> transitiveOutEdges;
    private final List<StreamEdge> transitiveInEdges;
    private final Map<Integer, StreamConfig> chainedConfigs;

    private InputOutputFormatContainer inputOutputFormatContainer = null;

    public OperatorChainInfo(
            int startNodeId,
            Map<Integer, ChainedSourceInfo> chainedSources,
            StreamGraph streamGraph) {
        this.startNodeId = startNodeId;
        this.chainedSources = chainedSources;
        this.streamGraph = streamGraph;
        chainedOperatorHashes = new HashMap<>();
        coordinatorProviders = new ArrayList<>();
        chainedNodes = new ArrayList<>();
        transitiveOutEdges = new ArrayList<>();
        transitiveInEdges = new ArrayList<>();
        chainedConfigs = new HashMap<>();
    }

    public OperatorChainInfo(int startNodeId, StreamGraph streamGraph) {
        this(startNodeId, new HashMap<>(), streamGraph);
    }

    public Integer getStartNodeId() {
        return startNodeId;
    }

    public List<Tuple2<byte[], byte[]>> getChainedOperatorHashes(int startNodeId) {
        return chainedOperatorHashes.get(startNodeId);
    }

    void addCoordinatorProvider(OperatorCoordinator.Provider coordinator) {
        coordinatorProviders.add(coordinator);
    }

    public List<OperatorCoordinator.Provider> getCoordinatorProviders() {
        return coordinatorProviders;
    }

    Map<Integer, ChainedSourceInfo> getChainedSources() {
        return chainedSources;
    }

    public OperatorID addNodeToChain(
            int currentNodeId,
            String operatorName,
            Map<Integer, byte[]> hashes,
            Map<Integer, byte[]> legacyHashes) {
        recordChainedNode(currentNodeId);
        return recordCoordinatorProvider(currentNodeId, operatorName, hashes, legacyHashes);
    }

    public OperatorID recordCoordinatorProvider(
            int currentNodeId,
            String operatorName,
            Map<Integer, byte[]> hashes,
            Map<Integer, byte[]> legacyHashes) {
        StreamNode streamNode = streamGraph.getStreamNode(currentNodeId);

        List<Tuple2<byte[], byte[]>> operatorHashes =
                chainedOperatorHashes.computeIfAbsent(startNodeId, k -> new ArrayList<>());

        byte[] primaryHashBytes = hashes.get(currentNodeId);

        operatorHashes.add(new Tuple2<>(primaryHashBytes, legacyHashes.get(currentNodeId)));

        OperatorID operatorId = new OperatorID(hashes.get(currentNodeId));
        streamNode.setOperatorId(operatorId);

        streamNode.getCoordinatorProvider(operatorName, operatorId).map(coordinatorProviders::add);

        return new OperatorID(primaryHashBytes);
    }

    public void setTransitiveOutEdges(final List<StreamEdge> transitiveOutEdges) {
        this.transitiveOutEdges.addAll(transitiveOutEdges);
    }

    public List<StreamEdge> getTransitiveOutEdges() {
        return transitiveOutEdges;
    }

    public void recordChainedNode(int currentNodeId) {
        StreamNode streamNode = streamGraph.getStreamNode(currentNodeId);
        chainedNodes.add(streamNode);
    }

    public OperatorChainInfo newChain(Integer startNodeId) {
        return new OperatorChainInfo(startNodeId, chainedSources, streamGraph);
    }

    public List<StreamNode> getAllChainedNodes() {
        return chainedNodes;
    }

    public void addChainedSource(Integer sourceNodeId, ChainedSourceInfo chainedSourceInfo) {
        this.chainedSources.put(sourceNodeId, chainedSourceInfo);
    }

    public List<StreamEdge> getTransitiveInEdges() {
        return this.transitiveInEdges;
    }

    public void addTransitiveInEdge(StreamEdge transitiveInEdge) {
        transitiveInEdges.add(transitiveInEdge);
    }

    public boolean hasFormatContainer() {
        return inputOutputFormatContainer != null;
    }

    public InputOutputFormatContainer getOrCreateFormatContainer() {
        if (inputOutputFormatContainer == null) {
            inputOutputFormatContainer =
                    new InputOutputFormatContainer(Thread.currentThread().getContextClassLoader());
        }
        return inputOutputFormatContainer;
    }

    public void addChainedConfig(Integer streamNodeId, StreamConfig config) {
        chainedConfigs.put(streamNodeId, config);
    }

    public Map<Integer, StreamConfig> getChainedConfigs() {
        return chainedConfigs;
    }

    public StreamConfig getChainedConfig(Integer streamNodeId) {
        return chainedConfigs.get(streamNodeId);
    }
}
