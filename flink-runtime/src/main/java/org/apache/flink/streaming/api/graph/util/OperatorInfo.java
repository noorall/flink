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

import org.apache.flink.api.common.operators.ResourceSpec;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.api.graph.StreamEdge;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** A public class to help maintain the information of an operator */
public class OperatorInfo {

    private String chainedName;

    private ResourceSpec chainedMinResource;

    private ResourceSpec chainedPreferredResources;

    private StreamConfig vertexConfig;

    /**
     * The config for node chained with the current operator, the key is the ID of the stream node
     */
    private final Map<Integer, StreamConfig> chainedConfigs;
    /**
     * This is used to cache the chainable outputs, to set the chainable outputs config after all
     * job vertices are created.
     */
    private final List<StreamEdge> chainableOutputs;
    /**
     * This is used to cache the non-chainable outputs, to set the non-chainable outputs config
     * after all job vertices are created.
     */
    private final List<StreamEdge> nonChainableOutputs;

    public OperatorInfo() {
        this.chainedConfigs = new HashMap<>();
        this.chainableOutputs = new ArrayList<>();
        this.nonChainableOutputs = new ArrayList<>();
    }

    public String getChainedName() {
        return chainedName;
    }

    public void setChainedName(String chainedName) {
        this.chainedName = chainedName;
    }

    public ResourceSpec getChainedMinResource() {
        return chainedMinResource;
    }

    public void setChainedMinResource(ResourceSpec chainedMinResource) {
        this.chainedMinResource = chainedMinResource;
    }

    public ResourceSpec getChainedPreferredResources() {
        return chainedPreferredResources;
    }

    public void setChainedPreferredResources(ResourceSpec chainedPreferredResources) {
        this.chainedPreferredResources = chainedPreferredResources;
    }

    public List<StreamEdge> getChainableOutputs() {
        return chainableOutputs;
    }

    public void setChainableOutputs(List<StreamEdge> chainableOutputs) {
        this.chainableOutputs.addAll(chainableOutputs);
    }

    public List<StreamEdge> getNonChainableOutputs() {
        return nonChainableOutputs;
    }

    public void setNonChainableOutputs(List<StreamEdge> NonChainableOutEdges) {
        this.nonChainableOutputs.addAll(NonChainableOutEdges);
    }

    public StreamConfig getVertexConfig() {
        return vertexConfig;
    }

    public void setVertexConfig(StreamConfig vertexConfig) {
        this.vertexConfig = vertexConfig;
    }

    public void addChainedConfig(Integer streamNodeId, StreamConfig streamConfig) {
        chainedConfigs.put(streamNodeId, streamConfig);
    }

    public Map<Integer, StreamConfig> getChainedConfigs() {
        return chainedConfigs;
    }
}
