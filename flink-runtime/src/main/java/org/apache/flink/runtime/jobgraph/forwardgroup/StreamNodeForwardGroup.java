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

package org.apache.flink.runtime.jobgraph.forwardgroup;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.streaming.api.graph.StreamGraph;
import org.apache.flink.streaming.api.graph.StreamNode;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/** Stream node level implement for {@link ForwardGroup}. */
public class StreamNodeForwardGroup implements ForwardGroup<Integer> {

    private int parallelism = ExecutionConfig.PARALLELISM_DEFAULT;

    private int maxParallelism = JobVertex.MAX_PARALLELISM_DEFAULT;

    private final Set<Integer> streamNodeIds = new HashSet<>();

    // For a group of chained stream nodes, their parallelism is consistent. In order to make
    // calculation and usage easier, we only use the start node to calculate forward group.
    public StreamNodeForwardGroup(final Set<StreamNode> streamNodes) {
        checkNotNull(streamNodes);

        Set<Integer> configuredParallelisms =
                streamNodes.stream()
                        .filter(
                                streamNode -> {
                                    streamNodeIds.add(streamNode.getId());
                                    return streamNode.getParallelism() > 0;
                                })
                        .map(StreamNode::getParallelism)
                        .collect(Collectors.toSet());

        checkState(configuredParallelisms.size() <= 1);
        if (configuredParallelisms.size() == 1) {
            this.parallelism = configuredParallelisms.iterator().next();
        }

        Set<Integer> configuredMaxParallelisms =
                streamNodes.stream()
                        .map(StreamNode::getMaxParallelism)
                        .filter(val -> val > 0)
                        .collect(Collectors.toSet());

        if (!configuredMaxParallelisms.isEmpty()) {
            this.maxParallelism = Collections.min(configuredMaxParallelisms);
            checkState(
                    parallelism == ExecutionConfig.PARALLELISM_DEFAULT
                            || maxParallelism >= parallelism,
                    "There is a start node in the forward group whose maximum parallelism is smaller than the group's parallelism");
        }
    }

    @Override
    public void setParallelism(int parallelism) {
        checkState(this.parallelism == ExecutionConfig.PARALLELISM_DEFAULT);
        this.parallelism = parallelism;
    }

    @Override
    public boolean isParallelismDecided() {
        return parallelism > 0;
    }

    @Override
    public int getParallelism() {
        checkState(isParallelismDecided());
        return parallelism;
    }

    @Override
    public void setMaxParallelism(int maxParallelism) {
        checkState(
                maxParallelism == ExecutionConfig.PARALLELISM_DEFAULT
                        || maxParallelism >= parallelism,
                "There is a job vertex in the forward group whose maximum parallelism is smaller than the group's parallelism");
        this.maxParallelism = maxParallelism;
    }

    @Override
    public boolean isMaxParallelismDecided() {
        return maxParallelism > 0;
    }

    @Override
    public int getMaxParallelism() {
        checkState(isMaxParallelismDecided());
        return maxParallelism;
    }

    @Override
    public Set<Integer> getVertexIds() {
        return streamNodeIds;
    }

    /**
     * Merges forwardGroupToMerge into this and update the parallelism information for stream nodes
     * in merged forward group.
     *
     * @param forwardGroupToMerge The forward group to be merged.
     * @return whether the merge was successful.
     */
    public boolean mergeForwardGroup(
            StreamNodeForwardGroup forwardGroupToMerge, StreamGraph streamGraph) {
        checkNotNull(forwardGroupToMerge);

        if (forwardGroupToMerge == this) {
            return true;
        }

        if (!ForwardGroupComputeUtil.canTargetMergeIntoSourceForwardGroup(
                this, forwardGroupToMerge)) {
            return false;
        }

        if (this.isParallelismDecided() && !forwardGroupToMerge.isParallelismDecided()) {
            forwardGroupToMerge.parallelism = this.parallelism;
            forwardGroupToMerge.updateNodeParallelism(streamGraph);
        } else if (!this.isParallelismDecided() && forwardGroupToMerge.isParallelismDecided()) {
            this.parallelism = forwardGroupToMerge.parallelism;
            this.updateNodeParallelism(streamGraph);
        } else {
            checkState(this.parallelism == forwardGroupToMerge.parallelism);
        }

        if (forwardGroupToMerge.isMaxParallelismDecided()
                && (!this.isMaxParallelismDecided()
                        || this.maxParallelism > forwardGroupToMerge.maxParallelism)) {
            this.setMaxParallelism(forwardGroupToMerge.maxParallelism);
            this.updateNodeMaxParallelism(streamGraph);
        } else if (this.isMaxParallelismDecided()
                && (!forwardGroupToMerge.isMaxParallelismDecided()
                        || forwardGroupToMerge.maxParallelism > this.maxParallelism)) {
            forwardGroupToMerge.setMaxParallelism(this.maxParallelism);
            forwardGroupToMerge.updateNodeMaxParallelism(streamGraph);
        } else {
            checkState(this.maxParallelism == forwardGroupToMerge.maxParallelism);
        }

        this.streamNodeIds.addAll(forwardGroupToMerge.streamNodeIds);

        return true;
    }

    private void updateNodeParallelism(StreamGraph streamGraph) {
        streamNodeIds.forEach(
                streamNodeId -> {
                    streamGraph.getStreamNode(streamNodeId).setParallelism(parallelism);
                });
    }

    private void updateNodeMaxParallelism(StreamGraph streamGraph) {
        streamNodeIds.forEach(
                streamNodeId -> {
                    streamGraph.getStreamNode(streamNodeId).setMaxParallelism(maxParallelism);
                });
    }

    @VisibleForTesting
    public int size() {
        return streamNodeIds.size();
    }
}
