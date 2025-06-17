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

package org.apache.flink.runtime.jobgraph;

import org.apache.flink.runtime.io.network.partition.ResultPartitionType;
import org.apache.flink.streaming.api.graph.StreamEdge;
import org.apache.flink.streaming.runtime.partitioner.ForwardPartitioner;
import org.apache.flink.streaming.runtime.partitioner.StreamPartitioner;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * An intermediate data set is the data set produced by an operator - either a source or any
 * intermediate operation.
 *
 * <p>Intermediate data sets may be read by other operators, materialized, or discarded.
 */
public class IntermediateDataSet implements java.io.Serializable {

    private static final long serialVersionUID = 1L;

    private final IntermediateDataSetID id; // the identifier

    private final JobVertex producer; // the operation that produced this data set

    // All consumers must have the same partitioner and parallelism
    private final List<JobEdge> consumers = new ArrayList<>();

    private final Map<JobVertexID, JobEdge> edges = new HashMap<>();

    // The type of partition to use at runtime
    private final ResultPartitionType resultType;

    private DistributionPattern distributionPattern;

    private boolean isBroadcast;

    private boolean isForward;

    /** The number of job edges that need to be created. */
    private int numJobEdgesToCreate;

    // --------------------------------------------------------------------------------------------

    public IntermediateDataSet(
            IntermediateDataSetID id, ResultPartitionType resultType, JobVertex producer) {
        this.id = checkNotNull(id);
        this.producer = checkNotNull(producer);
        this.resultType = checkNotNull(resultType);
    }

    // --------------------------------------------------------------------------------------------

    public IntermediateDataSetID getId() {
        return id;
    }

    public JobVertex getProducer() {
        return producer;
    }

    public List<JobEdge> getConsumers() {
        return this.consumers;
    }

    public boolean areAllConsumerVerticesCreated() {
        return numJobEdgesToCreate == consumers.size();
    }

    public boolean isBroadcast() {
        return isBroadcast;
    }

    public boolean isBroadcast(JobVertexID consumerId) {
        return edges.get(consumerId).isBroadcast();
    }

    public boolean isForward() {
        return isForward;
    }

    public boolean isForward(JobVertexID consumerId) {
        return edges.get(consumerId).isForward();
    }

    public DistributionPattern getDistributionPattern() {
        return distributionPattern;
    }

    public DistributionPattern getDistributionPattern(JobVertexID consumerId) {
        return edges.get(consumerId).getDistributionPattern();
    }

    private Set<DistributionPattern> patternsCache = new HashSet<>();

    public Set<DistributionPattern> getConsumingDistributionPatterns() {
        return patternsCache;
    }

    public ResultPartitionType getResultType() {
        return resultType;
    }

    // --------------------------------------------------------------------------------------------

    public void addConsumer(JobEdge edge) {
        // sanity check
        checkState(id.equals(edge.getSourceId()), "Incompatible dataset id.");

        if (consumers.isEmpty() && distributionPattern == null) {
            distributionPattern = edge.getDistributionPattern();
            isBroadcast = edge.isBroadcast();
            isForward = edge.isForward();
            patternsCache.add(distributionPattern);
        }
        checkState(
                patternsCache.contains(edge.getDistributionPattern()),
                this
                        + " has "
                        + patternsCache
                        + " not contains edge "
                        + edge
                        + " with pattern "
                        + edge.getDistributionPattern());
        consumers.add(edge);
        edges.put(edge.getTarget().getID(), edge);
    }

    public void configure(
            DistributionPattern distributionPattern, boolean isBroadcast, boolean isForward) {
        checkState(consumers.isEmpty(), "The output job edges have already been added.");
        if (this.distributionPattern == null) {
            this.distributionPattern = distributionPattern;
            this.isBroadcast = isBroadcast;
            this.isForward = isForward;
            patternsCache.add(distributionPattern);
        } else {
            checkState(
                    this.distributionPattern == distributionPattern,
                    "Incompatible distribution pattern.");
            checkState(this.isBroadcast == isBroadcast, "Incompatible broadcast type.");
            checkState(this.isForward == isForward, "Incompatible forward type.");
        }
    }

    public void updateOutputPattern(StreamEdge streamEdge) {
        // checkState(consumers.isEmpty(), "The output job edges have already been added.");

        updatedStreamEdges.add(streamEdge);
        DistributionPattern pattern =
                streamEdge.getPartitioner().isPointwise()
                        ? DistributionPattern.POINTWISE
                        : DistributionPattern.ALL_TO_ALL;
        patternsCache.add(pattern);

        if (updatedStreamEdges.size() == numJobEdgesToCreate) {

            Set<StreamPartitioner> partitioners = new HashSet<>();
            updatedStreamEdges.stream().map(StreamEdge::getPartitioner).forEach(partitioners::add);

            if (partitioners.size() == 1) {
                StreamPartitioner partitioner = partitioners.iterator().next();
                distributionPattern =
                        partitioner.isPointwise()
                                ? DistributionPattern.POINTWISE
                                : DistributionPattern.ALL_TO_ALL;
                isBroadcast = partitioner.isBroadcast();
                isForward = partitioner.getClass().equals(ForwardPartitioner.class);

                patternsCache.clear();
                patternsCache.add(distributionPattern);
            }
        }
    }

    private final List<StreamEdge> updatedStreamEdges = new ArrayList<>();

    public void increaseNumJobEdgesToCreate() {
        this.numJobEdgesToCreate++;
    }

    // --------------------------------------------------------------------------------------------

    @Override
    public String toString() {
        return "Intermediate Data Set (" + id + ")";
    }

    public Set<JobVertexID> getConsumingBroadcastVertices() {
        Set<JobVertexID> broadcastConsumerJobVertices = new HashSet<>();
        consumers.stream()
                .filter(JobEdge::isBroadcast)
                .map(JobEdge::getTarget)
                .map(JobVertex::getID)
                .forEach(broadcastConsumerJobVertices::add);
        return broadcastConsumerJobVertices;
    }
}
