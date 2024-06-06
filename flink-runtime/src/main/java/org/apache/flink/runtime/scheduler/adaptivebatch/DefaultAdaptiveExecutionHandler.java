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

package org.apache.flink.runtime.scheduler.adaptivebatch;

import org.apache.flink.configuration.BatchExecutionOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.executiongraph.ExecutionJobVertex;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.jobgraph.forwardgroup.StreamNodeForwardGroup;
import org.apache.flink.runtime.jobmaster.event.ExecutionJobVertexFinishedEvent;
import org.apache.flink.runtime.jobmaster.event.JobEvent;
import org.apache.flink.streaming.api.graph.AdaptiveJobGraphManager;
import org.apache.flink.streaming.api.graph.StreamEdge;
import org.apache.flink.streaming.api.graph.StreamEdgeUpdateRequestInfo;
import org.apache.flink.streaming.api.graph.StreamGraph;
import org.apache.flink.streaming.api.graph.StreamGraphManagerContext;
import org.apache.flink.streaming.api.graph.StreamNode;
import org.apache.flink.streaming.api.operators.AdaptiveJoin;
import org.apache.flink.streaming.runtime.partitioner.BroadcastPartitioner;
import org.apache.flink.streaming.runtime.partitioner.ForwardPartitioner;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.apache.flink.util.Preconditions.checkNotNull;

/** Default implementation of {@link AdaptiveExecutionHandler}. */
public class DefaultAdaptiveExecutionHandler implements AdaptiveExecutionHandler {

    private final Logger log = LoggerFactory.getLogger(DefaultAdaptiveExecutionHandler.class);

    private final Configuration configuration;

    private final Map<JobVertexID, ExecutionJobVertexFinishedEvent> jobVertexFinishedEvents =
            new HashMap<>();

    private final List<JobGraphUpdateListener> jobGraphUpdateListeners = new ArrayList<>();

    private final AdaptiveJobGraphManager jobGraphManager;

    private final Function<Integer, OperatorID> findOperatorIdByStreamNodeId;

    private final Set<Integer> updatedStreamNodeIds = new HashSet<>();

    public DefaultAdaptiveExecutionHandler(
            ClassLoader userClassloader,
            StreamGraph streamGraph,
            Executor serializationExecutor,
            Configuration configuration,
            Function<Integer, OperatorID> findOperatorIdByStreamNodeId) {
        this.jobGraphManager =
                new AdaptiveJobGraphManager(
                        userClassloader,
                        streamGraph,
                        serializationExecutor,
                        AdaptiveJobGraphManager.GenerateMode.LAZILY);
        this.findOperatorIdByStreamNodeId = checkNotNull(findOperatorIdByStreamNodeId);
        this.configuration = checkNotNull(configuration);
    }

    @Override
    public JobGraph getJobGraph() {
        log.info("Try get job graph.");
        return jobGraphManager.getJobGraph();
    }

    @Override
    public void handleJobEvent(JobEvent jobEvent) {
        try {
            tryAdjustStreamGraph(jobEvent);
        } catch (Exception e) {
            log.error("Failed to handle job event {}.", jobEvent, e);
            throw new RuntimeException(e);
        }
    }

    private void tryAdjustStreamGraph(JobEvent jobEvent) throws Exception {
        if (jobEvent instanceof ExecutionJobVertexFinishedEvent) {
            ExecutionJobVertexFinishedEvent event = (ExecutionJobVertexFinishedEvent) jobEvent;
            jobVertexFinishedEvents.put(event.getVertexId(), event);

            if (enableAdaptiveJoinType()) {
                tryAdjustJoinType(event);
            }
            tryUpdateJobGraph(event.getVertexId());
        }
    }

    private void tryUpdateJobGraph(JobVertexID jobVertexId) throws Exception {
        List<JobVertex> newlyCreatedJobVertices =
                jobGraphManager.onJobVertexFinishedAndUpdateGraph(jobVertexId);

        if (!newlyCreatedJobVertices.isEmpty()) {
            notifyJobGraphUpdated(newlyCreatedJobVertices);
        }
    }

    private boolean enableAdaptiveJoinType() {
        return configuration.get(BatchExecutionOptions.ADAPTIVE_JOIN_TYPE_ENABLED);
    }

    private void tryAdjustJoinType(ExecutionJobVertexFinishedEvent event) {
        JobVertexID jobVertexId = event.getVertexId();

        List<StreamEdge> outputEdges = jobGraphManager.findOutputEdgesByVertexId(jobVertexId);

        for (StreamEdge edge : outputEdges) {
            tryTransferToBroadCastJoin(edge);
        }
    }

    private void tryTransferToBroadCastJoin(StreamEdge edge) {
        StreamNode node = edge.getTargetNode();
        if (jobGraphManager.findVertexByStreamNodeId(node.getId()).isPresent()
                || updatedStreamNodeIds.contains(node.getId())) {
            return;
        }

        if (node.getOperatorFactory() instanceof AdaptiveJoin) {
            log.info("Try optimize adaptive join {} to broadcast join.", node);

            AdaptiveJoin adaptiveJoin = (AdaptiveJoin) node.getOperatorFactory();
            List<AdaptiveJoin.PotentialBroadcastSide> potentialBroadcastJoinSides =
                    adaptiveJoin.getPotentialBroadcastJoinSides();

            if (potentialBroadcastJoinSides.isEmpty()) {
                return;
            }

            List<StreamEdge> sameTypeEdges =
                    node.getInEdges().stream()
                            .filter(inEdge -> inEdge.getTypeNumber() == edge.getTypeNumber())
                            .collect(Collectors.toList());

            long producedBytes = 0L;
            for (StreamEdge inEdge : sameTypeEdges) {
                JobVertexID jobVertex =
                        jobGraphManager.findVertexByStreamNodeId(inEdge.getSourceId()).get();
                if (jobVertexFinishedEvents.containsKey(jobVertex)) {
                    for (BlockingResultInfo info :
                            jobVertexFinishedEvents.get(jobVertex).getResultInfo()) {
                        producedBytes += info.getNumBytesProduced();
                    }
                } else {
                    return;
                }
            }

            if (canBeBroadcast(producedBytes, edge.getTypeNumber(), potentialBroadcastJoinSides)) {
                List<StreamEdge> otherEdge =
                        node.getInEdges().stream()
                                .filter(e -> e.getTypeNumber() != edge.getTypeNumber())
                                .collect(Collectors.toList());

                if (jobGraphManager.updateStreamGraph(
                        context -> updateToBroadcastJoin(sameTypeEdges, otherEdge, context))) {
                    log.info("Update hash join to broadcast join successful!");

                    adaptiveJoin.markAsBroadcastJoin(getBroadCastSide(edge.getTypeNumber()));
                    updatedStreamNodeIds.add(node.getId());
                } else {
                    log.info("Failed to update hash join to broadcast join.");
                }
            }
        }
    }

    private boolean updateToBroadcastJoin(
            List<StreamEdge> toBroadcastEdges,
            List<StreamEdge> toForwardEdges,
            StreamGraphManagerContext context) {
        List<StreamEdgeUpdateRequestInfo> toBroadcastInfo =
                toBroadcastEdges.stream()
                        .map(
                                edge -> {
                                    StreamEdgeUpdateRequestInfo info =
                                            new StreamEdgeUpdateRequestInfo(
                                                    edge.getId(),
                                                    edge.getSourceId(),
                                                    edge.getTargetId());

                                    info.outputPartitioner(new BroadcastPartitioner<>());
                                    return info;
                                })
                        .collect(Collectors.toList());

        List<StreamEdgeUpdateRequestInfo> toForwardInfo =
                toForwardEdges.stream()
                        .map(
                                edge -> {
                                    StreamEdgeUpdateRequestInfo info =
                                            new StreamEdgeUpdateRequestInfo(
                                                    edge.getId(),
                                                    edge.getSourceId(),
                                                    edge.getTargetId());

                                    info.outputPartitioner(new ForwardPartitioner<>());
                                    return info;
                                })
                        .collect(Collectors.toList());

        return context.modifyStreamEdge(toBroadcastInfo) && context.modifyStreamEdge(toForwardInfo);
    }

    private boolean canBeBroadcast(
            long producedBytes,
            int typeNumber,
            List<AdaptiveJoin.PotentialBroadcastSide> potentialBroadcastSides) {
        boolean isSmallEnough = isProducedBytesBelowThreshold(producedBytes);
        boolean isBroadcastCandidate =
                isEdgeTypeAndSideCompatible(typeNumber, potentialBroadcastSides);
        return isSmallEnough && isBroadcastCandidate;
    }

    private boolean isProducedBytesBelowThreshold(long producedBytes) {
        return configuration.get(BatchExecutionOptions.ADAPTIVE_BROADCAST_JOIN_THRESHOLD).getBytes()
                >= producedBytes;
    }

    private boolean isEdgeTypeAndSideCompatible(
            int typeNumber, List<AdaptiveJoin.PotentialBroadcastSide> potentialBroadcastSides) {
        return potentialBroadcastSides.contains(getBroadCastSide(typeNumber));
    }

    private AdaptiveJoin.PotentialBroadcastSide getBroadCastSide(int edgeTypeNumber) {
        if (edgeTypeNumber == 1) {
            return AdaptiveJoin.PotentialBroadcastSide.LEFT;
        } else if (edgeTypeNumber == 2) {
            return AdaptiveJoin.PotentialBroadcastSide.RIGHT;
        } else {
            throw new IllegalArgumentException();
        }
    }

    @Override
    public boolean isStreamGraphConversionFinished() {
        return jobGraphManager.isStreamGraphConversionFinished();
    }

    private void notifyJobGraphUpdated(List<JobVertex> jobVertices) throws Exception {
        for (JobGraphUpdateListener listener : jobGraphUpdateListeners) {
            listener.onNewJobVerticesAdded(jobVertices);
        }
    }

    @Override
    public void registerJobGraphUpdateListener(JobGraphUpdateListener listener) {
        jobGraphUpdateListeners.add(listener);
    }

    @Override
    public void initializeJobGraph() throws Exception {
        List<JobVertex> list = jobGraphManager.initializeJobGraph();
        if (!list.isEmpty()) {
            notifyJobGraphUpdated(list);
        }
    }

    @Override
    public OperatorID findOperatorIdByStreamNodeId(int streamNodeId) {
        return findOperatorIdByStreamNodeId.apply(streamNodeId);
    }

    @Override
    public int getInitialParallelismByForwardGroup(ExecutionJobVertex jobVertex) {
        int vertexInitialParallelism = jobVertex.getParallelism();
        StreamNodeForwardGroup forwardGroup =
                jobGraphManager.findForwardGroupByVertexId(jobVertex.getJobVertexId());
        if (!jobVertex.isParallelismDecided()
                && forwardGroup != null
                && forwardGroup.isParallelismDecided()) {
            vertexInitialParallelism = forwardGroup.getParallelism();
            log.info(
                    "Parallelism of JobVertex: {} ({}) is decided to be {} according to forward group's parallelism.",
                    jobVertex.getName(),
                    jobVertex.getJobVertexId(),
                    vertexInitialParallelism);
        }

        return vertexInitialParallelism;
    }

    @Override
    public void updateForwardGroupByNewlyParallelism(
            ExecutionJobVertex jobVertex, int parallelism) {
        StreamNodeForwardGroup forwardGroup =
                jobGraphManager.findForwardGroupByVertexId(jobVertex.getJobVertexId());
        if (forwardGroup != null && !forwardGroup.isParallelismDecided()) {
            forwardGroup.setParallelism(parallelism);
        }
    }
}
