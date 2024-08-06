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
import org.apache.flink.runtime.jobgraph.IntermediateDataSet;
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
import org.apache.flink.streaming.api.operators.SkewedJoin;
import org.apache.flink.streaming.runtime.partitioner.BroadcastPartitioner;
import org.apache.flink.streaming.runtime.partitioner.ForwardForConsecutiveHashPartitioner;
import org.apache.flink.streaming.runtime.partitioner.ForwardPartitioner;
import org.apache.flink.streaming.runtime.partitioner.StreamPartitioner;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
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
        this.findOperatorIdByStreamNodeId = checkNotNull(findOperatorIdByStreamNodeId);
        this.configuration = checkNotNull(configuration);
        this.jobGraphManager =
                new AdaptiveJobGraphManager(
                        userClassloader,
                        streamGraph,
                        serializationExecutor,
                        AdaptiveJobGraphManager.GenerateMode.LAZILY,
                        getSplitFactor());
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

            if (enableSkewedJoin()) {
                tryAdjustSkewJoin(event);
            }

            tryUpdateJobGraph(event.getVertexId());
        }
    }

    private void tryUpdateJobGraph(JobVertexID jobVertexId) throws Exception {
        List<JobVertex> newlyCreatedJobVertices = jobGraphManager.onJobVertexFinished(jobVertexId);

        if (!newlyCreatedJobVertices.isEmpty()) {
            notifyJobGraphUpdated(newlyCreatedJobVertices);
        }
    }

    private boolean enableAdaptiveJoinType() {
        return configuration.get(BatchExecutionOptions.ADAPTIVE_JOIN_TYPE_ENABLED);
    }

    private boolean enableSkewedJoin() {
        return configuration.get(BatchExecutionOptions.SKEWED_JOIN_ENABLE);
    }

    private long getSkewedPartitionThreshold() {
        return configuration
                .get(BatchExecutionOptions.SKEWED_PARTITION_THRESHOLD_IN_BYTES)
                .getBytes();
    }

    private double getSkewedPartitionFactor() {
        return configuration.get(BatchExecutionOptions.SKEWED_PARTITION_FACTOR);
    }

    private boolean enableSkewedJoinForce() {
        return configuration.get(BatchExecutionOptions.SKEWED_JOIN_FORCE_OPTIMIZE);
    }

    private int getSplitFactor() {
        return configuration.get(BatchExecutionOptions.ADAPTIVE_SPLIT_FACTOR);
    }

    private void tryAdjustSkewJoin(ExecutionJobVertexFinishedEvent event) {
        JobVertexID jobVertexId = event.getVertexId();

        List<StreamEdge> outputEdges = jobGraphManager.findOutputEdgesByVertexId(jobVertexId);

        for (StreamEdge edge : outputEdges) {
            tryOptimizeSkewedJoin(edge);
        }
    }

    private void tryOptimizeSkewedJoin(StreamEdge edge) {
        StreamNode node = edge.getTargetNode();
        if (jobGraphManager.findVertexByStreamNodeId(node.getId()).isPresent()) {
            return;
        }
        if (edge.getPartitioner().isBroadcast() || edge.getPartitioner().isPointwise()) {
            return;
        }
        if (node.getOperatorFactory() instanceof SkewedJoin) {
            log.info("Try optimize skewed join {}.", node);
            SkewedJoin skewedJoin = (SkewedJoin) node.getOperatorFactory();
            List<AdaptiveJoin.JoinSide> splittableJoinSide = skewedJoin.getSplittableJoinSide();

            log.info("The splittable join sides are {}.", splittableJoinSide);

            if (splittableJoinSide.isEmpty()) {
                return;
            }

            List<StreamEdge> sameTypeEdges =
                    node.getInEdges().stream()
                            .filter(inEdge -> inEdge.getTypeNumber() == edge.getTypeNumber())
                            .collect(Collectors.toList());

            if (sameTypeEdges.size() != 1) {
                log.info("The input sides size is {} that not equals {}.", sameTypeEdges.size(), 1);
                return;
            }
            JobVertexID jobVertexID =
                    jobGraphManager.findVertexByStreamNodeId(edge.getSourceId()).get();

            if (!jobVertexFinishedEvents.containsKey(jobVertexID)) {
                log.info("The source vertex {} is not finish yet.", jobVertexID);
                return;
            }
            JobVertex jobVertex = getJobGraph().findVertexByID(jobVertexID);
            jobVertex.getProducedDataSets();
            for (BlockingResultInfo info :
                    jobVertexFinishedEvents.get(jobVertexID).getResultInfo()) {
                Optional<IntermediateDataSet> dataSet =
                        jobVertex.getProducedDataSetById(info.getResultId());
                if (!dataSet.isPresent()) {
                    log.info("ResultInfo with id {} does not exist in vertex.", info.getResultId());
                    continue;
                }
                if (!dataSet.get().getConsumerStreamEdges().contains(edge)) {
                    log.info("The edge does not consuming the dataset with id {}.", dataSet);
                    continue;
                }
                if (edge.getPartitioner().isBroadcast()) {
                    log.info(
                            "ResultInfo with id {} connect to a Broadcast edge, skip it.",
                            info.getResultId());
                    continue;
                }
                if (!DefaultVertexParallelismAndInputInfosDeciderV2.hasSkewPartitions(
                        info, getSkewedPartitionThreshold(), getSkewedPartitionFactor())) {
                    log.info("No skewed partition found, skipped.");
                    continue;
                }
                if (jobGraphManager.updateStreamGraph(
                                context -> tryTransferToHashPartitioner(node, context))
                        && jobGraphManager.updateStreamGraph(
                                context -> tryModifyIntraInputCorrection(edge, context, false))) {
                    log.info("ResultInfo with id {} is skewed.", info.getResultId());
                    skewedJoin.markAsSkewed();
                } else {
                    log.info(
                            "ResultInfo with id {} is skewed, but an error occurred while trying to modify the edge.",
                            info.getResultId());
                }
            }
        }
    }

    private boolean tryTransferToHashPartitioner(
            StreamNode node, StreamGraphManagerContext context) {
        List<StreamEdgeUpdateRequestInfo> updateRequestInfos = new ArrayList<>();
        for (StreamEdge edge : node.getOutEdges()) {
            StreamPartitioner<?> partitioner = edge.getPartitioner();
            if (partitioner instanceof ForwardForConsecutiveHashPartitioner) {
                StreamPartitioner<?> newPartitioner =
                        ((ForwardForConsecutiveHashPartitioner<?>) partitioner)
                                .getHashPartitioner();
                StreamEdgeUpdateRequestInfo streamEdgeUpdateRequestInfo =
                        new StreamEdgeUpdateRequestInfo(
                                        edge.getId(), edge.getSourceId(), edge.getTargetId())
                                .outputPartitioner(newPartitioner);
                updateRequestInfos.add(streamEdgeUpdateRequestInfo);
            }
        }

        if (updateRequestInfos.isEmpty()) {
            return true;
        }

        if (!enableSkewedJoinForce()) {
            log.info(
                    "Found the downstream of ForwardForConsecutiveHashPartitioner, but did not enable forced skewed optimization, skip it.");
            return false;
        }

        return context.modifyStreamEdge(updateRequestInfos);
    }

    private boolean tryModifyIntraInputCorrection(
            StreamEdge edge,
            StreamGraphManagerContext context,
            boolean existIntraInputCorrelation) {
        StreamEdgeUpdateRequestInfo streamEdgeUpdateRequestInfo =
                new StreamEdgeUpdateRequestInfo(
                                edge.getId(), edge.getSourceId(), edge.getTargetId())
                        .existIntraInputCorrelation(existIntraInputCorrelation);
        return context.modifyStreamEdge(Collections.singletonList(streamEdgeUpdateRequestInfo));
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
            List<AdaptiveJoin.JoinSide> potentialBroadcastJoinSides =
                    adaptiveJoin.getPotentialBroadcastJoinSides();
            log.info("The potential broadcast join sides are {}.", potentialBroadcastJoinSides);

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
            log.info(
                    "The edge {} (side {}) produced {} bytes",
                    edge,
                    edge.getTypeNumber(),
                    producedBytes);
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
            List<AdaptiveJoin.JoinSide> potentialBroadcastSides) {
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
            int typeNumber, List<AdaptiveJoin.JoinSide> potentialBroadcastSides) {
        return potentialBroadcastSides.contains(getBroadCastSide(typeNumber));
    }

    private AdaptiveJoin.JoinSide getBroadCastSide(int edgeTypeNumber) {
        if (edgeTypeNumber == 1) {
            return AdaptiveJoin.JoinSide.LEFT;
        } else if (edgeTypeNumber == 2) {
            return AdaptiveJoin.JoinSide.RIGHT;
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
