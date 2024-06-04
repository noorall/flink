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
import org.apache.flink.streaming.api.graph.StreamGraph;
import org.apache.flink.streaming.api.graph.StreamNode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Executor;
import java.util.function.Function;

import static org.apache.flink.util.Preconditions.checkNotNull;

/** Default implementation of {@link AdaptiveExecutionHandler}. */
public class DefaultAdaptiveExecutionHandler implements AdaptiveExecutionHandler {

    private final Logger log = LoggerFactory.getLogger(DefaultAdaptiveExecutionHandler.class);

    private final Configuration configuration;

    private final Map<JobVertexID, ExecutionJobVertexFinishedEvent> jobVertexFinishedEvents =
            new HashMap<>();

    private final List<JobGraphUpdateListener> jobGraphUpdateListeners = new ArrayList<>();

    private final AdaptiveJobGraphManager jobGraphManager;

    private Function<Integer, OperatorID> findOperatorIdByStreamNodeId;

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

    private boolean enableAdaptiveJoinType() {
        return configuration.get(BatchExecutionOptions.ADAPTIVE_JOIN_TYPE_ENABLED);
    }

    private void tryAdjustJoinType(ExecutionJobVertexFinishedEvent event) {
        JobVertexID jobVertexId = event.getVertexId();
        long bytes = 0L;
        for (BlockingResultInfo info : event.getResultInfo()) {
            bytes += info.getNumBytesProduced();
        }

        List<StreamEdge> outputEdges = jobGraphManager.findOutputEdgesByVertexId(jobVertexId);

        for (StreamEdge edge : outputEdges) {
            tryTransferToBroadCastJoin(bytes, edge);
        }
    }

    private void tryTransferToBroadCastJoin(long producedBytes, StreamEdge edge) {
        //        StreamNode node = edge.getTargetNode();
        //        List<StreamEdge> otherEdges =
        //                node.getInEdges().stream()
        //                        .filter(e -> edge.getSourceId() != e.getSourceId())
        //                        .collect(Collectors.toList());
        //        checkState(otherEdges.size() == 1);

        // if can not transfer
        // return false
        // else
        //        logicalGraphManager.modifyToBroadcastJoin(
        //                node, edge, otherEdges.get(0), new RescalePartitioner<>());

        //        if (false) {
        //            jobGraphManager.updateStreamGraph(
        //                    new ConvertToBroadcastJoinRequest(
        //                            node, edge, otherEdges.get(0), new RescalePartitioner<>()));
        //        }
    }

    // TODO currently only support Lazily update job graph
    private void tryUpdateJobGraph(JobVertexID newFinishedJobVertexId) throws Exception {
        List<StreamEdge> edges = jobGraphManager.findOutputEdgesByVertexId(newFinishedJobVertexId);

        List<StreamNode> tryToTranslate = new ArrayList<>();

        for (StreamEdge edge : edges) {
            StreamNode downNode = edge.getTargetNode();

            boolean isAllInputVerticesFinished = true;
            for (StreamEdge inEdge : downNode.getInEdges()) {
                Optional<JobVertexID> upStreamVertex =
                        jobGraphManager.findVertexByStreamNodeId(inEdge.getSourceId());
                if (!upStreamVertex.isPresent()
                        || !jobVertexFinishedEvents.containsKey(upStreamVertex.get())) {
                    isAllInputVerticesFinished = false;
                    break;
                }
            }

            if (isAllInputVerticesFinished) {
                tryToTranslate.add(downNode);
            }
        }

        List<JobVertex> list = jobGraphManager.createJobVerticesAndUpdateGraph(tryToTranslate);

        if (!list.isEmpty()) {
            notifyJobGraphUpdated(list);
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
