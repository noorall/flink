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
import org.apache.flink.configuration.PipelineOptions;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobmaster.event.JobEvent;
import org.apache.flink.runtime.jobmaster.event.ResultConsumableEvent;
import org.apache.flink.streaming.api.graph.StreamEdge;
import org.apache.flink.streaming.api.graph.StreamGraph;
import org.apache.flink.streaming.api.graph.StreamNode;
import org.apache.flink.streaming.api.graph.StreamingJobGraphGenerator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.apache.flink.util.Preconditions.checkNotNull;

/** Default implementation of {@link AdaptiveExecutionHandler} */
public class DefaultAdaptiveExecutionHandler implements AdaptiveExecutionHandler {

    private final Logger log = LoggerFactory.getLogger(DefaultAdaptiveExecutionHandler.class);

    private final StreamGraph streamGraph;

    private final ClassLoader userClassLoader;

    private final Configuration configuration;

    private JobGraph jobGraph;

    private final Map<JobVertexID, ResultConsumableEvent> jobVertexFinishedEvents = new HashMap<>();

    private final List<JobGraphUpdateListener> jobGraphUpdateListeners = new ArrayList<>();

    public DefaultAdaptiveExecutionHandler(
            StreamGraph streamGraph, ClassLoader userClassLoader, Configuration configuration) {
        this.streamGraph = checkNotNull(streamGraph);
        this.userClassLoader = checkNotNull(userClassLoader);
        this.configuration = checkNotNull(configuration);
    }

    @Override
    public JobGraph getJobGraph() {
        if (jobGraph == null) {
            jobGraph = streamGraph.getJobGraphAndAttachUserArtifacts(userClassLoader);
        }
        return jobGraph;
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
        if (jobEvent instanceof ResultConsumableEvent) {
            ResultConsumableEvent event = (ResultConsumableEvent) jobEvent;
            jobVertexFinishedEvents.put(event.getProducerId(), event);

            tryAdjustJoinType(event);
            tryUpdateJobGraph(event);
        }
    }

    private boolean enableAdaptiveJoinType() {
        return configuration.get(BatchExecutionOptions.ADAPTIVE_JOIN_TYPE_ENABLED);
    }

    private boolean isLazilyCreateJobVertex() {
        return configuration
                .get(PipelineOptions.PIPELINE_TRANSLATE_TO_JOB_GRAPH_STRATEGY)
                .equals(PipelineOptions.JobGraphTranslateStrategy.LAZILY);
    }

    private boolean isEagerlyCreateJobVertex() {
        return configuration
                .get(PipelineOptions.PIPELINE_TRANSLATE_TO_JOB_GRAPH_STRATEGY)
                .equals(PipelineOptions.JobGraphTranslateStrategy.EAGERLY);
    }

    private void tryAdjustJoinType(ResultConsumableEvent event) {
        if (enableAdaptiveJoinType()) {
            JobVertexID jobVertexId = event.getProducerId();
            List<BlockingResultInfo> resultInfo = event.getResultInfo();

            List<Integer> downStreamNodes =
                    StreamingJobGraphGenerator.findEndStreamNodeIdsByJobVertexId(jobVertexId);

            for (Integer nodeId : downStreamNodes) {
                StreamNode downNode = streamGraph.getStreamNode(nodeId);
                // TODO try change join type
            }
        }
    }

    private void tryUpdateJobGraph(ResultConsumableEvent event) throws Exception {
        JobVertexID jobVertexId = event.getProducerId();
        List<Integer> downStreamNodes =
                StreamingJobGraphGenerator.findEndStreamNodeIdsByJobVertexId(jobVertexId);

        List<Integer> tryToTranslate = new ArrayList<>();

        // TODO note there should not has if else branch and we should use the method which provided
        // by adaptive job graph generator try to update job graph.
        if (isEagerlyCreateJobVertex()) {
            tryToTranslate = downStreamNodes;
        } else if (isLazilyCreateJobVertex()) {
            for (Integer nodeId : downStreamNodes) {
                StreamNode downNode = streamGraph.getStreamNode(nodeId);

                boolean isAllInputVerticesFinished = true;
                for (StreamEdge inEdge : downNode.getInEdges()) {
                    Optional<JobVertexID> upStreamVertex =
                            StreamingJobGraphGenerator.findJobVertexIdByStreamNodeId(
                                    inEdge.getSourceId());
                    if (!upStreamVertex.isPresent()
                            || !jobVertexFinishedEvents.containsKey(upStreamVertex.get())) {
                        isAllInputVerticesFinished = false;
                        break;
                    }
                }

                if (isAllInputVerticesFinished) {
                    tryToTranslate.add(downNode.getId());
                }
            }
        }

        List<JobVertex> list =
                StreamingJobGraphGenerator.tryCreateJobVerticesAndUpdateJobGraph(
                        userClassLoader, streamGraph, tryToTranslate);

        if (!list.isEmpty()) {
            notifyJobGraphUpdated(list);
        }
    }

    private void notifyJobGraphUpdated(List<JobVertex> jobVertices) throws Exception {
        for (JobGraphUpdateListener listener : jobGraphUpdateListeners) {
            listener.onNewJobVerticesAdded(
                    jobVertices, StreamingJobGraphGenerator.hasMoreJobVerticesToBeAdded());
        }
    }

    @Override
    public void registerJobGraphUpdateListener(JobGraphUpdateListener listener) {
        jobGraphUpdateListeners.add(listener);
    }
}
