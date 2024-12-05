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

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobgraph.forwardgroup.StreamNodeForwardGroup;
import org.apache.flink.runtime.jobmaster.event.ExecutionJobVertexFinishedEvent;
import org.apache.flink.runtime.jobmaster.event.JobEvent;
import org.apache.flink.streaming.api.graph.AdaptiveGraphManager;
import org.apache.flink.streaming.api.graph.StreamGraph;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executor;

/** Default implementation of {@link AdaptiveExecutionHandler}. */
public class DefaultAdaptiveExecutionHandler implements AdaptiveExecutionHandler {

    private final Logger log = LoggerFactory.getLogger(DefaultAdaptiveExecutionHandler.class);

    private final List<JobGraphUpdateListener> jobGraphUpdateListeners = new ArrayList<>();

    private final AdaptiveGraphManager adaptiveGraphManager;

    public DefaultAdaptiveExecutionHandler(
            ClassLoader userClassloader, StreamGraph streamGraph, Executor serializationExecutor) {
        this.adaptiveGraphManager =
                new AdaptiveGraphManager(userClassloader, streamGraph, serializationExecutor);
    }

    @Override
    public JobGraph getJobGraph() {
        return adaptiveGraphManager.getJobGraph();
    }

    @Override
    public void handleJobEvent(JobEvent jobEvent) {
        try {
            tryUpdateJobGraph(jobEvent);
        } catch (Exception e) {
            log.error("Failed to handle job event {}.", jobEvent, e);
            throw new RuntimeException(e);
        }
    }

    private void tryUpdateJobGraph(JobEvent jobEvent) throws Exception {
        if (jobEvent instanceof ExecutionJobVertexFinishedEvent) {
            ExecutionJobVertexFinishedEvent event = (ExecutionJobVertexFinishedEvent) jobEvent;

            List<JobVertex> newlyCreatedJobVertices =
                    adaptiveGraphManager.onJobVertexFinished(event.getVertexId());

            if (!newlyCreatedJobVertices.isEmpty()) {
                notifyJobGraphUpdated(
                        newlyCreatedJobVertices, adaptiveGraphManager.getPendingOperatorsCount());
            }
        }
    }

    private void notifyJobGraphUpdated(List<JobVertex> jobVertices, int pendingOperatorsCount)
            throws Exception {
        for (JobGraphUpdateListener listener : jobGraphUpdateListeners) {
            listener.onNewJobVerticesAdded(jobVertices, pendingOperatorsCount);
        }
    }

    @Override
    public void registerJobGraphUpdateListener(JobGraphUpdateListener listener) {
        jobGraphUpdateListeners.add(listener);
    }

    @Override
    public int getInitialParallelism(JobVertexID jobVertexId) {
        JobVertex jobVertex = adaptiveGraphManager.getJobGraph().findVertexByID(jobVertexId);
        int vertexInitialParallelism = jobVertex.getParallelism();
        StreamNodeForwardGroup forwardGroup =
                adaptiveGraphManager.getStreamNodeForwardGroupByVertexId(jobVertexId);

        if (jobVertex.getParallelism() == ExecutionConfig.PARALLELISM_DEFAULT
                && forwardGroup != null
                && forwardGroup.isParallelismDecided()) {
            vertexInitialParallelism = forwardGroup.getParallelism();
            log.info(
                    "Parallelism of JobVertex: {} ({}) is decided to be {} according to forward group's parallelism.",
                    jobVertex.getName(),
                    jobVertexId,
                    vertexInitialParallelism);
        }

        return vertexInitialParallelism;
    }

    @Override
    public void notifyJobVertexParallelismDecided(JobVertexID jobVertexId, int parallelism) {
        StreamNodeForwardGroup forwardGroup =
                adaptiveGraphManager.getStreamNodeForwardGroupByVertexId(jobVertexId);
        if (forwardGroup != null && !forwardGroup.isParallelismDecided()) {
            forwardGroup.setParallelism(parallelism);
        }
    }

    @Override
    public ExecutionPlanSchedulingContext createExecutionPlanSchedulingContext(
            int defaultMaxParallelism) {
        return new AdaptiveExecutionPlanSchedulingContext(
                adaptiveGraphManager, defaultMaxParallelism);
    }
}
