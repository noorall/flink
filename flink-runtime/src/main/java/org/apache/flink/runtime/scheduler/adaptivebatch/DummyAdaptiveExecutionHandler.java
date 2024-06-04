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

import org.apache.flink.runtime.executiongraph.ExecutionJobVertex;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.jobgraph.forwardgroup.ForwardGroup;
import org.apache.flink.runtime.jobgraph.forwardgroup.ForwardGroupComputeUtil;
import org.apache.flink.runtime.jobmaster.event.JobEvent;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.Map;
import java.util.function.Function;

import static org.apache.flink.util.Preconditions.checkNotNull;

/** A dummy implementation of {@link AdaptiveExecutionHandler}. */
public class DummyAdaptiveExecutionHandler implements AdaptiveExecutionHandler {

    private final Logger log = LoggerFactory.getLogger(getClass());
    private final JobGraph jobGraph;
    private @Nullable Function<Integer, OperatorID> findOperatorIdByStreamNodeId;
    private final Map<JobVertexID, ForwardGroup> forwardGroupsByJobVertexId;

    public DummyAdaptiveExecutionHandler(
            JobGraph jobGraph,
            @Nullable Function<Integer, OperatorID> findOperatorIdByStreamNodeId) {
        this.jobGraph = checkNotNull(jobGraph);
        this.findOperatorIdByStreamNodeId = findOperatorIdByStreamNodeId;
        this.forwardGroupsByJobVertexId =
                ForwardGroupComputeUtil.computeForwardGroupsAndCheckParallelism(
                        getJobGraph().getVerticesSortedTopologicallyFromSources());
    }

    @Override
    public JobGraph getJobGraph() {
        return jobGraph;
    }

    @Override
    public void handleJobEvent(JobEvent jobEvent) {
        // do nothing
    }

    @Override
    public void registerJobGraphUpdateListener(JobGraphUpdateListener listener) {
        // do nothing
    }

    @Override
    public boolean isStreamGraphConversionFinished() {
        return true;
    }

    @Override
    public void initializeJobGraph() {}

    @Override
    public OperatorID findOperatorIdByStreamNodeId(int streamNodeId) {
        checkNotNull(findOperatorIdByStreamNodeId);

        return findOperatorIdByStreamNodeId.apply(streamNodeId);
    }

    @Override
    public int getInitialParallelismByForwardGroup(ExecutionJobVertex jobVertex) {
        int vertexInitialParallelism = jobVertex.getParallelism();
        ForwardGroup forwardGroup = forwardGroupsByJobVertexId.get(jobVertex.getJobVertexId());
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
        ForwardGroup forwardGroup = forwardGroupsByJobVertexId.get(jobVertex.getJobVertexId());
        if (forwardGroup != null && !forwardGroup.isParallelismDecided()) {
            forwardGroup.setParallelism(parallelism);
        }
    }
}
