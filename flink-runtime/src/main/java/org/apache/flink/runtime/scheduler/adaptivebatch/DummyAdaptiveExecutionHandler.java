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

import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.jobmaster.event.JobEvent;

import javax.annotation.Nullable;

import java.util.function.Function;

import static org.apache.flink.util.Preconditions.checkNotNull;

/** A dummy implementation of {@link AdaptiveExecutionHandler}. */
public class DummyAdaptiveExecutionHandler implements AdaptiveExecutionHandler {

    private final JobGraph jobGraph;
    private @Nullable Function<Integer, OperatorID> findOperatorIdByStreamNodeId;

    public DummyAdaptiveExecutionHandler(
            JobGraph jobGraph,
            @Nullable Function<Integer, OperatorID> findOperatorIdByStreamNodeId) {
        this.jobGraph = checkNotNull(jobGraph);
        this.findOperatorIdByStreamNodeId = findOperatorIdByStreamNodeId;
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
}
