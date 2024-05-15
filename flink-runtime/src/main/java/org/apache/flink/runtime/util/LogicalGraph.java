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

package org.apache.flink.runtime.util;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.operators.ResourceSpec;
import org.apache.flink.runtime.blob.PermanentBlobKey;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobType;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobgraph.tasks.JobCheckpointingSettings;
import org.apache.flink.streaming.api.graph.StreamGraph;
import org.apache.flink.streaming.api.graph.StreamNode;
import org.apache.flink.types.Either;

import java.net.URL;
import java.util.List;

import static org.apache.flink.util.Preconditions.checkState;

/** A wrapper that either contains {@link JobGraph} or {@link StreamGraph} */
public class LogicalGraph {

    private final Either<JobGraph, StreamGraph> graph;

    public LogicalGraph(Either<JobGraph, StreamGraph> graph) {
        this.graph = graph;
    }

    public static LogicalGraph createLogicalGraph(Object graph) {
        if (graph == null) {
            return null;
        }

        checkState(graph instanceof JobGraph || graph instanceof StreamGraph);
        if (graph instanceof JobGraph) {
            return new LogicalGraph(Either.Left((JobGraph) graph));
        } else {
            return new LogicalGraph(Either.Right((StreamGraph) graph));
        }
    }

    public LogicalGraph(StreamGraph streamGraph) {
        this.graph = Either.Right(streamGraph);
    }

    public JobID getJobId() {
        return graph.isLeft() ? graph.left().getJobID() : graph.right().getJobId();
    }

    public String getJobName() {
        return graph.isLeft() ? graph.left().getName() : graph.right().getJobName();
    }

    public boolean isJobGraph() {
        return graph.isLeft();
    }

    public StreamGraph getStreamGraph() {
        return graph.right();
    }

    public JobGraph getJobGraph() {
        return graph.left();
    }

    public boolean isEmpty() {
        return !graph.isLeft() && !graph.isRight();
    }

    public long getInitialClientHeartbeatTimeout() {
        return graph.isLeft()
                ? graph.left().getInitialClientHeartbeatTimeout()
                : graph.right().getInitialClientHeartbeatTimeout();
    }

    public JobType getJobType() {
        return graph.isLeft() ? graph.left().getJobType() : graph.right().getJobType();
    }

    public boolean isDynamic() {
        return graph.isLeft() ? graph.left().isDynamic() : graph.right().isDynamic();
    }

    public List<URL> getClassPaths() {
        return graph.isLeft() ? graph.left().getClasspaths() : graph.right().getClasspaths();
    }

    public List<PermanentBlobKey> getUserJarBlobKeys() {
        return graph.isLeft()
                ? graph.left().getUserJarBlobKeys()
                : graph.right().getUserJarBlobKeys();
    }

    public JobCheckpointingSettings getJobCheckpointingSettings() {
        return graph.isLeft()
                ? graph.left().getCheckpointingSettings()
                : graph.right().getJobCheckpointingSettings();
    }

    public boolean isPartialResourceConfigured() {
        boolean hasVerticesWithUnknownResource = false;
        boolean hasVerticesWithConfiguredResource = false;

        if (isJobGraph()) {
            for (JobVertex jobVertex : getJobGraph().getVertices()) {
                if (jobVertex.getMinResources() == ResourceSpec.UNKNOWN) {
                    hasVerticesWithUnknownResource = true;
                } else {
                    hasVerticesWithConfiguredResource = true;
                }

                if (hasVerticesWithUnknownResource && hasVerticesWithConfiguredResource) {
                    return true;
                }
            }
        } else {
            for (StreamNode streamNode : getStreamGraph().getStreamNodes()) {
                if (streamNode.getMinResources() == ResourceSpec.UNKNOWN) {
                    hasVerticesWithUnknownResource = true;
                } else {
                    hasVerticesWithConfiguredResource = true;
                }

                if (hasVerticesWithUnknownResource && hasVerticesWithConfiguredResource) {
                    return true;
                }
            }
        }

        return false;
    }

    @Override
    public String toString() {
        return isJobGraph() ? getJobGraph().toString() : getStreamGraph().toString();
    }
}
