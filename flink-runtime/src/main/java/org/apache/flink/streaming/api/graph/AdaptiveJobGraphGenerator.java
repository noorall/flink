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

package org.apache.flink.streaming.api.graph;

import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobVertex;

import java.util.List;

public interface AdaptiveJobGraphGenerator {

    /**
     * Creates new job vertices from a list of stream nodes and updates the job graph accordingly.
     *
     * @param streamNodes A list of {@link StreamNode} instances for which job vertices should be
     *     created.
     * @return A list of newly created {@link JobVertex} instances.
     */
    List<JobVertex> createJobVerticesAndUpdateGraph(List<StreamNode> streamNodes);

    /**
     * Checks if the conversion from StreamGraph to JobGraph has been finished.
     *
     * @return {@code true} if the StreamGraph to JobGraph conversion is finished, {@code false}
     *     otherwise.
     */
    boolean isStreamGraphConversionFinished();

    /**
     * Retrieves the JobGraph representation of the current Flink job.
     *
     * @return The current {@link JobGraph} instance.
     */
    JobGraph getJobGraph();

    /**
     * Updates the StreamGraph based on the specified update request information.
     *
     * @param requestInfo An object implementing {@link StreamGraphUpdateRequestInfo} that contains
     *     all the necessary information for the stream graph update.
     * @return {@code true} if the StreamGraph update was successful, {@code false} otherwise.
     */
    boolean updateStreamGraph(StreamGraphUpdateRequestInfo requestInfo);
}
