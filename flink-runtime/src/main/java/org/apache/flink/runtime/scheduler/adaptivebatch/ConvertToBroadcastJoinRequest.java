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

import org.apache.flink.streaming.api.graph.StreamEdge;
import org.apache.flink.streaming.api.graph.StreamGraphUpdateRequestInfo;
import org.apache.flink.streaming.api.graph.StreamNode;
import org.apache.flink.streaming.runtime.partitioner.StreamPartitioner;

public class ConvertToBroadcastJoinRequest implements StreamGraphUpdateRequestInfo {

    private final StreamNode joinNode;

    private final StreamEdge toBroadCast;

    private final StreamEdge toPointwise;

    private final StreamPartitioner<?> pointwisePartitioner;

    public ConvertToBroadcastJoinRequest(
            StreamNode joinNode,
            StreamEdge toBroadCast,
            StreamEdge toPointwise,
            StreamPartitioner<?> pointwisePartitioner) {
        this.joinNode = joinNode;
        this.toBroadCast = toBroadCast;
        this.toPointwise = toPointwise;
        this.pointwisePartitioner = pointwisePartitioner;
    }

    public StreamNode getJoinNode() {
        return joinNode;
    }

    public StreamEdge getToBroadCast() {
        return toBroadCast;
    }

    public StreamEdge getToPointwise() {
        return toPointwise;
    }

    public StreamPartitioner<?> getPointwisePartitioner() {
        return pointwisePartitioner;
    }
}
