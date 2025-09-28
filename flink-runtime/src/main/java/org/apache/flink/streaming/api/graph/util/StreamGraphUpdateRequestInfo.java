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

package org.apache.flink.streaming.api.graph.util;

import org.apache.flink.streaming.api.graph.StreamNode;

import java.util.List;

public class StreamGraphUpdateRequestInfo {
    private final List<Integer> nodesToRemove;
    private final List<StreamNode> nodesToAdd;
    private final List<StreamEdgeUpdateRequestInfo> edgesToUpdate;
    private final List<List<Integer>> forwardGroupsNeedToMerge;

    public StreamGraphUpdateRequestInfo(
            List<Integer> nodesToRemove,
            List<StreamNode> nodesToAdd,
            List<StreamEdgeUpdateRequestInfo> edgesToUpdate,
            List<List<Integer>> forwardGroupsNeedToMerge) {
        this.nodesToRemove = nodesToRemove;
        this.nodesToAdd = nodesToAdd;
        this.edgesToUpdate = edgesToUpdate;
        this.forwardGroupsNeedToMerge = forwardGroupsNeedToMerge;
    }
}
