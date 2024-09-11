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

/**
 * Defines a context for optimizing and working with a read-only view of a StreamGraph. It provides
 * methods to modify StreamEdges and StreamNodes within the StreamGraph.
 */
public interface StreamGraphContext {

    /**
     * Returns a read-only view of the StreamGraph.
     *
     * @return a read-only view of the StreamGraph.
     */
    StreamGraph getStreamGraph();

    /**
     * Modifies a StreamEdge within the StreamGraph.
     *
     * @param streamEdge the StreamEdge to be modified.
     */
    void modifyStreamEdge(StreamEdge streamEdge);

    /**
     * Modifies a StreamNode within the StreamGraph.
     *
     * @param streamNode the StreamNode to be modified.
     */
    void modifyStreamNode(StreamNode streamNode);
}
