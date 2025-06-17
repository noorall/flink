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

package org.apache.flink.streaming.api.graph.multinput;

import org.apache.flink.annotation.Internal;
import org.apache.flink.streaming.api.graph.StreamGraph;
import org.apache.flink.streaming.api.graph.StreamNode;

import java.util.HashSet;
import java.util.Set;

@Internal
public abstract class AbstractExecNodeExactlyOnceVisitor {

    private final Set<StreamNode> visited;
    protected final StreamGraph streamGraph;

    public AbstractExecNodeExactlyOnceVisitor(StreamGraph streamGraph) {
        this.visited = new HashSet<>();
        this.streamGraph = streamGraph;
    }

    public void visit(StreamNode node) {
        if (visited.contains(node)) {
            return;
        }
        visited.add(node);
        visitNode(node);
    }

    protected abstract void visitNode(StreamNode node);

    protected void visitInputs(StreamNode node) {
        node.getInEdges().forEach(edge -> visit(streamGraph.getStreamNode(edge.getSourceId())));
    }
}
