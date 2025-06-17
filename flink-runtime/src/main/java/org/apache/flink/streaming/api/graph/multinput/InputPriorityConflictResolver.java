/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.api.graph.multinput;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.planner.plan.nodes.exec.InputProperty;

/**
 * Subclass of the {@link InputPriorityGraphGenerator}.
 *
 * <p>This class resolve conflicts by inserting a {@link BatchExecExchange} into the conflicting
 * input.
 */
@Internal
public class InputPriorityConflictResolver extends InputPriorityGraphGenerator {

    private final StreamExecGraph streamExecGraph;

    /**
     * Create a {@link InputPriorityConflictResolver} for the given {@link ExecNode} graph.
     *
     * @param roots the first layer of nodes on the output side of the graph
     * @param safeDamBehavior when checking for conflicts we'll ignore the edges with {@link
     *     InputProperty.DamBehavior} stricter or equal than this
     * @param exchangeMode when a conflict occurs we'll insert an {@link BatchExecExchange} node
     *     with this exchange mode to resolve conflict
     */
    public InputPriorityConflictResolver(
            StreamExecGraph streamExecGraph, InputProperty.DamBehavior safeDamBehavior) {
        super(streamExecGraph.getRoots(), streamExecGraph.getFrozenNodes(), safeDamBehavior);
        this.streamExecGraph = streamExecGraph;
    }

    public void detectAndResolve() {
        createTopologyGraph();
    }

    @Override
    protected void resolveInputPriorityConflict(StreamExecNode node, int lowerInput) {
        // broken lower input edge, insert an exchange
        streamExecGraph.brokenEdge(node, lowerInput);
    }
}
