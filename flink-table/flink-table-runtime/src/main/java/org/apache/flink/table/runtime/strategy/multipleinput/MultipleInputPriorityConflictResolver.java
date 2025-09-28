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

package org.apache.flink.table.runtime.strategy.multipleinput;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.planner.plan.nodes.exec.InputProperty;
import org.apache.flink.table.runtime.strategy.multipleinput.utils.InputPriorityGraphGenerator;
import org.apache.flink.table.runtime.strategy.multipleinput.wrapper.WrappedNode;

import java.util.List;
import java.util.Set;


/**
 * Subclass of the {@link InputPriorityGraphGenerator}.
 *
 * <p>This class resolve conflicts by mark input edge as broken for the conflicting
 * input.
 */
@Internal
public class MultipleInputPriorityConflictResolver extends InputPriorityGraphGenerator {
    /**
     * Create a {@link MultipleInputPriorityConflictResolver} for the given {@link WrappedNode} graph.
     *
     * @param roots the first layer of nodes on the output side of the graph
     * @param safeDamBehavior when checking for conflicts we'll ignore the edges with {@link
     *         InputProperty.DamBehavior} stricter or equal than this
     */
    public MultipleInputPriorityConflictResolver(
            List<WrappedNode<?>> roots,
            Set<Integer> boundaries,
            InputProperty.DamBehavior safeDamBehavior) {
        super(roots, boundaries, safeDamBehavior);
    }

    public void detectAndResolve() {
        createTopologyGraph();
    }

    @Override
    protected void resolveInputPriorityConflict(
            WrappedNode<?> node,
            int higherInput,
            int lowerInput) {
        node.getInputEdges().get(lowerInput).broken();
    }
}
