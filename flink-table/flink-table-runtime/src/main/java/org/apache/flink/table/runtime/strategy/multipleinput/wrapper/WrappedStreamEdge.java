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

package org.apache.flink.table.runtime.strategy.multipleinput.wrapper;

import org.apache.flink.streaming.api.graph.util.ImmutableStreamEdge;

public class WrappedStreamEdge implements WrappedEdge<ImmutableStreamEdge> {
    private final ImmutableStreamEdge edge;

    private final WrappedNode<?> source;
    private final WrappedNode<?> target;

    private final boolean isFrozen;

    private boolean isBroken;

    public WrappedStreamEdge(
            ImmutableStreamEdge edge,
            WrappedNode<?> source,
            WrappedNode<?> target,
            boolean isFrozen) {
        this.edge = edge;
        this.source = source;
        this.target = target;
        this.isFrozen = isFrozen;
        isBroken = false;
    }

    @Override
    public WrappedNode<?> getSource() {
        return source;
    }

    @Override
    public WrappedNode<?> getTarget() {
        return target;
    }

    @Override
    public int getInputPriority() {
        // TODO: check relationship between input priority and type number
        return edge.getTypeNumber();
    }

    @Override
    public ImmutableStreamEdge getEdge() {
        return edge;
    }

    @Override
    public boolean isFrozen() {
        return isFrozen;
    }

    @Override
    public void broken() {
        isBroken = true;
    }

    @Override
    public boolean isBroken() {
        return isBroken;
    }
}
