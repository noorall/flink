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

import org.apache.flink.table.runtime.strategy.multipleinput.wrapper.WrappedNode;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class MultipleInputNodeCreateResult {
    private final List<WrappedNode<?>> removedNodes;
    private final List<WrappedNode<?>> createdNodes;

    public MultipleInputNodeCreateResult() {
        removedNodes = new ArrayList<>();
        createdNodes = new ArrayList<>();
    }

    public void addRemovedNode(WrappedNode<?> removedNode) {
        removedNodes.add(removedNode);
    }

    public void addCreatedNode(WrappedNode<?> createdNode) {
        createdNodes.add(createdNode);
    }

    public List<WrappedNode<?>> getRemovedNodes() {
        return Collections.unmodifiableList(removedNodes);
    }

    public List<WrappedNode<?>> getCreatedNodes() {
        return Collections.unmodifiableList(createdNodes);
    }

    public boolean isEmpty() {
        return removedNodes.isEmpty() && createdNodes.isEmpty();
    }
}
