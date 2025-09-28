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
import org.apache.flink.util.Preconditions;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class MultipleInputGroup {
    // We use list instead of set here to ensure that the inputs of a multiple input node
    // will not change order. Although order changes do not affect the correctness of the
    // query, it does affect plan test cases.
    private final List<WrappedNode<?>> members;

    // root is output of group
    private WrappedNode<?> root;

    public MultipleInputGroup(WrappedNode<?> root) {
        this.members = new ArrayList<>();
        members.add(root);
        this.root = root;
    }

    public void addMember(WrappedNode<?> wrapper) {
        Preconditions.checkState(
                wrapper.getMultipleInputGroup() == null,
                "The given exec node wrapper is already in a multiple input group. This is a bug.");
        members.add(wrapper);
        wrapper.setMultipleInputGroup(this);
    }

    public void removeMember(WrappedNode<?> wrapper) {
        if (wrapper == root) {
            removeRoot();
        } else {
            Preconditions.checkState(
                    members.remove(wrapper),
                    "The given exec node wrapper does not exist in the multiple input group. This is a bug.");
            wrapper.setMultipleInputGroup(null);
        }
    }

    public WrappedNode<?> getRoot() {
        return root;
    }

    public int getMemberSize() {
        return members.size();
    }

    public List<WrappedNode<?>> getMembers() {
        return members;
    }

    public void removeRoot() {
        Preconditions.checkNotNull(
                root, "Multiple input group does not have a root. This is a bug.");
        Set<WrappedNode<?>> sameGroupInputWrappers = new HashSet<>();
        for (WrappedNode<?> inputWrapper : root.getInputs()) {
            if (members.contains(inputWrapper)) {
                sameGroupInputWrappers.add(inputWrapper);
            }
        }
        Preconditions.checkState(
                sameGroupInputWrappers.size() < 2,
                "There are two or more inputs of the root remaining in the multiple input group. This is a bug.");

        members.remove(root);
        root.setMultipleInputGroup(null);
        if (sameGroupInputWrappers.isEmpty()) {
            root = null;
        } else {
            root = sameGroupInputWrappers.iterator().next();
        }
    }
}
