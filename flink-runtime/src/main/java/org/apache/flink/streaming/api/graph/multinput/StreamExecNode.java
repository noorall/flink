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
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.graph.StreamNode;
import org.apache.flink.table.planner.plan.nodes.exec.InputProperty;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

@Internal
public class StreamExecNode {

    @Nullable private final StreamNode streamNode;
    private final Map<Integer, InputProperty> inputPropertiesMapping;
    private final List<StreamExecEdge> inputs;

    private static final Logger LOG = LoggerFactory.getLogger(StreamExecNode.class);
    private final TypeInformation<?> outTypeInfo;

    public StreamExecNode(
            StreamNode node,
            List<StreamExecEdge> inputs,
            Map<Integer, InputProperty> inputPropertiesMapping) {
        this.streamNode = node;
        this.inputs = checkNotNull(inputs);

        checkNotNull(inputPropertiesMapping);

        this.inputPropertiesMapping = inputPropertiesMapping;

        checkArgument(
                inputs.stream().map(StreamExecEdge::getTypeNumber).distinct().count()
                        == inputPropertiesMapping.size());
        this.outTypeInfo = node.getOutTypeInfo();
    }

    public StreamExecNode(StreamNode node) {
        this.streamNode = node;
        this.inputs = new ArrayList<>();
        this.inputPropertiesMapping = new HashMap<>();
        this.outTypeInfo = node.getOutTypeInfo();
    }

    public StreamExecNode(
            TypeInformation<?> outTypeInfo,
            List<StreamExecEdge> inputs,
            Map<Integer, InputProperty> inputPropertiesMapping) {
        this.streamNode = null;
        this.inputs = checkNotNull(inputs);

        checkNotNull(inputPropertiesMapping);

        this.inputPropertiesMapping = inputPropertiesMapping;

        checkArgument(
                inputs.stream().map(StreamExecEdge::getTypeNumber).distinct().count()
                        == inputPropertiesMapping.size());
        this.outTypeInfo = outTypeInfo;
    }

    public List<StreamExecEdge> getInputEdges() {
        return inputs;
    }

    public void accept(AbstractStreamExecNodeExactlyOnceVisitor visitor) {
        visitor.visit(this);
    }

    public List<InputProperty> getInputProperties() {
        return inputs.stream()
                .map(StreamExecEdge::getTypeNumber)
                .map(inputPropertiesMapping::get)
                .collect(Collectors.toList());
    }

    public StreamExecEdge broken(int indexInTarget) {
        StreamExecEdge edge = inputs.get(indexInTarget);
        LOG.info("{} should be broken", edge);
        edge.broken();

        StreamExecEdge newEdge = new StreamExecEdge(edge.getSource(), edge.getTypeNumber());

        InputProperty property = getInputProperties().get(indexInTarget);
        InputProperty newInputProperty =
                InputProperty.builder()
                        .requiredDistribution(property.getRequiredDistribution())
                        .priority(property.getPriority())
                        .damBehavior(InputProperty.DamBehavior.BLOCKING)
                        .build();
        StreamExecNode node =
                new StreamExecNode(
                        edge.getSource().outTypeInfo,
                        List.of(newEdge),
                        Map.of(newEdge.getTypeNumber(), newInputProperty));
        edge.setSource(node);

        return newEdge;
    }

    @Nullable
    public StreamNode getStreamNode() {
        return streamNode;
    }

    public TypeInformation<?> getOutTypeInfo() {
        return outTypeInfo;
    }

    public void reverseInputProperties() {
        checkState(inputPropertiesMapping.size() == 2);
        InputProperty property1 = inputPropertiesMapping.get(1);
        InputProperty property2 = inputPropertiesMapping.get(2);
        inputPropertiesMapping.put(1, property2);
        inputPropertiesMapping.put(2, property1);
    }

    @Override
    public String toString() {
        if (streamNode == null) {
            return "StreamExecNode{" + "streamNode=null" + '}';
        }
        return "StreamExecNode{"
                + "streamNode="
                + streamNode
                + ", p = "
                + streamNode.getParallelism()
                + ", maxP = "
                + streamNode.getMaxParallelism()
                + '}';
    }
}
