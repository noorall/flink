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

package org.apache.flink.table.runtime.strategy.multipleinput.transformation;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.streaming.api.operators.StreamOperatorFactory;
import org.apache.flink.streaming.api.transformations.AbstractMultipleInputTransformation;
import org.apache.flink.table.runtime.strategy.multipleinput.wrapper.WrappedEdge;
import org.apache.flink.table.runtime.strategy.multipleinput.wrapper.WrappedNode;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

public class WrappedMultipleInputTransformation<OUT> extends AbstractMultipleInputTransformation<OUT> {
    List<WrappedEdge<?>> inputEdges = new ArrayList<>();

    public WrappedMultipleInputTransformation(
            String name,
            StreamOperatorFactory<OUT> operatorFactory,
            TypeInformation<OUT> outputType,
            int parallelism,
            boolean isParallelismConfigured
    ) {
        super(
                name,
                operatorFactory,
                outputType,
                parallelism,
                isParallelismConfigured,
                Collections.emptyList());
    }

    @Override
    public List<TypeInformation<?>> getInputTypes() {
        return inputEdges
                .stream()
                .map(WrappedEdge::getSource)
                .map(WrappedNode::getOutTypeInfo)
                .collect(
                        Collectors.toList());
    }

    public void addWrappedInput(WrappedEdge<?> inputEdge) {
        inputEdges.add(inputEdge);
    }

    public List<WrappedEdge<?>> getWrappedInputs() {
        return Collections.unmodifiableList(inputEdges);
    }

    @Override
    public List<Transformation<?>> getInputs() {
        throw new UnsupportedOperationException("Unsupported operation");
    }

    @Override
    protected List<Transformation<?>> getTransitivePredecessorsInternal() {
        throw new UnsupportedOperationException("Unsupported operation");
    }
}
