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

package org.apache.flink.table.runtime.strategy.multipleinput.translator;

import org.apache.flink.streaming.api.graph.StreamGraphContext;
import org.apache.flink.streaming.api.graph.StreamNode;
import org.apache.flink.streaming.runtime.io.MultipleInputSelectionHandler;
import org.apache.flink.streaming.runtime.tasks.MultipleInputStreamTask;
import org.apache.flink.table.runtime.strategy.multipleinput.transformation.WrappedMultipleInputTransformation;
import org.apache.flink.table.runtime.strategy.multipleinput.wrapper.WrappedEdge;

import java.util.List;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

public class RuntimeMutiInputTransformationTranslator<OUT> {
    public StreamNode translateInternal(
            final WrappedMultipleInputTransformation<OUT> transformation,
            StreamGraphContext context,
            String slotSharingGroup
    ) {
        checkNotNull(transformation);
        checkNotNull(context);

        final List<WrappedEdge<?>> inputEdges = transformation.getWrappedInputs();
        checkArgument(
                !inputEdges.isEmpty(),
                "Empty inputs for MultipleInputTransformation. Did you forget to add inputs?");
        MultipleInputSelectionHandler.checkSupportedInputCount(inputEdges.size());

        // TODO process id
        final int transformationId = transformation.getId();
        StreamNode streamNode = new StreamNode(
                transformationId,
                slotSharingGroup,
                transformation.getCoLocationGroupKey(),
                transformation.getOperatorFactory(),
                transformation.getName(),
                MultipleInputStreamTask.class,
                transformation.getInputTypes(),
                transformation.getOutputType(),
                transformation.getInputProperties());

        streamNode.setAttribute(transformation.getAttribute());
        streamNode.setParallelism(
                transformation.getParallelism(),
                transformation.isParallelismConfigured());
        streamNode.setMaxParallelism(transformation.getMaxParallelism());
        // TODO process input edges
        streamNode.setSupportsConcurrentExecutionAttempts(
                transformation.isSupportsConcurrentExecutionAttempts());

        return streamNode;
    }
}
