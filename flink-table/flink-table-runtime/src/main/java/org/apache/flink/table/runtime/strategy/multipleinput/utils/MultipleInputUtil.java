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

package org.apache.flink.table.runtime.strategy.multipleinput.utils;

import org.apache.flink.api.dag.Transformation;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.core.memory.ManagedMemoryUseCase;
import org.apache.flink.table.planner.plan.nodes.exec.InputProperty;
import org.apache.flink.table.runtime.strategy.multipleinput.wrapper.WrappedNode;

import java.util.List;
import java.util.Optional;
import java.util.Set;

public class MultipleInputUtil {
    // TODO: implement this part
    public static String getMultipleInputDescription(
            WrappedNode<?> root,
            Set<WrappedNode<?>> inputNodes,
            List<InputProperty> inputProperties) {
        return "RuntimeMultipleInput";
    }

    public static String createTransformationName(ReadableConfig config) {
        return "A MultipleInputTransformation";
    }

    public static String createTransformationDescription(ReadableConfig config) {
        return "A MultipleInputTransformation Description";
    }

    public static <T> void setManagedMemoryWeight(
            Transformation<T> transformation, long memoryBytes) {
        if (memoryBytes > 0) {
            final int weightInMebibyte = Math.max(1, (int) (memoryBytes >> 20));
            final Optional<Integer> previousWeight =
                    transformation.declareManagedMemoryUseCaseAtOperatorScope(
                            ManagedMemoryUseCase.OPERATOR, weightInMebibyte);
            if (previousWeight.isPresent()) {
                throw new RuntimeException(
                        "Managed memory weight has been set, this should not happen.");
            }
        }
    }
}
