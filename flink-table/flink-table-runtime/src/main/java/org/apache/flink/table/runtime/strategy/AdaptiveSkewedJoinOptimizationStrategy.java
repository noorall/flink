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

package org.apache.flink.table.runtime.strategy;

import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.runtime.scheduler.adaptivebatch.OperatorsFinished;
import org.apache.flink.streaming.api.graph.StreamGraphContext;
import org.apache.flink.streaming.api.graph.util.ImmutableStreamEdge;
import org.apache.flink.streaming.api.graph.util.ImmutableStreamNode;
import org.apache.flink.table.api.config.OptimizerConfigOptions;
import org.apache.flink.table.api.config.OptimizerConfigOptions.AdaptiveSkewedJoinStrategy;
import org.apache.flink.table.runtime.operators.join.AdaptiveJoin;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.flink.util.Preconditions.checkArgument;

public class AdaptiveSkewedJoinOptimizationStrategy
        extends BaseAdaptiveJoinOperatorOptimizationStrategy {
    private boolean initialized = false;
    private AdaptiveSkewedJoinStrategy adaptiveSkewedJoinStrategy;
    private long skewedThresholdInBytes;
    private double skewedFactor;

    private Map<Integer, Map<Integer, long[]>> aggregatedProducedBytesByTypeNumberAndNodeId;

    @Override
    public void tryOptimizeAdaptiveJoin(
            OperatorsFinished operatorsFinished,
            StreamGraphContext context,
            ImmutableStreamNode adaptiveJoinNode,
            ImmutableStreamEdge upstreamStreamEdge,
            AdaptiveJoin adaptiveJoin) {
        if (upstreamStreamEdge.isBroadcast() || upstreamStreamEdge.isPointwise()) {
            return;
        }
        operatorsFinished.getResultInfoMap().get(upstreamStreamEdge.getSourceId()).get(1);
    }

    @Override
    public boolean maybeOptimizeStreamGraph(
            OperatorsFinished operatorsFinished, StreamGraphContext context) throws Exception {
        initialize(context.getStreamGraph().getConfiguration());
        visitDownstreamAdaptiveJoinNode(operatorsFinished, context);

        return true;
    }

    private void initialize(ReadableConfig config) {
        if (!initialized) {
            skewedFactor =
                    config.get(
                            OptimizerConfigOptions
                                    .TABLE_OPTIMIZER_ADAPTIVE_SKEWED_JOIN_OPTIMIZATION_SKEWED_FACTOR);
            skewedThresholdInBytes =
                    config.get(
                                    OptimizerConfigOptions
                                            .TABLE_OPTIMIZER_ADAPTIVE_SKEWED_JOIN_OPTIMIZATION_SKEWED_THRESHOLD)
                            .getBytes();
            aggregatedProducedBytesByTypeNumberAndNodeId = new HashMap<>();
            initialized = true;
        }
    }

    private void aggregatedProducedBytesByTypeNumber(
            ImmutableStreamNode adaptiveJoinNode, int typeNumber, List<Long> subpartitionBytes) {
        Integer streamNodeId = adaptiveJoinNode.getId();

        long[] aggregatedSubpartitionBytes =
                aggregatedProducedBytesByTypeNumberAndNodeId
                        .computeIfAbsent(streamNodeId, k -> new HashMap<>())
                        .computeIfAbsent(
                                typeNumber, (ignore) -> new long[subpartitionBytes.size()]);
        checkArgument(subpartitionBytes.size() == aggregatedSubpartitionBytes.length);
        for (int i = 0; i < subpartitionBytes.size(); i++) {
            aggregatedSubpartitionBytes[i] += subpartitionBytes.get(i);
        }
    }

    private void freeNodeStatistic(Integer nodeId) {
        aggregatedProducedBytesByTypeNumberAndNodeId.remove(nodeId);
    }
}
