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

package org.apache.flink.table.planner.plan.nodes.exec.processor;

import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.api.config.OptimizerConfigOptions;
import org.apache.flink.table.planner.plan.nodes.exec.ExecEdge;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNode;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeGraph;
import org.apache.flink.table.planner.plan.nodes.exec.InputProperty;
import org.apache.flink.table.planner.plan.nodes.exec.InputProperty.KeepInputAsIsDistribution;
import org.apache.flink.table.planner.plan.nodes.exec.InputProperty.DistributionType;
import org.apache.flink.table.planner.plan.nodes.exec.batch.BatchExecAdaptiveJoin;
import org.apache.flink.table.planner.plan.nodes.exec.batch.BatchExecHashJoin;
import org.apache.flink.table.planner.plan.nodes.exec.batch.BatchExecSortMergeJoin;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecNode;
import org.apache.flink.table.planner.plan.nodes.exec.visitor.AbstractExecNodeExactlyOnceVisitor;
import org.apache.flink.table.planner.plan.utils.OperatorType;
import org.apache.flink.table.planner.utils.TableConfigUtils;

import java.util.ArrayList;
import java.util.List;

/**
 * A {@link ExecNodeGraphProcessor} which replace join nodes into adaptive join nodes.
 */
public class AdaptiveJoinNodeProcessor implements ExecNodeGraphProcessor {

    @Override
    public ExecNodeGraph process(ExecNodeGraph execGraph, ProcessorContext context) {
        if (execGraph.getRootNodes().get(0) instanceof StreamExecNode) {
            throw new TableException("StreamExecNode is not supported yet");
        }
        TableConfig tableConfig = context.getPlanner().getTableConfig();
        JobManagerOptions.SchedulerType schedulerType =
                context.getPlanner()
                        .getExecEnv()
                        .getConfig()
                        .getSchedulerType()
                        .orElse(JobManagerOptions.SchedulerType.AdaptiveBatch);
        if (!tableConfig.get(OptimizerConfigOptions.TABLE_OPTIMIZER_ADAPTIVE_JOIN_ENABLED)
                || TableConfigUtils.isOperatorDisabled(
                tableConfig, OperatorType.BroadcastHashJoin)) {
            return execGraph;
        }
        if (schedulerType != JobManagerOptions.SchedulerType.AdaptiveBatch) {
            return execGraph;
        }

        AbstractExecNodeExactlyOnceVisitor visitor =
                new AbstractExecNodeExactlyOnceVisitor() {
                    @Override
                    protected void visitNode(ExecNode<?> node) {
                        if (node.getInputProperties().stream()
                                .anyMatch(inputProperty ->
                                        inputProperty.getRequiredDistribution() instanceof KeepInputAsIsDistribution
                                                && ((KeepInputAsIsDistribution)inputProperty.getRequiredDistribution()).isStrict())) {
                            return;
                        }
                        for (int i = 0; i < node.getInputEdges().size(); ++i) {
                            ExecEdge edge = node.getInputEdges().get(i);
                            ExecNode<?> input = edge.getSource();
                            if (!checkAllInputShuffleIsHash(input)) {
                                continue;
                            }
                            if (input instanceof BatchExecHashJoin) {
                                BatchExecAdaptiveJoin adaptiveJoin
                                        = ((BatchExecHashJoin) input).toAdaptiveJoin();
                                replaceInputEdge(adaptiveJoin, input);
                                input = adaptiveJoin;
                            } else if (input instanceof BatchExecSortMergeJoin) {
                                BatchExecAdaptiveJoin adaptiveJoin
                                        = ((BatchExecSortMergeJoin) input).toAdaptiveJoin();
                                replaceInputEdge(adaptiveJoin, input);
                                input = adaptiveJoin;
                            }
                            node.replaceInputEdge(i, ExecEdge.builder()
                                    .source(input)
                                    .target(node)
                                    .shuffle(edge.getShuffle())
                                    .exchangeMode(edge.getExchangeMode())
                                    .build());
                        }
                        visitInputs(node);
                    }
                };

        execGraph.getRootNodes().forEach(node -> node.accept(visitor));
        return execGraph;
    }

    private boolean checkAllInputShuffleIsHash(ExecNode<?> input) {
        for (InputProperty inputProperty : input.getInputProperties()) {
            if (inputProperty.getRequiredDistribution().getType() != DistributionType.HASH) {
                return false;
            }
        }
        return true;
    }

    private void replaceInputEdge(ExecNode<?> newNode, ExecNode<?> originalNode) {
        List<ExecEdge> inputEdges = new ArrayList<>();
        for (int i = 0; i < originalNode.getInputEdges().size(); ++i) {
            ExecEdge edge = originalNode.getInputEdges().get(i);
            inputEdges.add(ExecEdge.builder()
                    .source(edge.getSource())
                    .target(newNode)
                    .shuffle(edge.getShuffle())
                    .exchangeMode(edge.getExchangeMode())
                    .build());
        }
        newNode.setInputEdges(inputEdges);
    }
}
