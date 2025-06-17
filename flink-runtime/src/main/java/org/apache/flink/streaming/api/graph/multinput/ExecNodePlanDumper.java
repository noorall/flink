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
import org.apache.flink.streaming.api.graph.StreamEdge;
import org.apache.flink.streaming.api.graph.StreamGraph;
import org.apache.flink.streaming.api.graph.StreamNode;

import java.util.ArrayList;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static org.apache.flink.util.Preconditions.checkNotNull;

/** An utility class for converting an exec node plan to a string as a tree style. */
@Internal
public class ExecNodePlanDumper {

    /**
     * Converts an ExecNode tree to a string as a tree style.
     *
     * @param node the ExecNode to convert
     * @param borders node sets that stop visit when meet them
     * @param includingBorders Whether print the border nodes
     * @return the plan of ExecNode
     */
    public static String treeToString(
            StreamNode node,
            List<StreamNode> borders,
            boolean includingBorders,
            StreamGraph streamGraph) {
        checkNotNull(node, "node should not be null.");
        // convert to mutable list
        List<StreamNode> borderList =
                new ArrayList<>(checkNotNull(borders, "borders should not be null."));
        TreeReuseInfo reuseInfo = new TreeReuseInfo(node, borderList, streamGraph);
        return doConvertTreeToString(
                node, reuseInfo, true, borderList, includingBorders, streamGraph);
    }

    private static String doConvertTreeToString(
            StreamNode node,
            ReuseInfo reuseInfo,
            boolean updateVisitedTimes,
            List<StreamNode> stopVisitNodes,
            boolean includingBorders,
            StreamGraph streamGraph) {
        StringBuilder sb = new StringBuilder();
        ExecNodeStringTreeBuilder visitor =
                new ExecNodeStringTreeBuilder(
                        sb,
                        reuseInfo,
                        updateVisitedTimes,
                        stopVisitNodes,
                        includingBorders,
                        streamGraph);
        visitor.visit(node);
        return sb.toString();
    }

    /** A class that describe the reuse info for the given DAG or tree. */
    private abstract static class ReuseInfo {
        // build reuse id
        private final ReuseIdBuilder reuseIdBuilder;
        // mapping node object to visited times
        protected final Map<StreamNode, Integer> mapNodeToVisitedTimes;

        protected ReuseInfo(
                List<StreamNode> nodes, List<StreamNode> borders, StreamGraph streamGraph) {
            this.reuseIdBuilder = new ReuseIdBuilder(borders, streamGraph);
            nodes.forEach(reuseIdBuilder::visit);
            this.mapNodeToVisitedTimes = new IdentityHashMap<>();
        }

        /** Returns reuse id if the given node is a reuse node, else -1. */
        Integer getReuseId(StreamNode node) {
            return reuseIdBuilder.getReuseId(node);
        }

        /** Returns true if the given node is first visited, else false. */
        abstract boolean isFirstVisited(StreamNode node);

        /** Updates visited times for given node, return the new times. */
        int addVisitedTimes(StreamNode node) {
            return mapNodeToVisitedTimes.compute(node, (k, v) -> v == null ? 1 : v + 1);
        }
    }

    /** ReuseInfo} for node tree. */
    private static class TreeReuseInfo extends ReuseInfo {
        public TreeReuseInfo(StreamNode node, List<StreamNode> borders, StreamGraph streamGraph) {
            super(Collections.singletonList(node), borders, streamGraph);
        }

        @Override
        boolean isFirstVisited(StreamNode node) {
            return mapNodeToVisitedTimes.get(node) == 1;
        }
    }

    /** Build reuse id in an ExecNode DAG or tree. */
    private static class ReuseIdBuilder {
        private final List<StreamNode> borders;
        // visited node set
        private final Set<StreamNode> visitedNodes =
                Collections.newSetFromMap(new IdentityHashMap<>());
        // mapping reuse node to its reuse id
        private final Map<StreamNode, Integer> mapReuseNodeToReuseId = new IdentityHashMap<>();
        private final AtomicInteger reuseIdGenerator = new AtomicInteger(0);
        private final StreamGraph streamGraph;

        public ReuseIdBuilder(List<StreamNode> borders, StreamGraph streamGraph) {
            this.borders = borders;
            this.streamGraph = streamGraph;
        }

        public void visit(StreamNode node) {
            if (borders.contains(node)) {
                return;
            }

            // if a node is visited more than once, this node is a reusable node
            if (visitedNodes.contains(node)) {
                if (!mapReuseNodeToReuseId.containsKey(node)) {
                    int reuseId = reuseIdGenerator.incrementAndGet();
                    mapReuseNodeToReuseId.put(node, reuseId);
                }
            } else {
                visitedNodes.add(node);
                visitInputs(node);
            }
        }

        protected void visitInputs(StreamNode node) {
            node.getInEdges().forEach(n -> this.visit(streamGraph.getStreamNode(n.getSourceId())));
        }

        /**
         * Returns reuse id if the given node is a reuse node (that means it has multiple outputs),
         * else -1.
         */
        public Integer getReuseId(StreamNode node) {
            return mapReuseNodeToReuseId.getOrDefault(node, -1);
        }
    }

    /** Convert ExecNode tree to string as a tree style. */
    private static class ExecNodeStringTreeBuilder {
        private final StringBuilder sb;
        private final ReuseInfo reuseInfo;
        private final boolean updateVisitedTimes;
        private final List<StreamNode> stopVisitNodes;
        private final boolean includingBorders;
        private final StreamGraph streamGraph;

        private List<Boolean> lastChildren = new ArrayList<>();
        private int depth = 0;

        private ExecNodeStringTreeBuilder(
                StringBuilder sb,
                ReuseInfo reuseInfo,
                boolean updateVisitedTimes,
                List<StreamNode> stopVisitNodes,
                boolean includingBorders,
                StreamGraph streamGraph) {
            this.sb = sb;
            this.reuseInfo = reuseInfo;
            this.updateVisitedTimes = updateVisitedTimes;
            this.stopVisitNodes = stopVisitNodes;
            this.includingBorders = includingBorders;
            this.streamGraph = streamGraph;
        }

        public void visit(StreamNode node) {
            if (updateVisitedTimes) {
                reuseInfo.addVisitedTimes(node);
            }
            if (depth > 0) {
                lastChildren
                        .subList(0, lastChildren.size() - 1)
                        .forEach(isLast -> sb.append(isLast ? "   " : ":  "));
                sb.append(lastChildren.get(lastChildren.size() - 1) ? "+- " : ":- ");
            }
            final int borderIndex = stopVisitNodes.indexOf(node);
            final boolean reachBorder = borderIndex >= 0;
            if (reachBorder && includingBorders) {
                sb.append("[#").append(borderIndex + 1).append("] ");
            }

            final int reuseId = reuseInfo.getReuseId(node);
            final boolean isReuseNode = reuseId >= 0;
            final boolean firstVisited = reuseInfo.isFirstVisited(node);

            if (isReuseNode && !firstVisited) {
                sb.append("Reused");
            } else {
                sb.append(node.getOperatorDescription());
            }

            if (isReuseNode) {
                if (firstVisited) {
                    sb.append("(reuse_id=[").append(reuseId).append("])");
                } else {
                    sb.append("(reference_id=[").append(reuseId).append("])");
                }
            }
            sb.append("\n");

            // whether visit input nodes of current node
            final boolean visitInputs =
                    (firstVisited || !isReuseNode) && !stopVisitNodes.contains(node);
            final List<StreamNode> inputNodes =
                    node.getInEdges().stream()
                            .map(StreamEdge::getSourceId)
                            .map(streamGraph::getStreamNode)
                            .collect(Collectors.toList());
            if (visitInputs && inputNodes.size() > 1) {
                inputNodes
                        .subList(0, inputNodes.size() - 1)
                        .forEach(
                                input -> {
                                    depth = depth + 1;
                                    lastChildren.add(false);
                                    this.visit(input);
                                    depth = depth - 1;
                                    lastChildren = lastChildren.subList(0, lastChildren.size() - 1);
                                });
            }
            if (visitInputs && !inputNodes.isEmpty()) {
                depth = depth + 1;
                lastChildren.add(true);
                this.visit(inputNodes.get(inputNodes.size() - 1));
                depth = depth - 1;
                lastChildren = lastChildren.subList(0, lastChildren.size() - 1);
            }
        }
    }

    private ExecNodePlanDumper() {}
}
