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

package org.apache.flink.streaming.api.graph;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.operators.ResourceSpec;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.IllegalConfigurationException;
import org.apache.flink.runtime.OperatorIDPair;
import org.apache.flink.runtime.jobgraph.InputOutputFormatVertex;
import org.apache.flink.runtime.jobgraph.IntermediateDataSet;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.jobgraph.forwardgroup.StreamNodeForwardGroup;
import org.apache.flink.runtime.jobmanager.scheduler.CoLocationGroupImpl;
import org.apache.flink.runtime.jobmanager.scheduler.SlotSharingGroup;
import org.apache.flink.runtime.operators.coordination.OperatorCoordinator;
import org.apache.flink.runtime.operators.util.TaskConfig;
import org.apache.flink.streaming.api.operators.ChainingStrategy;
import org.apache.flink.streaming.api.operators.SourceOperatorFactory;
import org.apache.flink.streaming.runtime.partitioner.ForwardForConsecutiveHashPartitioner;
import org.apache.flink.streaming.runtime.partitioner.ForwardForUnspecifiedPartitioner;
import org.apache.flink.streaming.runtime.partitioner.ForwardPartitioner;
import org.apache.flink.streaming.runtime.partitioner.StreamPartitioner;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.SerializedValue;
import org.apache.flink.util.concurrent.FutureUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.apache.flink.streaming.api.graph.StreamingJobGraphGenerator.addVertexIndexPrefixInVertexName;
import static org.apache.flink.streaming.api.graph.StreamingJobGraphGenerator.connect;
import static org.apache.flink.streaming.api.graph.StreamingJobGraphGenerator.createChainedName;
import static org.apache.flink.streaming.api.graph.StreamingJobGraphGenerator.isChainable;
import static org.apache.flink.streaming.api.graph.StreamingJobGraphGenerator.isChainableInput;
import static org.apache.flink.streaming.api.graph.StreamingJobGraphGenerator.markSupportingConcurrentExecutionAttempts;
import static org.apache.flink.streaming.api.graph.StreamingJobGraphGenerator.preValidate;
import static org.apache.flink.streaming.api.graph.StreamingJobGraphGenerator.setCoLocation;
import static org.apache.flink.streaming.api.graph.StreamingJobGraphGenerator.setManagedMemoryFraction;
import static org.apache.flink.streaming.api.graph.StreamingJobGraphGenerator.setOperatorChainedOutputsConfig;
import static org.apache.flink.streaming.api.graph.StreamingJobGraphGenerator.setOperatorConfig;
import static org.apache.flink.streaming.api.graph.StreamingJobGraphGenerator.setOperatorNonChainedOutputsConfig;
import static org.apache.flink.streaming.api.graph.StreamingJobGraphGenerator.setPhysicalEdges;
import static org.apache.flink.streaming.api.graph.StreamingJobGraphGenerator.setSlotSharing;
import static org.apache.flink.streaming.api.graph.StreamingJobGraphGenerator.setVertexDescription;
import static org.apache.flink.streaming.api.graph.StreamingJobGraphGenerator.tryConvertPartitionerForDynamicGraph;
import static org.apache.flink.streaming.api.graph.StreamingJobGraphGenerator.validateHybridShuffleExecuteInBatchMode;

public class AdaptiveJobGraphManager implements AdaptiveJobGraphGenerator, JobVertexMapper {

    // TODO: 所有以startNode为key的变量全部放到ChainInfo里去

    private static final Logger LOG = LoggerFactory.getLogger(AdaptiveJobGraphManager.class);

    private final StreamGraph streamGraph;

    private final Map<Integer, JobVertex> jobVerticesCache;

    private final JobGraph jobGraph;

    // the ids of nodes whose output result partition type should be set to BLOCKING
    private final Set<Integer> outputBlockingNodesID;

    private final StreamGraphHasher defaultStreamGraphHasher;

    private final StreamGraphHasher legacyStreamGraphHasher;

    private final AtomicBoolean hasHybridResultPartition;

    private final Executor serializationExecutor;

    // Futures for the serialization of operator coordinators
    private final Map<
                    JobVertexID,
                    List<CompletableFuture<SerializedValue<OperatorCoordinator.Provider>>>>
            coordinatorSerializationFuturesPerJobVertex = new HashMap<>();

    private final Map<Integer, Integer> frozenNodeToStartNodeMap;

    private final Map<JobVertexID, Integer> jobVertexToStartNodeMap;

    private final Map<Integer, byte[]> hashes;

    private final Map<Integer, byte[]> legacyHashes;

    // sourceNode -> <targetNodeId, NonChainedOutput>
    // need update when edge changed
    private final Map<Integer, Map<StreamEdge, NonChainedOutput>> opIntermediateOutputsCaches;

    private final GenerateMode generateMode;

    private final Map<Integer, StreamNodeForwardGroup> forwardGroupsByEndpointNodeIdCache;

    private final StreamGraphManagerContext streamGraphManagerContext;

    private final AtomicInteger vertexIndexId;

    private final SlotSharingGroup defaultSlotSharingGroup;

    private final Map<JobVertexID, SlotSharingGroup> vertexRegionSlotSharingGroups;

    private final Map<String, Tuple2<SlotSharingGroup, CoLocationGroupImpl>> coLocationGroups;

    /** The {@link OperatorChainInfo}s, key is the start node id of the chain. */
    private final Map<Integer, OperatorChainInfo> chainInfos;

    private final Map<Integer, OperatorChainInfo> pendingSourceChainInfos;

    private final Set<JobVertexID> finishedJobVertices;

    @VisibleForTesting
    public AdaptiveJobGraphManager(
            ClassLoader userClassloader,
            StreamGraph streamGraph,
            Executor serializationExecutor,
            GenerateMode generateMode) {
        preValidate(streamGraph, userClassloader);
        this.streamGraph = streamGraph;
        this.defaultStreamGraphHasher = new StreamGraphHasherV2();
        this.legacyStreamGraphHasher = new StreamGraphUserHashHasher();
        this.jobVerticesCache = new LinkedHashMap<>();
        this.outputBlockingNodesID = new HashSet<>();
        this.serializationExecutor = Preconditions.checkNotNull(serializationExecutor);
        this.chainInfos = new HashMap<>();
        this.frozenNodeToStartNodeMap = new HashMap<>();
        this.hashes = new HashMap<>();
        this.legacyHashes = new HashMap<>();
        this.generateMode = generateMode;
        this.hasHybridResultPartition = new AtomicBoolean(false);
        this.opIntermediateOutputsCaches = new HashMap<>();
        this.jobVertexToStartNodeMap = new HashMap<>();
        this.forwardGroupsByEndpointNodeIdCache = new HashMap<>();

        this.jobGraph = new JobGraph(streamGraph.getJobId(), streamGraph.getJobName());

        this.jobGraph.setClasspaths(streamGraph.getClasspaths());
        this.jobGraph.setSnapshotSettings(streamGraph.getJobCheckpointingSettings());
        this.jobGraph.setSavepointRestoreSettings(streamGraph.getSavepointRestoreSettings());
        this.jobGraph.setJobType(streamGraph.getJobType());
        this.jobGraph.setDynamic(streamGraph.isDynamic());
        this.jobGraph.setJobConfiguration(streamGraph.getJobConfiguration());

        // set the ExecutionConfig last when it has been finalized
        try {
            jobGraph.setExecutionConfig(streamGraph.getExecutionConfig());
        } catch (IOException e) {
            throw new IllegalConfigurationException(
                    "Could not serialize the ExecutionConfig."
                            + "This indicates that non-serializable types (like custom serializers) were registered");
        }

        this.jobGraph.enableApproximateLocalRecovery(
                streamGraph.getCheckpointConfig().isApproximateLocalRecoveryEnabled());

        streamGraph.getUserJarBlobKeys().forEach(jobGraph::addUserJarBlobKey);

        this.streamGraphManagerContext =
                new StreamGraphManagerContext(
                        forwardGroupsByEndpointNodeIdCache,
                        streamGraph,
                        frozenNodeToStartNodeMap,
                        opIntermediateOutputsCaches);

        this.vertexIndexId = new AtomicInteger(0);
        this.defaultSlotSharingGroup = new SlotSharingGroup();
        streamGraph
                .getSlotSharingGroupResource(StreamGraphGenerator.DEFAULT_SLOT_SHARING_GROUP)
                .ifPresent(defaultSlotSharingGroup::setResourceProfile);

        this.vertexRegionSlotSharingGroups = new HashMap<>();
        this.coLocationGroups = new HashMap<>();
        this.pendingSourceChainInfos = new TreeMap<>();
        this.finishedJobVertices = new HashSet<>();
        initializeJobGraph();
    }

    @Override
    public boolean isStreamGraphConversionFinished() {
        return streamGraph.getStreamNodes().size() == frozenNodeToStartNodeMap.size();
    }

    @Override
    public JobGraph getJobGraph() {
        return this.jobGraph;
    }

    @Override
    public List<JobVertex> onJobVertexFinished(JobVertexID finishedJobVertexId) {
        this.finishedJobVertices.add(finishedJobVertexId);
        if (generateMode == GenerateMode.EAGERLY) {
            return Collections.emptyList();
        }
        List<StreamNode> streamNodes = new ArrayList<>();
        for (StreamEdge outEdge : findOutputEdgesByVertexId(finishedJobVertexId)) {
            streamNodes.add(outEdge.getTargetNode());
        }
        return createJobVerticesAndUpdateGraph(streamNodes);
    }

    @Override
    public boolean updateStreamGraph(Function<StreamGraphManagerContext, Boolean> updateFunc) {
        return updateFunc.apply(streamGraphManagerContext);
    }

    @Override
    public Optional<JobVertexID> findVertexByStreamNodeId(int streamNodeId) {
        if (frozenNodeToStartNodeMap.containsKey(streamNodeId)) {
            Integer startNodeId = frozenNodeToStartNodeMap.get(streamNodeId);
            return Optional.of(jobVerticesCache.get(startNodeId).getID());
        }
        return Optional.empty();
    }

    @Override
    public List<StreamEdge> findOutputEdgesByVertexId(JobVertexID jobVertexId) {
        if (!jobVertexToStartNodeMap.containsKey(jobVertexId)) {
            return Collections.emptyList();
        }
        Integer startNodeId = jobVertexToStartNodeMap.get(jobVertexId);
        return chainInfos.get(startNodeId).getTransitiveOutEdges();
    }

    @Override
    public StreamNodeForwardGroup findForwardGroupByVertexId(JobVertexID jobVertexId) {
        Integer startNodeId = jobVertexToStartNodeMap.get(jobVertexId);
        return forwardGroupsByEndpointNodeIdCache.get(startNodeId);
    }

    private void initializeJobGraph() {
        List<StreamNode> sourceNodes =
                streamGraph.getStreamNodes().stream()
                        .filter(node -> node.getInEdges().isEmpty())
                        .collect(Collectors.toList());
        if (jobGraph.isDynamic()) {
            setVertexParallelismsForDynamicGraphIfNecessary(sourceNodes);
        }
        createJobVerticesAndUpdateGraph(sourceNodes);
    }

    private List<JobVertex> createJobVerticesAndUpdateGraph(List<StreamNode> streamNodes) {
        Map<Integer, OperatorBuildContext> operatorBuildContexts = new LinkedHashMap<>();

        Map<Integer, OperatorChainInfo> chainInfos =
                createOperatorChainInfos(streamNodes, operatorBuildContexts);

        Map<Integer, JobVertex> createdJobVertices =
                createJobVerticesByChainInfos(chainInfos, operatorBuildContexts);

        generateConfigForJobVertices(createdJobVertices, chainInfos, operatorBuildContexts);

        return new ArrayList<>(createdJobVertices.values());
    }

    private void generateConfigForJobVertices(
            Map<Integer, JobVertex> jobVertices,
            Map<Integer, OperatorChainInfo> chainInfos,
            Map<Integer, OperatorBuildContext> operatorBuildContexts) {
        chainInfos
                .values()
                .forEach(chainInfo -> initStreamConfigs(chainInfo, operatorBuildContexts));
        generateAllOutputConfigs(jobVertices, chainInfos, operatorBuildContexts);
        finalizeConfig(jobVertices, chainInfos, operatorBuildContexts);
    }

    private void setVertexParallelismsForDynamicGraphIfNecessary(List<StreamNode> streamNodes) {
        List<StreamEdge> chainableOutputs = new ArrayList<>();

        Map<Integer, List<StreamEdge>> transitiveOutEdgesMap = new HashMap<>();
        Map<Integer, List<StreamNode>> chainedStreamNodesMap = new LinkedHashMap<>();
        Set<Integer> finishedChain = new HashSet<>();
        for (StreamNode streamNode : streamNodes) {
            traverseFullGraph(
                    streamNode.getId(),
                    streamNode.getId(),
                    chainableOutputs,
                    transitiveOutEdgesMap,
                    chainedStreamNodesMap,
                    finishedChain);
        }
        computeForwardGroupAndSetNodeParallelisms(
                transitiveOutEdgesMap, chainedStreamNodesMap, chainableOutputs);
    }

    private void traverseFullGraph(
            Integer startNodeId,
            Integer currentNodeId,
            List<StreamEdge> allChainableOutputs,
            Map<Integer, List<StreamEdge>> transitiveOutEdgesMap,
            Map<Integer, List<StreamNode>> chainedStreamNodesMap,
            Set<Integer> finishedChain) {
        if (finishedChain.contains(startNodeId)) {
            return;
        }
        List<StreamEdge> chainableOutputs = new ArrayList<>();
        List<StreamEdge> nonChainableOutputs = new ArrayList<>();
        StreamNode currentNode = streamGraph.getStreamNode(currentNodeId);
        for (StreamEdge streamEdge : currentNode.getOutEdges()) {
            if (isChainable(streamEdge, streamGraph)) {
                chainableOutputs.add(streamEdge);
            } else {
                nonChainableOutputs.add(streamEdge);
            }
        }

        allChainableOutputs.addAll(chainableOutputs);

        transitiveOutEdgesMap
                .computeIfAbsent(startNodeId, k -> new ArrayList<>())
                .addAll(nonChainableOutputs);

        chainedStreamNodesMap.computeIfAbsent(startNodeId, k -> new ArrayList<>()).add(currentNode);

        for (StreamEdge chainable : chainableOutputs) {
            traverseFullGraph(
                    startNodeId,
                    chainable.getTargetId(),
                    allChainableOutputs,
                    transitiveOutEdgesMap,
                    chainedStreamNodesMap,
                    finishedChain);
        }
        for (StreamEdge nonChainable : nonChainableOutputs) {
            traverseFullGraph(
                    nonChainable.getTargetId(),
                    nonChainable.getTargetId(),
                    allChainableOutputs,
                    transitiveOutEdgesMap,
                    chainedStreamNodesMap,
                    finishedChain);
        }
        if (currentNodeId.equals(startNodeId)) {
            finishedChain.add(startNodeId);
        }
    }

    private void computeForwardGroupAndSetNodeParallelisms(
            Map<Integer, List<StreamEdge>> transitiveOutEdgesMap,
            Map<Integer, List<StreamNode>> chainedStreamNodesMap,
            List<StreamEdge> chainableOutputs) {
        // reset parallelism for job vertices whose parallelism is not configured
        for (List<StreamNode> chainedStreamNodes : chainedStreamNodesMap.values()) {
            boolean isParallelismConfigured =
                    chainedStreamNodes.stream().anyMatch(StreamNode::isParallelismConfigured);
            if (!isParallelismConfigured && streamGraph.isAutoParallelismEnabled()) {
                chainedStreamNodes.forEach(
                        n -> n.setParallelism(ExecutionConfig.PARALLELISM_DEFAULT, false));
            }
        }

        final Map<StreamNode, Set<StreamNode>> forwardProducersByStartNode = new LinkedHashMap<>();

        for (int startNodeId : chainedStreamNodesMap.keySet()) {
            Set<StreamNode> forwardConsumers =
                    transitiveOutEdgesMap.get(startNodeId).stream()
                            .filter(
                                    edge -> {
                                        StreamPartitioner<?> partitioner = edge.getPartitioner();
                                        if (partitioner
                                                        instanceof
                                                        ForwardForConsecutiveHashPartitioner
                                                || partitioner
                                                        instanceof
                                                        ForwardForUnspecifiedPartitioner) {
                                            return chainableOutputs.contains(edge);
                                        }
                                        return partitioner instanceof ForwardPartitioner;
                                    })
                            .map(StreamEdge::getTargetId)
                            .map(streamGraph::getStreamNode)
                            .collect(Collectors.toSet());
            for (StreamNode forwardConsumer : forwardConsumers) {
                forwardProducersByStartNode
                        .computeIfAbsent(forwardConsumer, ignored -> new HashSet<>())
                        .add(streamGraph.getStreamNode(startNodeId));
            }
        }

        final Map<Integer, StreamNodeForwardGroup> forwardGroupsByStartNodeId =
                StreamNodeForwardGroup.computeForwardGroup(
                        chainedStreamNodesMap.keySet(),
                        startNode ->
                                forwardProducersByStartNode.getOrDefault(
                                        startNode, Collections.emptySet()),
                        chainedStreamNodesMap::get,
                        streamGraph);

        forwardGroupsByStartNodeId.forEach(
                (startNodeId, forwardGroup) -> {
                    this.forwardGroupsByEndpointNodeIdCache.put(startNodeId, forwardGroup);
                    transitiveOutEdgesMap
                            .get(startNodeId)
                            .forEach(
                                    streamEdge -> {
                                        this.forwardGroupsByEndpointNodeIdCache.put(
                                                streamEdge.getSourceId(), forwardGroup);
                                    });
                });

        chainedStreamNodesMap.forEach(this::setNodeParallelism);
    }

    private void setNodeParallelism(Integer startNodeId, List<StreamNode> chainedStreamNodes) {
        StreamNodeForwardGroup streamNodeForwardGroup =
                forwardGroupsByEndpointNodeIdCache.get(startNodeId);
        // set parallelism for vertices in forward group
        if (streamNodeForwardGroup != null && streamNodeForwardGroup.isParallelismDecided()) {
            chainedStreamNodes.forEach(
                    streamNode ->
                            streamNode.setParallelism(
                                    streamNodeForwardGroup.getParallelism(), true));
        }
        if (streamNodeForwardGroup != null && streamNodeForwardGroup.isMaxParallelismDecided()) {
            chainedStreamNodes.forEach(
                    streamNode ->
                            streamNode.setMaxParallelism(
                                    streamNodeForwardGroup.getMaxParallelism()));
        }
    }

    private void initStreamConfigs(
            OperatorChainInfo chainInfo, Map<Integer, OperatorBuildContext> operatorBuildContexts) {
        int startNodeId = chainInfo.getStartNodeId();
        List<StreamNode> chainedNodes = chainInfo.getAllChainedNodes();
        for (int i = 0; i < chainedNodes.size(); i++) {
            int currentNodeId = chainedNodes.get(i).getId();
            StreamConfig config;

            if (chainInfo.getChainedSources().containsKey(currentNodeId)) {
                config = chainInfo.getChainedSources().get(currentNodeId).getOperatorConfig();
            } else if (currentNodeId == startNodeId) {
                config = new StreamConfig(jobVerticesCache.get(currentNodeId).getConfiguration());
            } else {
                config = new StreamConfig(new Configuration());
            }

            if (currentNodeId == startNodeId) {
                config.setChainStart();
                config.setTransitiveChainedTaskConfigs(chainInfo.getChainedConfigs());
            } else {
                chainInfo.addChainedConfig(currentNodeId, config);
            }
            config.setChainIndex(i);
            config.setOperatorName(chainedNodes.get(currentNodeId).getOperatorName());
            config.setOperatorID(new OperatorID(hashes.get(currentNodeId)));
            setOperatorConfig(
                    currentNodeId,
                    config,
                    chainInfo.getChainedSources(),
                    streamGraph,
                    chainInfo.getChainedConfigs());
            operatorBuildContexts.get(currentNodeId).setStreamConfig(config);
            setOperatorChainedOutputsConfig(
                    config,
                    operatorBuildContexts.get(startNodeId).getChainableOutEdges(),
                    streamGraph);
            if (operatorBuildContexts.get(startNodeId).getChainableOutEdges().isEmpty()) {
                config.setChainEnd();
            }
        }
    }

    private void generateAllOutputConfigs(
            Map<Integer, JobVertex> jobVertices,
            Map<Integer, OperatorChainInfo> chainInfos,
            Map<Integer, OperatorBuildContext> operatorBuildContexts) {
        setAllOperatorNonChainedOutputsConfigs(operatorBuildContexts);
        List<StreamEdge> physicalEdgesInOrder = new ArrayList<>();
        for (Integer startNodeId : jobVertices.keySet()) {
            setVertexNonChainedOutputsConfig(
                    startNodeId,
                    operatorBuildContexts.get(startNodeId).getStreamConfig(),
                    chainInfos.get(startNodeId).getTransitiveOutEdges(),
                    physicalEdgesInOrder);
        }
        connectNonChainedInput(chainInfos, physicalEdgesInOrder);
        setPhysicalEdges(
                physicalEdgesInOrder, key -> operatorBuildContexts.get(key).getStreamConfig());
    }

    private void setAllOperatorNonChainedOutputsConfigs(
            Map<Integer, OperatorBuildContext> operatorBuildContexts) {
        // set non chainable output config
        operatorBuildContexts.forEach(
                (vertexId, operatorBuildContext) -> {
                    Map<StreamEdge, NonChainedOutput> outputsConsumedByEdge =
                            opIntermediateOutputsCaches.computeIfAbsent(
                                    vertexId, ignored -> new HashMap<>());
                    setOperatorNonChainedOutputsConfig(
                            vertexId,
                            operatorBuildContext.getStreamConfig(),
                            operatorBuildContext.getNonChainableOutEdges(),
                            outputsConsumedByEdge,
                            streamGraph,
                            outputBlockingNodesID,
                            hasHybridResultPartition);
                });
    }

    private void finalizeConfig(
            Map<Integer, JobVertex> jobVertices,
            Map<Integer, OperatorChainInfo> chainInfos,
            Map<Integer, OperatorBuildContext> operatorBuildContexts) {
        markSupportingConcurrentExecutionAttempts(
                jobVertices, streamGraph, id -> chainInfos.get(id).getChainedConfigs());

        validateHybridShuffleExecuteInBatchMode(hasHybridResultPartition, jobGraph);

        setSlotSharingAndCoLocation(jobVertices, hasHybridResultPartition, streamGraph, jobGraph);

        setManagedMemoryFraction(
                Collections.unmodifiableMap(jobVerticesCache),
                id -> operatorBuildContexts.get(id).getStreamConfig(),
                id -> chainInfos.get(id).getChainedConfigs(),
                id -> streamGraph.getStreamNode(id).getManagedMemoryOperatorScopeUseCaseWeights(),
                id -> streamGraph.getStreamNode(id).getManagedMemorySlotScopeUseCases());

        if (streamGraph.isVertexNameIncludeIndexPrefix()) {
            addVertexIndexPrefixInVertexName(new ArrayList<>(jobVertices.values()), vertexIndexId);
        }

        setVertexDescription(jobVertices, streamGraph, k -> chainInfos.get(k).getChainedConfigs());

        try {
            FutureUtils.combineAll(
                            operatorBuildContexts.values().stream()
                                    .map(
                                            context ->
                                                    context.getStreamConfig()
                                                            .triggerSerializationAndReturnFuture(
                                                                    serializationExecutor))
                                    .collect(Collectors.toList()))
                    .get();

            waitForSerializationFuturesAndUpdateJobVertices();
        } catch (Exception e) {
            throw new FlinkRuntimeException("Error in serialization.", e);
        }

        if (!streamGraph.getJobStatusHooks().isEmpty()) {
            jobGraph.setJobStatusHooks(streamGraph.getJobStatusHooks());
        }
    }

    private void setSlotSharingAndCoLocation(
            Map<Integer, JobVertex> jobVertices,
            AtomicBoolean hasHybridResultPartition,
            StreamGraph streamGraph,
            JobGraph jobGraph) {
        setSlotSharing(
                jobVertices,
                hasHybridResultPartition,
                streamGraph,
                jobGraph,
                defaultSlotSharingGroup,
                vertexRegionSlotSharingGroups);
        setCoLocation(jobVertices, streamGraph, coLocationGroups);
    }

    private void waitForSerializationFuturesAndUpdateJobVertices()
            throws ExecutionException, InterruptedException {
        for (Map.Entry<
                        JobVertexID,
                        List<CompletableFuture<SerializedValue<OperatorCoordinator.Provider>>>>
                futuresPerJobVertex : coordinatorSerializationFuturesPerJobVertex.entrySet()) {
            final JobVertexID jobVertexId = futuresPerJobVertex.getKey();
            final JobVertex jobVertex = jobGraph.findVertexByID(jobVertexId);

            Preconditions.checkState(
                    jobVertex != null,
                    "OperatorCoordinator providers were registered for JobVertexID '%s' but no corresponding JobVertex can be found.",
                    jobVertexId);
            FutureUtils.combineAll(futuresPerJobVertex.getValue())
                    .get()
                    .forEach(jobVertex::addOperatorCoordinator);
        }
    }

    private void setVertexNonChainedOutputsConfig(
            Integer startNodeId,
            StreamConfig config,
            List<StreamEdge> transitiveOutEdges,
            List<StreamEdge> physicalEdgesInOrder) {
        LinkedHashSet<NonChainedOutput> transitiveOutputs = new LinkedHashSet<>();
        for (StreamEdge edge : transitiveOutEdges) {
            NonChainedOutput output = opIntermediateOutputsCaches.get(edge.getSourceId()).get(edge);
            transitiveOutputs.add(output);
            if (jobVerticesCache.containsKey(edge.getTargetId())) {
                connect(startNodeId, edge, output, physicalEdgesInOrder, jobVerticesCache);
            } else {
                JobVertex jobVertex = jobVerticesCache.get(startNodeId);
                IntermediateDataSet dataSet =
                        jobVertex.getOrCreateResultDataSet(
                                output.getDataSetId(), output.getPartitionType());
                dataSet.addStreamEdge(edge);
            }
        }
        config.setVertexNonChainedOutputs(new ArrayList<>(transitiveOutputs));
    }

    private void connectNonChainedInput(
            Map<Integer, OperatorChainInfo> chainInfos, List<StreamEdge> physicalEdgesInOrder) {
        for (OperatorChainInfo chainInfo : chainInfos.values()) {
            List<StreamEdge> streamEdges = chainInfo.getTransitiveInEdges();
            for (StreamEdge edge : streamEdges) {
                NonChainedOutput output =
                        opIntermediateOutputsCaches.get(edge.getSourceId()).get(edge);
                Integer sourceStartNodeId = frozenNodeToStartNodeMap.get(edge.getSourceId());
                connect(sourceStartNodeId, edge, output, physicalEdgesInOrder, jobVerticesCache);
            }
        }
    }

    private Map<Integer, JobVertex> createJobVerticesByChainInfos(
            Map<Integer, OperatorChainInfo> chainInfos,
            Map<Integer, OperatorBuildContext> operatorBuildContexts) {
        Map<Integer, JobVertex> createdJobVertices = new LinkedHashMap<>();
        for (OperatorChainInfo chainInfo : chainInfos.values()) {
            JobVertex jobVertex = createJobVertex(chainInfo, operatorBuildContexts);
            createdJobVertices.put(chainInfo.getStartNodeId(), jobVertex);
            jobVertexToStartNodeMap.put(jobVertex.getID(), chainInfo.getStartNodeId());
            chainInfo
                    .getAllChainedNodes()
                    .forEach(
                            node ->
                                    frozenNodeToStartNodeMap.put(
                                            node.getId(), chainInfo.getStartNodeId()));
        }
        return createdJobVertices;
    }

    private JobVertex createJobVertex(
            OperatorChainInfo chainInfo, Map<Integer, OperatorBuildContext> operatorBuildContexts) {
        JobVertex jobVertex;
        Integer streamNodeId = chainInfo.getStartNodeId();
        StreamNode streamNode = streamGraph.getStreamNode(streamNodeId);

        byte[] hash = hashes.get(streamNodeId);

        if (hash == null) {
            throw new IllegalStateException(
                    "Cannot find node hash. "
                            + "Did you generate them before calling this method?");
        }

        JobVertexID jobVertexId = new JobVertexID(hash);

        List<Tuple2<byte[], byte[]>> chainedOperators =
                chainInfo.getChainedOperatorHashes(streamNodeId);
        List<OperatorIDPair> operatorIDPairs = new ArrayList<>();
        if (chainedOperators != null) {
            for (Tuple2<byte[], byte[]> chainedOperator : chainedOperators) {
                OperatorID userDefinedOperatorID =
                        chainedOperator.f1 == null ? null : new OperatorID(chainedOperator.f1);
                operatorIDPairs.add(
                        OperatorIDPair.of(
                                new OperatorID(chainedOperator.f0), userDefinedOperatorID));
            }
        }

        if (chainInfo.hasFormatContainer()) {
            jobVertex =
                    new InputOutputFormatVertex(
                            operatorBuildContexts.get(streamNodeId).getChainedName(),
                            jobVertexId,
                            operatorIDPairs);
            chainInfo
                    .getOrCreateFormatContainer()
                    .write(new TaskConfig(jobVertex.getConfiguration()));
        } else {
            jobVertex =
                    new JobVertex(
                            operatorBuildContexts.get(streamNodeId).getChainedName(),
                            jobVertexId,
                            operatorIDPairs);
        }

        if (streamNode.getConsumeClusterDatasetId() != null) {
            jobVertex.addIntermediateDataSetIdToConsume(streamNode.getConsumeClusterDatasetId());
        }

        final List<CompletableFuture<SerializedValue<OperatorCoordinator.Provider>>>
                serializationFutures = new ArrayList<>();
        for (OperatorCoordinator.Provider coordinatorProvider :
                chainInfo.getCoordinatorProviders()) {
            serializationFutures.add(
                    CompletableFuture.supplyAsync(
                            () -> {
                                try {
                                    return new SerializedValue<>(coordinatorProvider);
                                } catch (IOException e) {
                                    throw new FlinkRuntimeException(
                                            String.format(
                                                    "Coordinator Provider for node %s is not serializable.",
                                                    operatorBuildContexts
                                                            .get(streamNodeId)
                                                            .getChainedName()),
                                            e);
                                }
                            },
                            serializationExecutor));
        }
        if (!serializationFutures.isEmpty()) {
            coordinatorSerializationFuturesPerJobVertex.put(jobVertexId, serializationFutures);
        }

        jobVertex.setResources(
                operatorBuildContexts.get(streamNodeId).getMinResource(),
                operatorBuildContexts.get(streamNodeId).getPreferredResources());

        jobVertex.setInvokableClass(streamNode.getJobVertexClass());

        int parallelism = streamNode.getParallelism();

        if (parallelism > 0) {
            jobVertex.setParallelism(parallelism);
        } else {
            parallelism = jobVertex.getParallelism();
        }

        jobVertex.setMaxParallelism(streamNode.getMaxParallelism());

        if (LOG.isDebugEnabled()) {
            LOG.debug("Parallelism set: {} for {}", parallelism, streamNodeId);
        }

        jobVerticesCache.put(streamNodeId, jobVertex);

        jobGraph.addVertex(jobVertex);

        jobVertex.setParallelismConfigured(
                chainInfo.getAllChainedNodes().stream()
                        .anyMatch(StreamNode::isParallelismConfigured));

        return jobVertex;
    }

    private Map<Integer, OperatorChainInfo> createOperatorChainInfos(
            List<StreamNode> streamNodes,
            Map<Integer, OperatorBuildContext> operatorBuildContexts) {
        final Map<Integer, OperatorChainInfo> chainEntryPoints =
                buildAndGetChainEntryPoints(streamNodes, operatorBuildContexts);
        final List<OperatorChainInfo> chainInfos = new ArrayList<>(chainEntryPoints.values());
        for (OperatorChainInfo info : chainInfos) {
            generateOperatorChainInfo(
                    info.getStartNodeId(), info, chainEntryPoints, operatorBuildContexts);
        }
        return chainEntryPoints;
    }

    private Map<Integer, OperatorChainInfo> buildAndGetChainEntryPoints(
            List<StreamNode> streamNodes,
            Map<Integer, OperatorBuildContext> operatorBuildContexts) {
        for (StreamNode streamNode : streamNodes) {
            buildChainEntryPoint(streamNode);
        }
        return getChainEntryPoints(operatorBuildContexts);
    }

    private void buildChainEntryPoint(StreamNode streamNode) {
        int streamNodeId = streamNode.getId();
        if (!isSourceChainable(streamNode)) {
            pendingSourceChainInfos.computeIfAbsent(
                    streamNodeId, ignored -> new OperatorChainInfo(streamNodeId, streamGraph));
        } else {
            generateHashesByStreamNode(streamNode);
            final StreamEdge sourceOutEdge = streamNode.getOutEdges().get(0);
            final int startNodeId = sourceOutEdge.getTargetId();

            final SourceOperatorFactory<?> sourceOpFact =
                    (SourceOperatorFactory<?>) streamNode.getOperatorFactory();

            Preconditions.checkNotNull(sourceOpFact);

            final OperatorCoordinator.Provider coordinatorProvider =
                    sourceOpFact.getCoordinatorProvider(
                            streamNode.getOperatorName(),
                            new OperatorID(hashes.get(streamNode.getId())));

            final StreamConfig operatorConfig = new StreamConfig(new Configuration());
            final StreamConfig.SourceInputConfig inputConfig =
                    new StreamConfig.SourceInputConfig(sourceOutEdge);
            operatorConfig.setOperatorName(streamNode.getOperatorName());

            OperatorChainInfo chainInfo =
                    pendingSourceChainInfos.computeIfAbsent(
                            startNodeId,
                            ignored -> new OperatorChainInfo(startNodeId, streamGraph));

            chainInfo.addChainedSource(
                    streamNodeId, new ChainedSourceInfo(operatorConfig, inputConfig));
            chainInfo.recordChainedNode(streamNodeId);
            chainInfo.addCoordinatorProvider(coordinatorProvider);
        }
    }

    private Map<Integer, OperatorChainInfo> getChainEntryPoints(
            Map<Integer, OperatorBuildContext> operatorBuildContexts) {

        final Map<Integer, OperatorChainInfo> chainEntryPoints = new TreeMap<>();
        Iterator<Map.Entry<Integer, OperatorChainInfo>> iterator =
                pendingSourceChainInfos.entrySet().iterator();

        while (iterator.hasNext()) {
            Map.Entry<Integer, OperatorChainInfo> entry = iterator.next();
            Integer startNodeId = entry.getKey();
            OperatorChainInfo chainInfo = entry.getValue();
            if (!isReadyToCreateJobVertex(chainInfo)) {
                continue;
            }
            chainEntryPoints.put(startNodeId, chainInfo);
            for (Integer sourceNodeId : chainInfo.getChainedSources().keySet()) {
                StreamNode sourceNode = streamGraph.getStreamNode(sourceNodeId);
                StreamEdge sourceOutEdge = sourceNode.getOutEdges().get(0);
                operatorBuildContexts
                        .computeIfAbsent(sourceNodeId, key -> new OperatorBuildContext())
                        .setChainableOutEdges(Collections.singletonList(sourceOutEdge));
            }
            LOG.info(
                    "chainInfo with startNodeId {} has been removed from pending queue.",
                    startNodeId);
            iterator.remove();
        }
        return chainEntryPoints;
    }

    private boolean isReadyToChain(Integer startNodeId) {
        if (chainInfos.containsKey(startNodeId)) {
            return false;
        }
        StreamNode node = streamGraph.getStreamNode(startNodeId);
        for (StreamEdge edge : node.getInEdges()) {
            if (!hashes.containsKey(edge.getSourceId())) {
                return false;
            }
        }
        return true;
    }

    private List<StreamEdge> generateOperatorChainInfo(
            final Integer currentNodeId,
            final OperatorChainInfo chainInfo,
            final Map<Integer, OperatorChainInfo> chainEntryPoints,
            final Map<Integer, OperatorBuildContext> operatorBuildContexts) {
        Integer startNodeId = chainInfo.getStartNodeId();

        if (chainInfos.containsKey(startNodeId)) {
            return new ArrayList<>();
        }

        List<StreamEdge> transitiveOutEdges = new ArrayList<>();
        List<StreamEdge> chainableOutputs = new ArrayList<>();
        List<StreamEdge> nonChainableOutputs = new ArrayList<>();

        StreamNode currentNode = streamGraph.getStreamNode(currentNodeId);

        generateHashesByStreamNode(currentNode);

        boolean isOutputOnlyAfterEndOfStream = currentNode.isOutputOnlyAfterEndOfStream();

        if (isOutputOnlyAfterEndOfStream) {
            outputBlockingNodesID.add(currentNode.getId());
        }

        for (StreamEdge outEdge : currentNode.getOutEdges()) {
            if (isChainable(outEdge, streamGraph)) {
                chainableOutputs.add(outEdge);
            } else {
                nonChainableOutputs.add(outEdge);
            }
        }

        for (StreamEdge inEdge : currentNode.getInEdges()) {
            // inEdge exist in generated jobVertex
            if (frozenNodeToStartNodeMap.containsKey(inEdge.getSourceId())) {
                chainInfo.addTransitiveInEdge(inEdge);
            }
        }

        for (StreamEdge chainable : chainableOutputs) {
            // Mark downstream nodes in the same chain as outputBlocking
            if (isOutputOnlyAfterEndOfStream) {
                outputBlockingNodesID.add(chainable.getTargetId());
            }
            transitiveOutEdges.addAll(
                    generateOperatorChainInfo(
                            chainable.getTargetId(),
                            chainInfo,
                            chainEntryPoints,
                            operatorBuildContexts));
            // Mark upstream nodes in the same chain as outputBlocking
            if (outputBlockingNodesID.contains(chainable.getTargetId())) {
                outputBlockingNodesID.add(currentNodeId);
            }
        }

        for (StreamEdge nonChainable : nonChainableOutputs) {
            transitiveOutEdges.add(nonChainable);
            if (generateMode == GenerateMode.EAGERLY
                    && isReadyToChain(nonChainable.getTargetId())) {
                generateOperatorChainInfo(
                        nonChainable.getTargetId(),
                        chainEntryPoints.computeIfAbsent(
                                nonChainable.getTargetId(),
                                (k) -> chainInfo.newChain(nonChainable.getTargetId())),
                        chainEntryPoints,
                        operatorBuildContexts);
            }
        }

        OperatorBuildContext operatorBuildContext =
                operatorBuildContexts.computeIfAbsent(
                        currentNodeId, key -> new OperatorBuildContext());

        operatorBuildContext.setChainedName(
                createChainedName(
                        currentNodeId,
                        chainableOutputs,
                        Optional.ofNullable(chainEntryPoints.get(currentNodeId)),
                        streamGraph,
                        key -> operatorBuildContexts.get(key).getChainedName()));

        operatorBuildContext.setMinResource(
                createChainedResources(
                        currentNode.getMinResources(),
                        chainableOutputs,
                        key -> operatorBuildContexts.get(key).getMinResource()));

        operatorBuildContext.setPreferredResources(
                createChainedResources(
                        currentNode.getPreferredResources(),
                        chainableOutputs,
                        key -> operatorBuildContexts.get(key).getPreferredResources()));

        operatorBuildContext.setChainableOutEdges(chainableOutputs);

        operatorBuildContext.setNonChainableOutEdges(nonChainableOutputs);

        tryConvertPartitionerForDynamicGraph(chainableOutputs, nonChainableOutputs, streamGraph);

        OperatorID currentOperatorId =
                chainInfo.addNodeToChain(
                        currentNodeId,
                        streamGraph.getStreamNode(currentNodeId).getOperatorName(),
                        hashes,
                        legacyHashes);

        if (currentNode.getInputFormat() != null) {
            chainInfo
                    .getOrCreateFormatContainer()
                    .addInputFormat(currentOperatorId, currentNode.getInputFormat());
        }

        if (currentNode.getOutputFormat() != null) {
            chainInfo
                    .getOrCreateFormatContainer()
                    .addOutputFormat(currentOperatorId, currentNode.getOutputFormat());
        }

        if (currentNodeId.equals(startNodeId)) {
            chainInfo.setTransitiveOutEdges(transitiveOutEdges);
            chainInfos.put(startNodeId, chainInfo);
        }

        return transitiveOutEdges;
    }

    private void generateHashesByStreamNode(StreamNode streamNode) {
        // Generate deterministic hashes for the nodes in order to identify them across
        // submission if they didn't change.
        if (hashes.containsKey(streamNode.getId())) {
            LOG.info("Hash for node {} has already been generated.", streamNode);
            return;
        }
        legacyStreamGraphHasher.generateHashesByStreamNode(streamNode, streamGraph, legacyHashes);
        Preconditions.checkState(
                defaultStreamGraphHasher.generateHashesByStreamNode(
                        streamNode, streamGraph, hashes),
                "Failed to generate hash for streamNode with ID '%s'",
                streamNode.getId());
    }

    private boolean isReadyToCreateJobVertex(OperatorChainInfo chainInfo) {
        Integer startNodeId = chainInfo.getStartNodeId();
        if (chainInfos.containsKey(startNodeId)) {
            return false;
        }
        StreamNode startNode = streamGraph.getStreamNode(startNodeId);
        for (StreamEdge inEdges : startNode.getInEdges()) {
            Integer sourceNodeId = inEdges.getSourceId();
            if (!hashes.containsKey(sourceNodeId)) {
                return false;
            }
            if (chainInfo.getChainedSources().containsKey(sourceNodeId)
                    || generateMode == GenerateMode.EAGERLY) {
                continue;
            }
            Optional<JobVertexID> upstreamJobVertex = findVertexByStreamNodeId(sourceNodeId);
            if (!upstreamJobVertex.isPresent()
                    || !finishedJobVertices.contains(upstreamJobVertex.get())) {
                return false;
            }
        }
        return true;
    }

    public ResourceSpec createChainedResources(
            ResourceSpec orginResourceSpec,
            List<StreamEdge> chainedOutputs,
            Function<Integer, ResourceSpec> resourceSpecProvider) {
        for (StreamEdge chainable : chainedOutputs) {
            orginResourceSpec =
                    orginResourceSpec.merge(resourceSpecProvider.apply(chainable.getTargetId()));
        }
        return orginResourceSpec;
    }

    private boolean isSourceChainable(StreamNode sourceNode) {
        if (!sourceNode.getInEdges().isEmpty()
                || sourceNode.getOperatorFactory() == null
                || !(sourceNode.getOperatorFactory() instanceof SourceOperatorFactory)
                || sourceNode.getOutEdges().size() != 1) {
            return false;
        }
        final StreamEdge sourceOutEdge = sourceNode.getOutEdges().get(0);
        final StreamNode target = streamGraph.getStreamNode(sourceOutEdge.getTargetId());
        final ChainingStrategy targetChainingStrategy =
                Preconditions.checkNotNull(target.getOperatorFactory()).getChainingStrategy();
        return targetChainingStrategy == ChainingStrategy.HEAD_WITH_SOURCES
                && isChainableInput(sourceOutEdge, streamGraph);
    }

    public enum GenerateMode {
        LAZILY,
        EAGERLY
    }

    private static class OperatorBuildContext {
        private String chainedName;
        private ResourceSpec minResource;
        private ResourceSpec preferredResources;
        private StreamConfig streamConfig;
        private final List<StreamEdge> chainableOutEdges;
        private final List<StreamEdge> nonChainableOutEdges;

        public OperatorBuildContext() {
            chainableOutEdges = new ArrayList<>();
            nonChainableOutEdges = new ArrayList<>();
        }

        public String getChainedName() {
            return chainedName;
        }

        public void setChainedName(String chainedName) {
            this.chainedName = chainedName;
        }

        public ResourceSpec getMinResource() {
            return minResource;
        }

        public void setMinResource(ResourceSpec minResource) {
            this.minResource = minResource;
        }

        public ResourceSpec getPreferredResources() {
            return preferredResources;
        }

        public void setPreferredResources(ResourceSpec preferredResources) {
            this.preferredResources = preferredResources;
        }

        public List<StreamEdge> getChainableOutEdges() {
            return chainableOutEdges;
        }

        public void setChainableOutEdges(List<StreamEdge> chainableOutEdges) {
            this.chainableOutEdges.addAll(chainableOutEdges);
        }

        public List<StreamEdge> getNonChainableOutEdges() {
            return nonChainableOutEdges;
        }

        public void setNonChainableOutEdges(List<StreamEdge> NonChainableOutEdges) {
            this.nonChainableOutEdges.addAll(NonChainableOutEdges);
        }

        public StreamConfig getStreamConfig() {
            return streamConfig;
        }

        public void setStreamConfig(StreamConfig streamConfig) {
            this.streamConfig = streamConfig;
        }
    }
}
