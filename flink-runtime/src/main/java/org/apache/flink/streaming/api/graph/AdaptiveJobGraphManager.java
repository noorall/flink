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
import org.apache.flink.api.common.operators.ResourceSpec;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.OperatorIDPair;
import org.apache.flink.runtime.jobgraph.InputOutputFormatContainer;
import org.apache.flink.runtime.jobgraph.InputOutputFormatVertex;
import org.apache.flink.runtime.jobgraph.IntermediateDataSet;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.operators.coordination.OperatorCoordinator;
import org.apache.flink.runtime.operators.util.TaskConfig;
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
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import static org.apache.flink.streaming.api.graph.StreamingJobGraphGenerator.addVertexIndexPrefixInVertexName;
import static org.apache.flink.streaming.api.graph.StreamingJobGraphGenerator.connect;
import static org.apache.flink.streaming.api.graph.StreamingJobGraphGenerator.createChainedMinResources;
import static org.apache.flink.streaming.api.graph.StreamingJobGraphGenerator.createChainedName;
import static org.apache.flink.streaming.api.graph.StreamingJobGraphGenerator.createChainedPreferredResources;
import static org.apache.flink.streaming.api.graph.StreamingJobGraphGenerator.getOrCreateFormatContainer;
import static org.apache.flink.streaming.api.graph.StreamingJobGraphGenerator.isChainable;
import static org.apache.flink.streaming.api.graph.StreamingJobGraphGenerator.markSupportingConcurrentExecutionAttempts;
import static org.apache.flink.streaming.api.graph.StreamingJobGraphGenerator.preValidate;
import static org.apache.flink.streaming.api.graph.StreamingJobGraphGenerator.setAllOperatorNonChainedOutputsConfigs;
import static org.apache.flink.streaming.api.graph.StreamingJobGraphGenerator.setManagedMemoryFraction;
import static org.apache.flink.streaming.api.graph.StreamingJobGraphGenerator.setOperatorChainedOutputsConfig;
import static org.apache.flink.streaming.api.graph.StreamingJobGraphGenerator.setOperatorConfig;
import static org.apache.flink.streaming.api.graph.StreamingJobGraphGenerator.setPhysicalEdges;
import static org.apache.flink.streaming.api.graph.StreamingJobGraphGenerator.setSlotSharingAndCoLocation;
import static org.apache.flink.streaming.api.graph.StreamingJobGraphGenerator.setVertexDescription;
import static org.apache.flink.streaming.api.graph.StreamingJobGraphGenerator.setVertexParallelismsForDynamicGraphIfNecessary;
import static org.apache.flink.streaming.api.graph.StreamingJobGraphGenerator.tryConvertPartitionerForDynamicGraph;
import static org.apache.flink.streaming.api.graph.StreamingJobGraphGenerator.validateHybridShuffleExecuteInBatchMode;

public class AdaptiveJobGraphManager implements AdaptiveJobGraphGenerator, JobVertexMapper {
    private static final Logger LOG = LoggerFactory.getLogger(AdaptiveJobGraphManager.class);

    private final StreamGraph streamGraph;

    private final Map<Integer, JobVertex> jobVertices;
    private final JobGraph jobGraph;

    private final Map<Integer, Map<Integer, StreamConfig>> chainedConfigs;

    private final Map<Integer, StreamConfig> vertexConfigs;
    private final Map<Integer, String> chainedNames;

    private final Map<Integer, ResourceSpec> chainedMinResources;
    private final Map<Integer, ResourceSpec> chainedPreferredResources;

    private final Map<Integer, InputOutputFormatContainer> chainedInputOutputFormats;

    // the ids of nodes whose output result partition type should be set to BLOCKING
    private final Set<Integer> outputBlockingNodesID;

    private final StreamGraphHasher defaultStreamGraphHasher;
    private final StreamGraphHasher legacyStreamGraphHasher;

    private AtomicBoolean hasHybridResultPartition;

    private final Executor serializationExecutor;

    // Futures for the serialization of operator coordinators
    private final Map<
                    JobVertexID,
                    List<CompletableFuture<SerializedValue<OperatorCoordinator.Provider>>>>
            coordinatorSerializationFuturesPerJobVertex = new HashMap<>();

    /** The {@link OperatorChainInfo}s, key is the start node id of the chain. */
    private final Map<Integer, OperatorChainInfo> chainInfos;

    /**
     * This is used to cache the non-chainable outputs, to set the non-chainable outputs config
     * after all job vertices are created.
     */
    private final Map<Integer, Map<Integer, List<StreamEdge>>> opChainableOutputsCaches;

    private final Map<Integer, Integer> frozenNodeToStartNodeMap;

    private final Map<JobVertexID, Integer> jobVertexToStartNodeMap;

    private final Map<Integer, byte[]> hashes;

    private final Map<Integer, byte[]> legacyHashes;

    // sourceNode -> <targetNodeId, NonChainedOutput>
    // need update when edge changed
    private final Map<Integer, Map<StreamEdge, NonChainedOutput>> opIntermediateOutputsCaches;

    private final GenerateMode generateMode;

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

        this.jobVertices = new LinkedHashMap<>();
        this.chainedConfigs = new HashMap<>();
        this.vertexConfigs = new HashMap<>();
        this.chainedNames = new HashMap<>();
        this.chainedMinResources = new HashMap<>();
        this.chainedPreferredResources = new HashMap<>();
        this.chainedInputOutputFormats = new HashMap<>();
        this.outputBlockingNodesID = new HashSet<>();
        this.serializationExecutor = Preconditions.checkNotNull(serializationExecutor);
        this.chainInfos = new HashMap<>();
        this.opChainableOutputsCaches = new HashMap<>();
        this.frozenNodeToStartNodeMap = new HashMap<>();
        this.hashes = new HashMap<>();
        this.legacyHashes = new HashMap<>();
        this.generateMode = generateMode;
        this.hasHybridResultPartition = new AtomicBoolean(false);
        this.opIntermediateOutputsCaches = new HashMap<>();
        this.jobVertexToStartNodeMap = new HashMap<>();

        this.jobGraph = new JobGraph(streamGraph.getJobId(), streamGraph.getJobName());
        streamGraph.getUserJarBlobKeys().forEach(jobGraph::addUserJarBlobKey);

        this.jobGraph.setClasspaths(streamGraph.getClasspaths());
        this.jobGraph.setSnapshotSettings(streamGraph.getJobCheckpointingSettings());
        this.jobGraph.setSavepointRestoreSettings(streamGraph.getSavepointRestoreSettings());
        this.jobGraph.setJobType(streamGraph.getJobType());
        this.jobGraph.setDynamic(streamGraph.isDynamic());

        this.jobGraph.enableApproximateLocalRecovery(
                streamGraph.getCheckpointConfig().isApproximateLocalRecoveryEnabled());
    }

    @Override
    public boolean isStreamGraphConversionFinished() {
        return streamGraph.getStreamNodes().size() == frozenNodeToStartNodeMap.size();
    }

    public List<JobVertex> initializeJobGraph() {
        List<StreamNode> sourceNodes =
                streamGraph.getStreamNodes().stream()
                        .filter(node -> node.getInEdges().isEmpty())
                        .collect(Collectors.toList());

        return createJobVerticesAndUpdateGraph(sourceNodes);
    }

    @Override
    public JobGraph getJobGraph() {
        return this.jobGraph;
    }

    @Override
    @VisibleForTesting
    public List<JobVertex> createJobVerticesAndUpdateGraph(List<StreamNode> streamNodes) {
        Map<Integer, List<StreamEdge>> nonChainableOutputsCache = new LinkedHashMap<>();
        Map<Integer, List<StreamEdge>> nonChainedInputsCache = new LinkedHashMap<>();
        List<StreamNode> validatedStreamNodes = validateStreamNodes(streamNodes);

        Map<Integer, OperatorChainInfo> chainInfos =
                createOperatorChainInfos(
                        validatedStreamNodes, nonChainableOutputsCache, nonChainedInputsCache);

        Map<Integer, JobVertex> createdJobVertices = createJobVerticesByChainInfos(chainInfos);

        generateConfigForJobVertices(
                createdJobVertices, chainInfos, nonChainableOutputsCache, nonChainedInputsCache);

        return new ArrayList<>(createdJobVertices.values());
    }

    @Override
    public boolean updateStreamGraph(StreamGraphUpdateRequestInfo requestInfo) {
        if (requestInfo instanceof ModifyStreamEdgeRequestInfo) {
            return changeStreamEdgePartitioner((ModifyStreamEdgeRequestInfo) requestInfo);
        } else {
            return false;
        }
    }

    @Override
    public Optional<JobVertexID> findVertexByStreamNodeId(int streamNodeId) {
        if (frozenNodeToStartNodeMap.containsKey(streamNodeId)) {
            Integer startNodeId = frozenNodeToStartNodeMap.get(streamNodeId);
            return Optional.of(jobVertices.get(startNodeId).getID());
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

    private boolean changeStreamEdgePartitioner(
            ModifyStreamEdgeRequestInfo modifyStreamEdgeRequestInfo) {
        StreamEdge streamEdge = modifyStreamEdgeRequestInfo.getStreamEdge();
        StreamPartitioner<?> newPartitioner = modifyStreamEdgeRequestInfo.getOutputPartitioner();
        if (frozenNodeToStartNodeMap.containsKey(streamEdge.getTargetId())) {
            return false;
        }
        streamEdge.setPartitioner(newPartitioner);
        Integer sourceNodeId = streamEdge.getSourceId();
        Integer startNodeId = frozenNodeToStartNodeMap.get(sourceNodeId);
        NonChainedOutput output =
                (startNodeId != null)
                        ? opIntermediateOutputsCaches.get(startNodeId).get(streamEdge)
                        : null;
        if (output != null) {
            output.setPartitioner(newPartitioner);
        }
        return true;
    }

    private List<StreamNode> validateStreamNodes(List<StreamNode> streamNodes) {
        List<StreamNode> validatedStreamNodes = new ArrayList<>();
        for (StreamNode streamNode : streamNodes) {
            Integer startNodeId = streamNode.getId();
            if (frozenNodeToStartNodeMap.containsKey(startNodeId)) {
                continue;
            }
            if (!isReadyToChain(startNodeId)) {
                continue;
            }
            validatedStreamNodes.add(streamNode);
        }
        return validatedStreamNodes;
    }

    private void generateConfigForJobVertices(
            Map<Integer, JobVertex> jobVertices,
            Map<Integer, OperatorChainInfo> chainInfos,
            Map<Integer, List<StreamEdge>> nonChainableOutputsCache,
            Map<Integer, List<StreamEdge>> nonChainedInputsCache) {
        if (jobGraph.isDynamic()) {
            setVertexParallelismsForDynamicGraphIfNecessary(jobVertices, chainInfos, streamGraph);
        }
        chainInfos.values().forEach(this::initStreamConfigs);
        generateAllOutputConfigs(jobVertices, nonChainableOutputsCache, nonChainedInputsCache);
        finalizeConfig(jobVertices);
    }

    private void initStreamConfigs(OperatorChainInfo chainInfo) {
        Integer startNodeId = chainInfo.getStartNodeId();
        List<StreamNode> chainedNodes = chainInfo.getAllChainedNodes();
        for (int i = 0; i < chainedNodes.size(); i++) {
            StreamNode currentNode = chainedNodes.get(i);
            if (opChainableOutputsCaches.get(startNodeId).get(currentNode.getId()) == null) {
                continue;
            }
            StreamConfig config =
                    new StreamConfig(
                            currentNode.getId() == startNodeId
                                    ? jobVertices.get(currentNode.getId()).getConfiguration()
                                    : new Configuration());
            if (currentNode.getId() == startNodeId) {
                config.setChainStart();
                config.setTransitiveChainedTaskConfigs(chainedConfigs.get(startNodeId));
            } else {
                chainedConfigs.computeIfAbsent(startNodeId, k -> new HashMap<>());
                chainedConfigs.get(startNodeId).put(currentNode.getId(), config);
            }
            config.setChainIndex(i);
            config.setOperatorName(currentNode.getOperatorName());
            config.setOperatorID(new OperatorID(hashes.get(currentNode.getId())));
            setOperatorConfig(
                    currentNode.getId(),
                    config,
                    chainInfo.getChainedSources(),
                    streamGraph,
                    chainedConfigs);
            vertexConfigs.put(currentNode.getId(), config);
            setOperatorChainedOutputsConfig(
                    config,
                    opChainableOutputsCaches.get(startNodeId).get(currentNode.getId()),
                    streamGraph);
            if (opChainableOutputsCaches.get(startNodeId).get(currentNode.getId()).isEmpty()) {
                config.setChainEnd();
            }
        }
    }

    private void generateAllOutputConfigs(
            Map<Integer, JobVertex> jobVertices,
            Map<Integer, List<StreamEdge>> nonChainableOutputsCache,
            Map<Integer, List<StreamEdge>> nonChainedInputsCache) {
        setAllOperatorNonChainedOutputsConfigs(
                opIntermediateOutputsCaches,
                nonChainableOutputsCache,
                vertexConfigs,
                streamGraph,
                outputBlockingNodesID,
                hasHybridResultPartition);
        List<StreamEdge> physicalEdgesInOrder = new ArrayList<>();
        setAllVerticesNonChainedOutputsConfigs(jobVertices, physicalEdgesInOrder);
        connectNonChainedInput(nonChainedInputsCache, physicalEdgesInOrder);
        setPhysicalEdges(physicalEdgesInOrder, vertexConfigs);
    }

    private void finalizeConfig(Map<Integer, JobVertex> jobVertices) {
        markSupportingConcurrentExecutionAttempts(jobVertices, chainedConfigs, streamGraph);

        validateHybridShuffleExecuteInBatchMode(hasHybridResultPartition, jobGraph);

        setSlotSharingAndCoLocation(jobVertices, hasHybridResultPartition, streamGraph, jobGraph);

        setManagedMemoryFraction(
                Collections.unmodifiableMap(jobVertices),
                Collections.unmodifiableMap(vertexConfigs),
                Collections.unmodifiableMap(chainedConfigs),
                id -> streamGraph.getStreamNode(id).getManagedMemoryOperatorScopeUseCaseWeights(),
                id -> streamGraph.getStreamNode(id).getManagedMemorySlotScopeUseCases());

        // TODO： generate name via JobVertex rather than through JobGraph.
        addVertexIndexPrefixInVertexName(streamGraph, jobGraph);
        setVertexDescription(jobVertices, streamGraph, chainedConfigs);
        try {
            FutureUtils.combineAll(
                            vertexConfigs.values().stream()
                                    .map(
                                            config ->
                                                    config.triggerSerializationAndReturnFuture(
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

    private void setAllVerticesNonChainedOutputsConfigs(
            Map<Integer, JobVertex> jobVertices, List<StreamEdge> physicalEdgesInOrder) {
        for (Integer startNodeId : jobVertices.keySet()) {
            setVertexNonChainedOutputsConfig(startNodeId, physicalEdgesInOrder);
        }
    }

    private void setVertexNonChainedOutputsConfig(
            Integer startNodeId, List<StreamEdge> physicalEdgesInOrder) {
        StreamConfig config = vertexConfigs.get(startNodeId);
        List<StreamEdge> transitiveOutEdges = chainInfos.get(startNodeId).getTransitiveOutEdges();
        LinkedHashSet<NonChainedOutput> transitiveOutputs = new LinkedHashSet<>();
        for (StreamEdge edge : transitiveOutEdges) {
            NonChainedOutput output = opIntermediateOutputsCaches.get(edge.getSourceId()).get(edge);
            transitiveOutputs.add(output);
            if (jobVertices.containsKey(edge.getTargetId())) {
                connect(startNodeId, edge, output, physicalEdgesInOrder, jobVertices);
            } else {
                JobVertex jobVertex = jobVertices.get(startNodeId);
                IntermediateDataSet dataSet =
                        jobVertex.getOrCreateResultDataSet(
                                output.getDataSetId(), output.getPartitionType());
                dataSet.addStreamEdge(edge);
            }
        }
        config.setVertexNonChainedOutputs(new ArrayList<>(transitiveOutputs));
    }

    private void connectNonChainedInput(
            Map<Integer, List<StreamEdge>> nonChainedInputsCache,
            List<StreamEdge> physicalEdgesInOrder) {
        for (List<StreamEdge> streamEdges : nonChainedInputsCache.values()) {
            for (StreamEdge edge : streamEdges) {
                NonChainedOutput output =
                        opIntermediateOutputsCaches.get(edge.getSourceId()).get(edge);
                Integer sourceStartNodeId = frozenNodeToStartNodeMap.get(edge.getSourceId());
                connect(sourceStartNodeId, edge, output, physicalEdgesInOrder, jobVertices);
            }
        }
    }

    private Map<Integer, JobVertex> createJobVerticesByChainInfos(
            Map<Integer, OperatorChainInfo> chainInfos) {
        Map<Integer, JobVertex> createdJobVertices = new HashMap<>();
        for (OperatorChainInfo chainInfo : chainInfos.values()) {
            JobVertex jobVertex = createJobVertex(chainInfo.getStartNodeId(), chainInfo);
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

    private JobVertex createJobVertex(Integer streamNodeId, OperatorChainInfo chainInfo) {

        JobVertex jobVertex;
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

        if (chainedInputOutputFormats.containsKey(streamNodeId)) {
            jobVertex =
                    new InputOutputFormatVertex(
                            chainedNames.get(streamNodeId), jobVertexId, operatorIDPairs);

            chainedInputOutputFormats
                    .get(streamNodeId)
                    .write(new TaskConfig(jobVertex.getConfiguration()));
        } else {
            jobVertex = new JobVertex(chainedNames.get(streamNodeId), jobVertexId, operatorIDPairs);
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
                                                    chainedNames.get(streamNodeId)),
                                            e);
                                }
                            },
                            serializationExecutor));
        }
        if (!serializationFutures.isEmpty()) {
            coordinatorSerializationFuturesPerJobVertex.put(jobVertexId, serializationFutures);
        }

        jobVertex.setResources(
                chainedMinResources.get(streamNodeId), chainedPreferredResources.get(streamNodeId));

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

        jobVertices.put(streamNodeId, jobVertex);
        jobGraph.addVertex(jobVertex);

        jobVertex.setParallelismConfigured(
                chainInfo.getAllChainedNodes().stream()
                        .anyMatch(StreamNode::isParallelismConfigured));

        return jobVertex;
    }

    private Map<Integer, OperatorChainInfo> createOperatorChainInfos(
            List<StreamNode> streamNodes,
            Map<Integer, List<StreamEdge>> nonChainableOutputsCache,
            Map<Integer, List<StreamEdge>> nonChainableInputsCache) {
        final Map<Integer, OperatorChainInfo> chainEntryPoints =
                buildChainedInputsAndGetHeadInputs(streamNodes);
        final List<OperatorChainInfo> chainInfos =
                chainEntryPoints.entrySet().stream()
                        .sorted(Map.Entry.comparingByKey())
                        .map(Map.Entry::getValue)
                        .collect(Collectors.toList());
        for (OperatorChainInfo info : chainInfos) {
            generateOperatorChainInfo(
                    info.getStartNodeId(),
                    info,
                    chainEntryPoints,
                    nonChainableOutputsCache,
                    nonChainableInputsCache);
        }
        return chainEntryPoints;
    }

    private Map<Integer, OperatorChainInfo> buildChainedInputsAndGetHeadInputs(
            List<StreamNode> streamNodes) {
        final Map<Integer, OperatorChainInfo> chainEntryPoints = new HashMap<>();
        for (StreamNode streamNode : streamNodes) {
            // TODO: process source chain
            int sourceNodeId = streamNode.getId();
            // avoid a node appearing twice
            // Vertex
            //  |  \
            // Edge1 Edge2
            //   \ /
            //   Node
            if (isReadyToChain(sourceNodeId) && !chainEntryPoints.containsKey(sourceNodeId)) {
                chainEntryPoints.put(
                        sourceNodeId, new OperatorChainInfo(sourceNodeId, streamGraph));
            }
        }
        return chainEntryPoints;
    }

    /**
     * A node can be considered as a head node if it meets the following conditions:
     *
     * <p>1. The hash for all sourceNode instances has been generated. <br>
     * 2. The node must not exist in any other JobVertex.
     */
    private boolean isReadyToChain(Integer headNodeId) {
        if (chainInfos.containsKey(headNodeId)) {
            return false;
        }
        StreamNode node = streamGraph.getStreamNode(headNodeId);
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
            final Map<Integer, List<StreamEdge>> nonChainableOutputsCache,
            final Map<Integer, List<StreamEdge>> nonChainableInputsCache) {
        Integer startNodeId = chainInfo.getStartNodeId();
        if (!chainInfos.containsKey(startNodeId)) {
            List<StreamEdge> transitiveOutEdges = new ArrayList<>();
            List<StreamEdge> chainableOutputs = new ArrayList<>();
            List<StreamEdge> nonChainableOutputs = new ArrayList<>();

            StreamNode currentNode = streamGraph.getStreamNode(currentNodeId);

            Preconditions.checkState(
                    generateHashesByStreamNode(currentNode),
                    "Failed to generate hash for streamNode with ID '%s'",
                    currentNode.getId());

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
                    nonChainableInputsCache
                            .computeIfAbsent(currentNodeId, ignored -> new ArrayList<>())
                            .add(inEdge);
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
                                nonChainableOutputsCache,
                                nonChainableInputsCache));
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
                            nonChainableOutputsCache,
                            nonChainableInputsCache);
                }
            }

            chainedNames.put(
                    currentNodeId,
                    createChainedName(
                            currentNodeId,
                            chainableOutputs,
                            Optional.ofNullable(chainEntryPoints.get(currentNodeId)),
                            streamGraph,
                            chainedNames));
            chainedMinResources.put(
                    currentNodeId,
                    createChainedMinResources(
                            currentNodeId, chainableOutputs, streamGraph, chainedMinResources));
            chainedPreferredResources.put(
                    currentNodeId,
                    createChainedPreferredResources(
                            currentNodeId,
                            chainableOutputs,
                            streamGraph,
                            chainedPreferredResources));

            OperatorID currentOperatorId =
                    chainInfo.addNodeToChain(
                            currentNodeId,
                            streamGraph.getStreamNode(currentNodeId).getOperatorName(),
                            hashes,
                            legacyHashes);

            if (currentNode.getInputFormat() != null) {
                getOrCreateFormatContainer(startNodeId, chainedInputOutputFormats)
                        .addInputFormat(currentOperatorId, currentNode.getInputFormat());
            }

            if (currentNode.getOutputFormat() != null) {
                getOrCreateFormatContainer(startNodeId, chainedInputOutputFormats)
                        .addOutputFormat(currentOperatorId, currentNode.getOutputFormat());
            }

            tryConvertPartitionerForDynamicGraph(
                    chainableOutputs, nonChainableOutputs, streamGraph);

            // we cache the outputs here, and set the config later
            nonChainableOutputsCache.put(currentNodeId, nonChainableOutputs);

            opChainableOutputsCaches
                    .computeIfAbsent(startNodeId, k -> new LinkedHashMap<>())
                    .put(currentNodeId, chainableOutputs);

            if (currentNodeId.equals(startNodeId)) {
                chainInfo.setTransitiveOutEdges(transitiveOutEdges);
                chainInfos.put(startNodeId, chainInfo);
            }

            return transitiveOutEdges;

        } else {
            return new ArrayList<>();
        }
    }

    // TODO: remove this function soon
    private boolean generateHashesByChainInfo(OperatorChainInfo chainInfo) {
        // Generate deterministic hashes for the nodes in order to identify them across
        // submission if they didn't change.
        legacyStreamGraphHasher.generateHashesByStreamNodes(
                chainInfo.getAllChainedNodes(), streamGraph, legacyHashes);
        return defaultStreamGraphHasher.generateHashesByStreamNodes(
                chainInfo.getAllChainedNodes(), streamGraph, hashes);
    }

    private boolean generateHashesByStreamNode(StreamNode streamNode) {
        // Generate deterministic hashes for the nodes in order to identify them across
        // submission if they didn't change.
        legacyStreamGraphHasher.generateHashesByStreamNode(streamNode, streamGraph, legacyHashes);
        return defaultStreamGraphHasher.generateHashesByStreamNode(streamNode, streamGraph, hashes);
    }

    public enum GenerateMode {
        LAZILY,
        EAGERLY
    }
}
