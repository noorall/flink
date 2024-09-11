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
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.IllegalConfigurationException;
import org.apache.flink.runtime.OperatorIDPair;
import org.apache.flink.runtime.jobgraph.InputOutputFormatVertex;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.jobgraph.forwardgroup.StreamNodeForwardGroup;
import org.apache.flink.runtime.operators.coordination.OperatorCoordinator;
import org.apache.flink.runtime.operators.util.TaskConfig;
import org.apache.flink.streaming.api.graph.util.ChainedSourceInfo;
import org.apache.flink.streaming.api.graph.util.JobVertexBuildContext;
import org.apache.flink.streaming.api.graph.util.OperatorChainInfo;
import org.apache.flink.streaming.api.graph.util.OperatorInfo;
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
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static org.apache.flink.streaming.api.graph.StreamingJobGraphGenerator.addVertexIndexPrefixInVertexName;
import static org.apache.flink.streaming.api.graph.StreamingJobGraphGenerator.connect;
import static org.apache.flink.streaming.api.graph.StreamingJobGraphGenerator.createChainedMinResources;
import static org.apache.flink.streaming.api.graph.StreamingJobGraphGenerator.createChainedName;
import static org.apache.flink.streaming.api.graph.StreamingJobGraphGenerator.createChainedPreferredResources;
import static org.apache.flink.streaming.api.graph.StreamingJobGraphGenerator.isChainable;
import static org.apache.flink.streaming.api.graph.StreamingJobGraphGenerator.isChainableInput;
import static org.apache.flink.streaming.api.graph.StreamingJobGraphGenerator.markSupportingConcurrentExecutionAttempts;
import static org.apache.flink.streaming.api.graph.StreamingJobGraphGenerator.preValidate;
import static org.apache.flink.streaming.api.graph.StreamingJobGraphGenerator.setAllOperatorNonChainedOutputsConfigs;
import static org.apache.flink.streaming.api.graph.StreamingJobGraphGenerator.setManagedMemoryFraction;
import static org.apache.flink.streaming.api.graph.StreamingJobGraphGenerator.setOperatorChainedOutputsConfig;
import static org.apache.flink.streaming.api.graph.StreamingJobGraphGenerator.setOperatorConfig;
import static org.apache.flink.streaming.api.graph.StreamingJobGraphGenerator.setPhysicalEdges;
import static org.apache.flink.streaming.api.graph.StreamingJobGraphGenerator.setSlotSharingAndCoLocation;
import static org.apache.flink.streaming.api.graph.StreamingJobGraphGenerator.setVertexDescription;
import static org.apache.flink.streaming.api.graph.StreamingJobGraphGenerator.tryConvertPartitionerForDynamicGraph;
import static org.apache.flink.streaming.api.graph.StreamingJobGraphGenerator.validateHybridShuffleExecuteInBatchMode;

public class AdaptiveGraphManager implements AdaptiveGraphGenerator {

    // TODO: 所有以startNode为key的变量全部放到ChainInfo里去

    private static final Logger LOG = LoggerFactory.getLogger(AdaptiveGraphManager.class);

    private final StreamGraph streamGraph;

    private final JobGraph jobGraph;

    private final StreamGraphHasher defaultStreamGraphHasher;

    private final StreamGraphHasher legacyStreamGraphHasher;

    private final Executor serializationExecutor;

    private final GenerateMode generateMode;

    private final AtomicInteger vertexIndexId;

    // Futures for the serialization of operator coordinators
    private final Map<
                    JobVertexID,
                    List<CompletableFuture<SerializedValue<OperatorCoordinator.Provider>>>>
            coordinatorSerializationFuturesPerJobVertex = new HashMap<>();

    private final Map<Integer, Integer> frozenNodeToStartNodeMap;

    private final Map<JobVertexID, Integer> jobVertexToStartNodeMap;

    private final Map<Integer, byte[]> hashes;

    private final Map<Integer, byte[]> legacyHashes;

    // When the downstream vertex is not created, we need to cache the out
    private final Map<Integer, Map<StreamEdge, NonChainedOutput>> opIntermediateOutputsCaches;

    private final Map<Integer, StreamNodeForwardGroup> forwardGroupsByEndpointNodeIdCache;

    private final Map<Integer, JobVertex> jobVertices;

    /** TODO: remove chainInfos when support adaptive scheduler */
    private final Map<Integer, OperatorChainInfo> chainInfos;

    private final Map<Integer, OperatorChainInfo> pendingSourceChainInfos;

    private final Set<JobVertexID> finishedJobVertices;

    @VisibleForTesting
    public AdaptiveGraphManager(
            ClassLoader userClassloader,
            StreamGraph streamGraph,
            Executor serializationExecutor,
            GenerateMode generateMode) {
        preValidate(streamGraph, userClassloader);
        this.streamGraph = streamGraph;
        this.serializationExecutor = Preconditions.checkNotNull(serializationExecutor);
        this.generateMode = generateMode;

        this.defaultStreamGraphHasher = new StreamGraphHasherV2();
        this.legacyStreamGraphHasher = new StreamGraphUserHashHasher();

        this.jobVertices = new LinkedHashMap<>();
        this.pendingSourceChainInfos = new TreeMap<>();

        this.chainInfos = new HashMap<>();
        this.frozenNodeToStartNodeMap = new HashMap<>();
        this.hashes = new HashMap<>();
        this.legacyHashes = new HashMap<>();
        this.opIntermediateOutputsCaches = new HashMap<>();
        this.jobVertexToStartNodeMap = new HashMap<>();
        this.forwardGroupsByEndpointNodeIdCache = new HashMap<>();
        this.vertexIndexId = new AtomicInteger(0);
        this.finishedJobVertices = new HashSet<>();

        // TODO: When supporting streamGraph submission, modify this section.
        //        this.jobGraph = new JobGraph(streamGraph.getJobId(), streamGraph.getJobName());
        //        this.jobGraph.setClasspaths(streamGraph.getClasspaths());
        //        this.jobGraph.setSnapshotSettings(streamGraph.getJobCheckpointingSettings());
        //        streamGraph.getUserJarBlobKeys().forEach(jobGraph::addUserJarBlobKey);
        this.jobGraph = new JobGraph(streamGraph.getJobName());
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
        initializeJobGraph();
    }

    @Override
    public JobGraph getJobGraph() {
        return this.jobGraph;
    }

    @Override
    public StreamGraphContext getStreamGraphContext() {
        return null;
    }

    @Override
    public List<JobVertex> onJobVertexFinished(JobVertexID finishedJobVertexId) {
        this.finishedJobVertices.add(finishedJobVertexId);
        if (generateMode == GenerateMode.EAGERLY) {
            return Collections.emptyList();
        }
        List<StreamNode> streamNodes = new ArrayList<>();
        for (StreamEdge outEdge : findOutputEdgesByVertexId(finishedJobVertexId)) {
            streamNodes.add(streamGraph.getStreamNode(outEdge.getTargetId()));
        }
        return createJobVerticesAndUpdateGraph(streamNodes);
    }

    private Optional<JobVertexID> findVertexByStreamNodeId(int streamNodeId) {
        if (frozenNodeToStartNodeMap.containsKey(streamNodeId)) {
            Integer startNodeId = frozenNodeToStartNodeMap.get(streamNodeId);
            return Optional.of(jobVertices.get(startNodeId).getID());
        }
        return Optional.empty();
    }

    private List<StreamEdge> findOutputEdgesByVertexId(JobVertexID jobVertexId) {
        if (!jobVertexToStartNodeMap.containsKey(jobVertexId)) {
            return Collections.emptyList();
        }
        Integer startNodeId = jobVertexToStartNodeMap.get(jobVertexId);
        // TODO: modify this part when support adaptive scheduler
        return chainInfos.get(startNodeId).getTransitiveOutEdges();
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
        final JobVertexBuildContext jobVertexBuildContext = new JobVertexBuildContext(streamGraph);

        createOperatorChainInfos(streamNodes, jobVertexBuildContext);

        createJobVerticesByChainInfos(jobVertexBuildContext);

        generateConfigForJobVertices(jobVertexBuildContext);

        return new ArrayList<>(jobVertexBuildContext.getJobVertices().values());
    }

    private void generateConfigForJobVertices(JobVertexBuildContext jobVertexBuildContext) {
        jobVertexBuildContext
                .getChainInfos()
                .values()
                .forEach(chainInfo -> initStreamConfigs(chainInfo, jobVertexBuildContext));
        generateAllOutputConfigs(jobVertexBuildContext);
        finalizeConfig(jobVertexBuildContext);
    }

    private void initStreamConfigs(
            OperatorChainInfo chainInfo, JobVertexBuildContext jobVertexBuildContext) {
        int startNodeId = chainInfo.getStartNodeId();
        List<StreamNode> chainedNodes = chainInfo.getAllChainedNodes();
        OperatorInfo startOperatorInfo = jobVertexBuildContext.getOperatorInfo(startNodeId);

        for (int i = 0; i < chainedNodes.size(); i++) {
            int currentNodeId = chainedNodes.get(i).getId();
            OperatorInfo currentOperatorInfo = jobVertexBuildContext.getOperatorInfo(currentNodeId);
            StreamConfig config;

            if (chainInfo.getChainedSources().containsKey(currentNodeId)) {
                config = chainInfo.getChainedSources().get(currentNodeId).getOperatorConfig();
            } else if (currentNodeId == startNodeId) {
                config =
                        new StreamConfig(
                                jobVertexBuildContext
                                        .getJobVertex(currentNodeId)
                                        .getConfiguration());
            } else {
                config = new StreamConfig(new Configuration());
            }

            setOperatorConfig(
                    currentNodeId, config, chainInfo.getChainedSources(), jobVertexBuildContext);
            setOperatorChainedOutputsConfig(
                    config, currentOperatorInfo.getChainableOutputs(), jobVertexBuildContext);

            config.setChainIndex(i);
            config.setOperatorName(chainedNodes.get(currentNodeId).getOperatorName());

            if (currentNodeId == startNodeId) {
                config.setChainStart();
                config.setTransitiveChainedTaskConfigs(startOperatorInfo.getChainedConfigs());
            } else {
                startOperatorInfo.addChainedConfig(currentNodeId, config);
            }

            config.setOperatorID(new OperatorID(hashes.get(currentNodeId)));

            if (currentOperatorInfo.getChainableOutputs().isEmpty()) {
                config.setChainEnd();
            }
        }
    }

    private void generateAllOutputConfigs(JobVertexBuildContext jobVertexBuildContext) {
        Map<Integer, OperatorChainInfo> chainInfos = jobVertexBuildContext.getChainInfos();
        // this may be used by uncreated down stream vertex
        final Map<Integer, Map<StreamEdge, NonChainedOutput>> opIntermediateOutputs =
                new HashMap<>();

        setAllOperatorNonChainedOutputsConfigs(opIntermediateOutputs, jobVertexBuildContext);

        setAllVertexNonChainedOutputsConfigs(opIntermediateOutputs, jobVertexBuildContext);

        connectNonChainedInput(chainInfos, jobVertexBuildContext.getPhysicalEdgesInOrder());

        // The order of physicalEdges should be consistent with the order in which JobEdge was
        // created
        setPhysicalEdges(jobVertexBuildContext);
    }

    private void setAllVertexNonChainedOutputsConfigs(
            final Map<Integer, Map<StreamEdge, NonChainedOutput>> opIntermediateOutputs,
            JobVertexBuildContext jobVertexBuildContext) {
        jobVertexBuildContext
                .getJobVertices()
                .keySet()
                .forEach(
                        startNodeId ->
                                setVertexNonChainedOutputsConfig(
                                        startNodeId, opIntermediateOutputs, jobVertexBuildContext));
    }

    private void finalizeConfig(JobVertexBuildContext jobVertexBuildContext) {
        markSupportingConcurrentExecutionAttempts(jobVertexBuildContext);

        validateHybridShuffleExecuteInBatchMode(jobVertexBuildContext);

        // When generating in a single step, there may be differences between the results and the
        // full image generation. We consider this difference to be normal because they do not need
        // to be in the same shared group
        setSlotSharingAndCoLocation(jobGraph, jobVertexBuildContext);

        setManagedMemoryFraction(
                jobVertexBuildContext.getJobVertices(),
                id -> jobVertexBuildContext.getOrCreateOperatorInfo(id).getVertexConfig(),
                id -> jobVertexBuildContext.getOrCreateOperatorInfo(id).getChainedConfigs(),
                id -> streamGraph.getStreamNode(id).getManagedMemoryOperatorScopeUseCaseWeights(),
                id -> streamGraph.getStreamNode(id).getManagedMemorySlotScopeUseCases());

        addVertexIndexPrefixInVertexName(jobVertexBuildContext, vertexIndexId, jobGraph);

        setVertexDescription(jobVertexBuildContext);

        // Perhaps it can also be split into separate static functions here, as it is consistent
        // with StreamingJobGraphGenerator
        try {
            FutureUtils.combineAll(
                            jobVertexBuildContext.getOperatorInfos().values().stream()
                                    .map(
                                            operatorInfo ->
                                                    operatorInfo
                                                            .getVertexConfig()
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
            Map<Integer, Map<StreamEdge, NonChainedOutput>> opIntermediateOutputs,
            JobVertexBuildContext jobVertexBuildContext) {

        StreamConfig config =
                jobVertexBuildContext.getOrCreateOperatorInfo(startNodeId).getVertexConfig();
        List<StreamEdge> transitiveOutEdges =
                jobVertexBuildContext.getChainInfo(startNodeId).getTransitiveOutEdges();

        LinkedHashSet<NonChainedOutput> transitiveOutputs = new LinkedHashSet<>();
        for (StreamEdge edge : transitiveOutEdges) {
            NonChainedOutput output = opIntermediateOutputs.get(edge.getSourceId()).get(edge);
            transitiveOutputs.add(output);
            // When a downstream vertex has been created, a connection to the downstream will be
            // created, otherwise only an IntermediateDataSet will be created for it.
            if (jobVertexBuildContext.getJobVertices().containsKey(edge.getTargetId())) {
                connect(
                        startNodeId,
                        edge,
                        output,
                        jobVertices,
                        jobVertexBuildContext.getPhysicalEdgesInOrder());
            } else {
                JobVertex jobVertex = jobVertexBuildContext.getJobVertices().get(startNodeId);
                jobVertex.getOrCreateResultDataSet(
                        output.getDataSetId(), output.getPartitionType());
                // we cache the output here for downstream vertex
                opIntermediateOutputsCaches
                        .computeIfAbsent(edge.getSourceId(), k -> new HashMap<>())
                        .put(edge, output);
                // TODO: When supporting streamGraph submission, modify this section.
                //                IntermediateDataSet dataSet =
                //                        jobVertex.getOrCreateResultDataSet(
                //                                output.getDataSetId(), output.getPartitionType());
                //                dataSet.addStreamEdge(edge);
            }
        }
        config.setVertexNonChainedOutputs(new ArrayList<>(transitiveOutputs));
    }

    // Create JobEdge with the completed upstream jobVertex
    private void connectNonChainedInput(
            Map<Integer, OperatorChainInfo> chainInfos, List<StreamEdge> physicalEdgesInOrder) {
        for (OperatorChainInfo chainInfo : chainInfos.values()) {
            List<StreamEdge> streamEdges = chainInfo.getTransitiveInEdges();
            for (StreamEdge edge : streamEdges) {
                NonChainedOutput output =
                        opIntermediateOutputsCaches.get(edge.getSourceId()).get(edge);
                Integer sourceStartNodeId = frozenNodeToStartNodeMap.get(edge.getSourceId());
                connect(sourceStartNodeId, edge, output, jobVertices, physicalEdgesInOrder);
            }
        }
    }

    private void createJobVerticesByChainInfos(JobVertexBuildContext jobVertexBuildContext) {
        Map<Integer, OperatorChainInfo> chainInfos = jobVertexBuildContext.getChainInfos();
        for (OperatorChainInfo chainInfo : chainInfos.values()) {
            JobVertex jobVertex = createJobVertex(chainInfo, jobVertexBuildContext);
            jobVertexBuildContext.addJobVertex(chainInfo.getStartNodeId(), jobVertex);
            jobVertices.put(chainInfo.getStartNodeId(), jobVertex);
            jobGraph.addVertex(jobVertex);
            jobVertexToStartNodeMap.put(jobVertex.getID(), chainInfo.getStartNodeId());
            chainInfo
                    .getAllChainedNodes()
                    .forEach(
                            node ->
                                    frozenNodeToStartNodeMap.put(
                                            node.getId(), chainInfo.getStartNodeId()));
        }
    }

    private JobVertex createJobVertex(
            OperatorChainInfo chainInfo, JobVertexBuildContext jobVertexBuildContext) {
        JobVertex jobVertex;
        Integer streamNodeId = chainInfo.getStartNodeId();
        StreamNode streamNode = streamGraph.getStreamNode(streamNodeId);
        OperatorInfo operatorInfo = jobVertexBuildContext.getOperatorInfo(streamNodeId);

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
                            operatorInfo.getChainedName(), jobVertexId, operatorIDPairs);
            chainInfo
                    .getOrCreateFormatContainer()
                    .write(new TaskConfig(jobVertex.getConfiguration()));
        } else {
            jobVertex = new JobVertex(operatorInfo.getChainedName(), jobVertexId, operatorIDPairs);
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
                                                    operatorInfo.getChainedName()),
                                            e);
                                }
                            },
                            serializationExecutor));
        }
        if (!serializationFutures.isEmpty()) {
            coordinatorSerializationFuturesPerJobVertex.put(jobVertexId, serializationFutures);
        }

        jobVertex.setResources(
                operatorInfo.getChainedMinResource(), operatorInfo.getChainedPreferredResources());

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

        jobVertex.setParallelismConfigured(
                chainInfo.getAllChainedNodes().stream()
                        .anyMatch(StreamNode::isParallelismConfigured));

        return jobVertex;
    }

    private void createOperatorChainInfos(
            List<StreamNode> streamNodes, JobVertexBuildContext jobVertexBuildContext) {
        final Map<Integer, OperatorChainInfo> chainEntryPoints =
                buildAndGetChainEntryPoints(streamNodes, jobVertexBuildContext);
        final List<OperatorChainInfo> chainInfos = new ArrayList<>(chainEntryPoints.values());
        for (OperatorChainInfo info : chainInfos) {
            generateOperatorChainInfo(
                    info.getStartNodeId(), info, chainEntryPoints, jobVertexBuildContext);
        }
    }

    private Map<Integer, OperatorChainInfo> buildAndGetChainEntryPoints(
            List<StreamNode> streamNodes, JobVertexBuildContext jobVertexBuildContext) {
        for (StreamNode streamNode : streamNodes) {
            buildChainEntryPoint(streamNode);
        }
        return getChainEntryPoints(jobVertexBuildContext);
    }

    private void buildChainEntryPoint(StreamNode streamNode) {
        int streamNodeId = streamNode.getId();
        if (!isSourceChainable(streamNode)) {
            pendingSourceChainInfos.computeIfAbsent(
                    streamNodeId,
                    ignored ->
                            new OperatorChainInfo(
                                    streamNodeId,
                                    hashes,
                                    legacyHashes,
                                    new HashMap<>(),
                                    streamGraph));
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
                            ignored ->
                                    new OperatorChainInfo(
                                            startNodeId,
                                            hashes,
                                            legacyHashes,
                                            new HashMap<>(),
                                            streamGraph));

            chainInfo.addChainedSource(
                    streamNodeId, new ChainedSourceInfo(operatorConfig, inputConfig));
            chainInfo.recordChainedNode(streamNodeId);
            chainInfo.addCoordinatorProvider(coordinatorProvider);
        }
    }

    private Map<Integer, OperatorChainInfo> getChainEntryPoints(
            JobVertexBuildContext jobVertexBuildContext) {

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
                // we cache the chainable outputs here, and set the chained config later
                jobVertexBuildContext
                        .getOrCreateOperatorInfo(sourceNodeId)
                        .setChainableOutputs(Collections.singletonList(sourceOutEdge));
            }
            LOG.info(
                    "chainInfo with startNodeId {} has been removed from pending queue.",
                    startNodeId);
            iterator.remove();
        }
        return chainEntryPoints;
    }

    private List<StreamEdge> generateOperatorChainInfo(
            final Integer currentNodeId,
            final OperatorChainInfo chainInfo,
            final Map<Integer, OperatorChainInfo> chainEntryPoints,
            final JobVertexBuildContext jobVertexBuildContext) {

        Integer startNodeId = chainInfo.getStartNodeId();
        // TODO: remove chainInfos
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
            jobVertexBuildContext.addOutputBlockingNode(currentNodeId);
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
                jobVertexBuildContext.addOutputBlockingNode(chainable.getTargetId());
            }
            transitiveOutEdges.addAll(
                    generateOperatorChainInfo(
                            chainable.getTargetId(),
                            chainInfo,
                            chainEntryPoints,
                            jobVertexBuildContext));
            // Mark upstream nodes in the same chain as outputBlocking
            if (jobVertexBuildContext.isOutputBlockingNode(chainable.getTargetId())) {
                jobVertexBuildContext.addOutputBlockingNode(currentNodeId);
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
                        jobVertexBuildContext);
            }
        }

        OperatorInfo operatorInfo = jobVertexBuildContext.getOrCreateOperatorInfo(currentNodeId);

        operatorInfo.setChainedName(
                createChainedName(
                        currentNodeId,
                        chainableOutputs,
                        Optional.ofNullable(chainEntryPoints.get(currentNodeId)),
                        jobVertexBuildContext));

        operatorInfo.setChainedMinResource(
                createChainedMinResources(currentNodeId, chainableOutputs, jobVertexBuildContext));

        operatorInfo.setChainedPreferredResources(
                createChainedPreferredResources(
                        currentNodeId, chainableOutputs, jobVertexBuildContext));

        // we cache the non-chainable outputs here, and set the non-chained config later
        operatorInfo.setNonChainableOutputs(nonChainableOutputs);

        operatorInfo.setChainableOutputs(chainableOutputs);

        tryConvertPartitionerForDynamicGraph(
                chainableOutputs, nonChainableOutputs, jobVertexBuildContext);

        OperatorID currentOperatorId =
                chainInfo.addNodeToChain(
                        currentNodeId, streamGraph.getStreamNode(currentNodeId).getOperatorName());

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
            jobVertexBuildContext.addChainInfo(startNodeId, chainInfo);
            chainInfos.put(startNodeId, chainInfo);
        }

        return transitiveOutEdges;
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
}
