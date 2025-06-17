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
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.api.graph.multinput.input.FirstInputOfTwoInput;
import org.apache.flink.streaming.api.graph.multinput.input.InputSpec;
import org.apache.flink.streaming.api.graph.multinput.input.OneInput;
import org.apache.flink.streaming.api.graph.multinput.input.SecondInputOfTwoInput;
import org.apache.flink.streaming.api.graph.multinput.output.BroadcastingOutput;
import org.apache.flink.streaming.api.graph.multinput.output.CopyingBroadcastingOutput;
import org.apache.flink.streaming.api.graph.multinput.output.CopyingFirstInputOfTwoInputStreamOperatorOutput;
import org.apache.flink.streaming.api.graph.multinput.output.CopyingOneInputStreamOperatorOutput;
import org.apache.flink.streaming.api.graph.multinput.output.CopyingSecondInputOfTwoInputStreamOperatorOutput;
import org.apache.flink.streaming.api.graph.multinput.output.FirstInputOfTwoInputStreamOperatorOutput;
import org.apache.flink.streaming.api.graph.multinput.output.OneInputStreamOperatorOutput;
import org.apache.flink.streaming.api.graph.multinput.output.SecondInputOfTwoInputStreamOperatorOutput;
import org.apache.flink.streaming.api.operators.AbstractStreamOperatorV2;
import org.apache.flink.streaming.api.operators.Input;
import org.apache.flink.streaming.api.operators.MultipleInputStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.Output;
import org.apache.flink.streaming.api.operators.StreamOperator;
import org.apache.flink.streaming.api.operators.StreamOperatorParameters;
import org.apache.flink.streaming.api.operators.TwoInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.IdentityHashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.stream.Collectors;

import static org.apache.flink.util.Preconditions.checkArgument;

/** Base {@link MultipleInputStreamOperator} to handle multiple inputs in table module. */
@Internal
public abstract class MultipleInputStreamOperatorBase<OUT> extends AbstractStreamOperatorV2<OUT>
        implements MultipleInputStreamOperator<OUT> {

    private final List<InputSpec> inputSpecs;

    protected final Map<Integer, InputSpec> inputSpecMap;

    /** The head operators of this multiple input operator. */
    private final List<TableOperatorWrapper<?, ?>> headWrappers;

    /** The tail operator of this multiple input operator. */
    private final TableOperatorWrapper<?, OUT> tailWrapper;

    /** all operator as topological ordering in this multiple input operator. */
    protected final Deque<TableOperatorWrapper<?, ?>> topologicalOrderingOperators;

    public MultipleInputStreamOperatorBase(
            StreamOperatorParameters<OUT> parameters,
            List<InputSpec> inputSpecs,
            List<TableOperatorWrapper<?, ?>> headWrappers,
            TableOperatorWrapper<?, OUT> tailWrapper) {
        super(parameters, inputSpecs.size());
        this.inputSpecs = inputSpecs;
        this.headWrappers = headWrappers;
        this.tailWrapper = tailWrapper;
        // get all operator list as topological ordering
        this.topologicalOrderingOperators = getAllOperatorsAsTopologicalOrdering();
        // create all operators by corresponding operator factory
        createAllOperators(parameters);
        this.inputSpecMap =
                inputSpecs.stream()
                        .collect(Collectors.toMap(InputSpec::getMultipleInputId, s -> s));
        checkArgument(inputSpecMap.size() == inputSpecs.size());
    }

    @Override
    public List<Input> getInputs() {
        createInputs();
        return inputs;
    }

    private List<Input> inputs;

    private void createInputs() {
        if (inputs == null) {
            this.inputs = inputSpecs.stream().map(this::createInput).collect(Collectors.toList());
        }
    }

    private Input createInput(InputSpec inputSpec) {
        StreamOperator<?> operator = inputSpec.getOutput().getStreamOperator();
        if (operator instanceof OneInputStreamOperator) {
            return new OneInput((OneInputStreamOperator<?, ?>) operator);
        } else if (operator instanceof TwoInputStreamOperator) {
            TwoInputStreamOperator<?, ?, ?> twoInputOp = (TwoInputStreamOperator<?, ?, ?>) operator;
            if (inputSpec.getOutputOpInputId() == 1) {
                return new FirstInputOfTwoInput(twoInputOp);
            } else {
                return new SecondInputOfTwoInput(twoInputOp);
            }
        } else if (operator instanceof BatchMultipleInputStreamOperator) {
            BatchMultipleInputStreamOperator<?> multiOp =
                    (BatchMultipleInputStreamOperator<?>) operator;
            return multiOp.getInputs().get(inputSpec.getOutputOpInputId() - 1);
        } else {
            throw new RuntimeException("Unsupported StreamOperator: " + operator);
        }
    }

    /**
     * Open all sub-operators in a multiple input operator from <b>tail to head</b>, contrary to
     * {@link StreamOperator#close()} which happens <b>head to tail</b> (see {@link #close()}).
     */
    @Override
    public void open() throws Exception {
        super.open();
        final Iterator<TableOperatorWrapper<?, ?>> it =
                topologicalOrderingOperators.descendingIterator();
        while (it.hasNext()) {
            StreamOperator<?> operator = it.next().getStreamOperator();
            operator.open();
        }
    }

    /**
     * Finish all sub-operators in a multiple input operator effect way. Finishing happens from
     * <b>head to tail</b> sub-operator in a multiple input operator, contrary to {@link
     * StreamOperator#open()} which happens <b>tail to head</b>.
     */
    @Override
    public void finish() throws Exception {
        super.finish();
        for (TableOperatorWrapper<?, ?> wrapper : topologicalOrderingOperators) {
            StreamOperator<?> operator = wrapper.getStreamOperator();
            operator.finish();
        }
    }

    /**
     * Closes all sub-operators in a multiple input operator effect way. Closing happens from
     * <b>head to tail</b> sub-operator in a multiple input operator, contrary to {@link
     * StreamOperator#open()} which happens <b>tail to head</b>.
     */
    @Override
    public void close() throws Exception {
        super.close();
        for (TableOperatorWrapper<?, ?> wrapper : topologicalOrderingOperators) {
            wrapper.close();
        }
    }

    private Deque<TableOperatorWrapper<?, ?>> getAllOperatorsAsTopologicalOrdering() {
        final Deque<TableOperatorWrapper<?, ?>> allOperators = new ArrayDeque<>();
        final Queue<TableOperatorWrapper<?, ?>> toVisitOperators = new LinkedList<>();

        // mapping an operator to its input count
        final Map<TableOperatorWrapper<?, ?>, Integer> operatorToInputCount =
                buildOperatorToInputCountMap();

        // find the operators which all inputs are not in this multiple input operator to traverse
        // first
        for (TableOperatorWrapper<?, ?> wrapper : headWrappers) {
            if (operatorToInputCount.get(wrapper) == 0) {
                toVisitOperators.add(wrapper);
            }
        }
        checkArgument(!toVisitOperators.isEmpty(), "This should not happen.");

        while (!toVisitOperators.isEmpty()) {
            TableOperatorWrapper<?, ?> wrapper = toVisitOperators.poll();
            allOperators.add(wrapper);

            for (TableOperatorWrapper<?, ?> output : wrapper.getOutputWrappers()) {
                int inputCountRemaining = operatorToInputCount.get(output) - 1;
                operatorToInputCount.put(output, inputCountRemaining);
                if (inputCountRemaining == 0) {
                    toVisitOperators.add(output);
                }
            }
        }

        return allOperators;
    }

    private Map<TableOperatorWrapper<?, ?>, Integer> buildOperatorToInputCountMap() {
        final Map<TableOperatorWrapper<?, ?>, Integer> operatorToInputCount =
                new IdentityHashMap<>();
        final Queue<TableOperatorWrapper<?, ?>> toVisitOperators = new LinkedList<>();
        toVisitOperators.add(tailWrapper);

        while (!toVisitOperators.isEmpty()) {
            TableOperatorWrapper<?, ?> wrapper = toVisitOperators.poll();
            List<TableOperatorWrapper<?, ?>> inputs = wrapper.getInputWrappers();
            operatorToInputCount.put(wrapper, inputs.size());
            toVisitOperators.addAll(inputs);
        }

        return operatorToInputCount;
    }

    /**
     * Create all sub-operators by corresponding operator factory in a multiple input operator from
     * <b>tail to head</b>.
     */
    @SuppressWarnings("unchecked")
    private <T> void createAllOperators(StreamOperatorParameters<OUT> parameters) {
        final boolean isObjectReuseEnabled =
                parameters.getContainingTask().getExecutionConfig().isObjectReuseEnabled();
        final ExecutionConfig executionConfig = parameters.getContainingTask().getExecutionConfig();
        final Iterator<TableOperatorWrapper<?, ?>> it =
                topologicalOrderingOperators.descendingIterator();

        while (it.hasNext()) {
            final TableOperatorWrapper<?, ?> wrapper = it.next();
            if (wrapper == this.tailWrapper) {
                // output = this.output;
                final StreamOperatorParameters<OUT> newParameters =
                        createSubOperatorParameters(parameters, this.output, wrapper);
                tailWrapper.createOperator(newParameters);
            } else {
                TableOperatorWrapper<?, T> castedWrapper = (TableOperatorWrapper<?, T>) wrapper;
                final Output<StreamRecord<T>> output;
                final int numberOfOutputs = castedWrapper.getOutputEdges().size();
                final Output<StreamRecord<T>>[] outputs = new Output[numberOfOutputs];
                for (int i = 0; i < numberOfOutputs; ++i) {
                    TableOperatorWrapper.Edge edge = castedWrapper.getOutputEdges().get(i);
                    int inputId = edge.getInputId();
                    StreamOperator<?> outputOperator = edge.getTarget().getStreamOperator();
                    if (isObjectReuseEnabled) {
                        outputs[i] = createOutput(outputOperator, inputId);
                    } else {
                        // the source's output type info is equal to the target's type info for the
                        // corresponding index
                        TypeSerializer<T> serializer =
                                (TypeSerializer<T>)
                                        edge.getSource()
                                                .getOutputType()
                                                .createSerializer(
                                                        executionConfig.getSerializerConfig());
                        outputs[i] = createCopyingOutput(serializer, outputOperator, inputId);
                    }
                }
                if (outputs.length == 1) {
                    output = outputs[0];
                } else {
                    // This is the inverse of creating the normal Output.
                    // In case of object reuse, we need to copy in the broadcast output.
                    // Because user's operator may change the record passed to it.
                    if (isObjectReuseEnabled) {
                        output = new CopyingBroadcastingOutput<>(outputs);
                    } else {
                        output = new BroadcastingOutput<>(outputs);
                    }
                }

                final StreamOperatorParameters<T> newParameters =
                        createSubOperatorParameters(parameters, output, wrapper);
                castedWrapper.createOperator(newParameters);
            }
        }
    }

    private <T> StreamOperatorParameters<T> createSubOperatorParameters(
            StreamOperatorParameters<OUT> multipleInputOperatorParameters,
            Output<StreamRecord<T>> output,
            TableOperatorWrapper<?, ?> wrapper) {
        final StreamConfig streamConfig =
                createStreamConfig(multipleInputOperatorParameters, wrapper);
        return new StreamOperatorParameters<>(
                multipleInputOperatorParameters.getContainingTask(),
                streamConfig,
                output,
                multipleInputOperatorParameters::getProcessingTimeService,
                multipleInputOperatorParameters.getOperatorEventDispatcher(),
                multipleInputOperatorParameters.getMailboxExecutor());
    }

    protected StreamConfig createStreamConfig(
            StreamOperatorParameters<OUT> multipleInputOperatorParameters,
            TableOperatorWrapper<?, ?> wrapper) {
        final ExecutionConfig executionConfig = getExecutionConfig();

        final StreamConfig streamConfig =
                new StreamConfig(
                        multipleInputOperatorParameters
                                .getStreamConfig()
                                .getConfiguration()
                                .clone());
        streamConfig.setOperatorName(wrapper.getOperatorName());
        streamConfig.setNumberOfNetworkInputs(wrapper.getAllInputTypes().size());
        streamConfig.setNumberOfOutputs(wrapper.getOutputEdges().size());
        streamConfig.setupNetworkInputs(
                wrapper.getAllInputTypes().stream()
                        .map(t -> t.createSerializer(executionConfig.getSerializerConfig()))
                        .toArray(TypeSerializer[]::new));
        streamConfig.setTypeSerializerOut(
                wrapper.getOutputType().createSerializer(executionConfig.getSerializerConfig()));
        streamConfig.serializeAllConfigs();
        return streamConfig;
    }

    private <T> Output<StreamRecord<T>> createOutput(
            StreamOperator<?> outputOperator, int inputId) {
        if (outputOperator instanceof OneInputStreamOperator) {
            OneInputStreamOperator<T, ?> oneInputOp = (OneInputStreamOperator<T, ?>) outputOperator;
            return new OneInputStreamOperatorOutput<>(oneInputOp);
        } else if (outputOperator instanceof TwoInputStreamOperator) {
            if (inputId == 1) {
                TwoInputStreamOperator<T, ?, ?> twoInputOp =
                        (TwoInputStreamOperator<T, ?, ?>) outputOperator;
                return new FirstInputOfTwoInputStreamOperatorOutput<>(twoInputOp);
            } else {
                TwoInputStreamOperator<?, T, ?> twoInputOp =
                        (TwoInputStreamOperator<?, T, ?>) outputOperator;
                return new SecondInputOfTwoInputStreamOperatorOutput<>(twoInputOp);
            }
        } else {
            throw new RuntimeException("Unsupported StreamOperator: " + outputOperator);
        }
    }

    private <T> Output<StreamRecord<T>> createCopyingOutput(
            TypeSerializer<T> serializer, StreamOperator<?> outputOperator, int inputId) {
        if (outputOperator instanceof OneInputStreamOperator) {
            final OneInputStreamOperator<T, ?> oneInputOp =
                    (OneInputStreamOperator<T, ?>) outputOperator;
            return new CopyingOneInputStreamOperatorOutput<>(oneInputOp, serializer);
        } else if (outputOperator instanceof TwoInputStreamOperator) {
            if (inputId == 1) {
                final TwoInputStreamOperator<T, ?, ?> twoInputOp =
                        (TwoInputStreamOperator<T, ?, ?>) outputOperator;
                return new CopyingFirstInputOfTwoInputStreamOperatorOutput<>(
                        twoInputOp, serializer);
            } else {
                final TwoInputStreamOperator<?, T, ?> twoInputOp =
                        (TwoInputStreamOperator<?, T, ?>) outputOperator;
                return new CopyingSecondInputOfTwoInputStreamOperatorOutput<>(
                        twoInputOp, serializer);
            }
        } else {
            throw new RuntimeException("Unsupported StreamOperator: " + outputOperator);
        }
    }
}
