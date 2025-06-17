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

package org.apache.flink.streaming.api.graph.multinput.input;

import org.apache.flink.annotation.Internal;
import org.apache.flink.streaming.api.operators.Input;
import org.apache.flink.streaming.api.operators.TwoInputStreamOperator;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.operators.asyncprocessing.AsyncStateProcessing;
import org.apache.flink.streaming.runtime.streamrecord.LatencyMarker;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.watermarkstatus.WatermarkStatus;
import org.apache.flink.util.function.ThrowingConsumer;

/** {@link Input} for the first input of {@link TwoInputStreamOperator}. */
@Internal
public class FirstInputOfTwoInput<IN1, IN2, OUT> extends InputBase<IN1>
        implements AsyncStateProcessing {

    private final TwoInputStreamOperator<IN1, IN2, OUT> operator;

    public FirstInputOfTwoInput(TwoInputStreamOperator<IN1, IN2, OUT> operator) {
        this.operator = operator;
    }

    @Override
    public void processElement(StreamRecord<IN1> element) throws Exception {
        operator.processElement1(element);
    }

    @Override
    public void processWatermark(Watermark mark) throws Exception {
        operator.processWatermark1(mark);
    }

    @Override
    public void processLatencyMarker(LatencyMarker latencyMarker) throws Exception {
        operator.processLatencyMarker1(latencyMarker);
    }

    @Override
    public void processWatermarkStatus(WatermarkStatus watermarkStatus) throws Exception {
        operator.processWatermarkStatus1(watermarkStatus);
    }

    @Internal
    @Override
    public final boolean isAsyncStateProcessingEnabled() {
        return (operator instanceof AsyncStateProcessing)
                && ((AsyncStateProcessing) operator).isAsyncStateProcessingEnabled();
    }

    @Internal
    @Override
    public final <T> ThrowingConsumer<StreamRecord<T>, Exception> getRecordProcessor(int inputId) {
        return ((AsyncStateProcessing) operator).getRecordProcessor(1);
    }
}
