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

package org.apache.flink.streaming.api.graph.multinput.output;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.runtime.event.WatermarkEvent;
import org.apache.flink.streaming.api.operators.Output;
import org.apache.flink.streaming.api.operators.TwoInputStreamOperator;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.LatencyMarker;
import org.apache.flink.streaming.runtime.streamrecord.RecordAttributes;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.watermarkstatus.WatermarkStatus;
import org.apache.flink.util.OutputTag;

/**
 * An {@link Output} that can be used to emit copying elements and other messages for the second
 * input of {@link TwoInputStreamOperator}.
 */
@Internal
public class CopyingSecondInputOfTwoInputStreamOperatorOutput<IN1, IN2, OUT>
        extends OutputBase<IN2> {

    private final TwoInputStreamOperator<IN1, IN2, OUT> operator;
    private final TypeSerializer<IN2> serializer;

    public CopyingSecondInputOfTwoInputStreamOperatorOutput(
            TwoInputStreamOperator<IN1, IN2, OUT> operator, TypeSerializer<IN2> serializer) {
        super(operator);
        this.operator = operator;
        this.serializer = serializer;
    }

    @Override
    public void emitWatermark(Watermark mark) {
        try {
            operator.processWatermark2(mark);
        } catch (Exception e) {
            throw new ExceptionInMultipleInputOperatorException(e);
        }
    }

    @Override
    public void emitWatermarkStatus(WatermarkStatus watermarkStatus) {
        try {
            operator.processWatermarkStatus2(watermarkStatus);
        } catch (Exception e) {
            throw new ExceptionInMultipleInputOperatorException(e);
        }
    }

    @Override
    public void emitLatencyMarker(LatencyMarker latencyMarker) {
        try {
            operator.processLatencyMarker2(latencyMarker);
        } catch (Exception e) {
            throw new ExceptionInMultipleInputOperatorException(e);
        }
    }

    @Override
    public void emitRecordAttributes(RecordAttributes recordAttributes) {
        try {
            operator.processRecordAttributes2(recordAttributes);
        } catch (Exception e) {
            throw new ExceptionInMultipleInputOperatorException(e);
        }
    }

    @Override
    public void emitWatermark(WatermarkEvent watermark) {
        try {
            operator.processWatermark2(watermark);
        } catch (Exception e) {
            throw new ExceptionInMultipleInputOperatorException(e);
        }
    }

    @Override
    public void collect(StreamRecord<IN2> record) {
        pushToOperator(record);
    }

    @Override
    public <X> void collect(OutputTag<X> outputTag, StreamRecord<X> record) {
        pushToOperator(record);
    }

    protected <X> void pushToOperator(StreamRecord<X> record) {
        try {
            // we know that the given outputTag matches our OutputTag so the record
            // must be of the type that our operator expects.
            @SuppressWarnings("unchecked")
            StreamRecord<IN2> castRecord = (StreamRecord<IN2>) record;
            StreamRecord<IN2> copy = castRecord.copy(serializer.copy(castRecord.getValue()));

            operator.processElement2(copy);
        } catch (Exception e) {
            throw new ExceptionInMultipleInputOperatorException(e);
        }
    }
}
