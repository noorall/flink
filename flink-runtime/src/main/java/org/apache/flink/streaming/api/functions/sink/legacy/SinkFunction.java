/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.api.functions.sink.legacy;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.Public;
import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.common.functions.Function;

import java.io.Serializable;

/**
 * Interface for implementing user defined sink functionality.
 *
 * @param <IN> Input type parameter.
 * @deprecated This interface will be removed in future versions. Use the new {@link
 *     org.apache.flink.api.connector.sink2.Sink} interface instead.
 */
@Internal
@Deprecated
public interface SinkFunction<IN> extends Function, Serializable {

    /**
     * @deprecated Use {@link #invoke(Object, Context)}.
     */
    default void invoke(IN value) throws Exception {}

    /**
     * Writes the given value to the sink. This function is called for every record.
     *
     * <p>You have to override this method when implementing a {@code SinkFunction}, this is a
     * {@code default} method for backward compatibility with the old-style method only.
     *
     * @param value The input record.
     * @param context Additional context about the input record.
     * @throws Exception This method may throw exceptions. Throwing an exception will cause the
     *     operation to fail and may trigger recovery.
     */
    default void invoke(IN value, Context context) throws Exception {
        invoke(value);
    }

    /**
     * Writes the given watermark to the sink. This function is called for every watermark.
     *
     * <p>This method is intended for advanced sinks that propagate watermarks.
     *
     * @param watermark The watermark.
     * @throws Exception This method may throw exceptions. Throwing an exception will cause the
     *     operation to fail and may trigger recovery.
     */
    default void writeWatermark(Watermark watermark) throws Exception {}

    /**
     * This method is called at the end of data processing.
     *
     * <p>The method is expected to flush all remaining buffered data. Exceptions will cause the
     * pipeline to be recognized as failed, because the last data items are not processed properly.
     * You may use this method to flush remaining buffered elements in the state into transactions
     * which you can commit in the last checkpoint.
     *
     * <p><b>NOTE:</b>This method does not need to close any resources. You should release external
     * resources in the {@link RichSinkFunction#close()} method.
     *
     * @throws Exception This method may throw exceptions. Throwing an exception will cause the
     *     operation to fail and may trigger recovery.
     */
    default void finish() throws Exception {}

    /**
     * Context that {@link SinkFunction SinkFunctions } can use for getting additional data about an
     * input record.
     *
     * <p>The context is only valid for the duration of a {@link SinkFunction#invoke(Object,
     * Context)} call. Do not store the context and use afterwards!
     */
    @Public // Interface might be extended in the future with additional methods.
    interface Context {

        /** Returns the current processing time. */
        long currentProcessingTime();

        /** Returns the current event-time watermark. */
        long currentWatermark();

        /**
         * Returns the timestamp of the current input record or {@code null} if the element does not
         * have an assigned timestamp.
         */
        Long timestamp();
    }
}
