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
import org.apache.flink.streaming.api.operators.KeyContextHandler;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

@Internal
public abstract class InputBase<IN> implements Input<IN>, KeyContextHandler {

    @Override
    public void setKeyContextElement(StreamRecord<IN> record) throws Exception {
        // do nothing
    }

    @Override
    public boolean hasKeyContext() {
        // Currently, we can simply return false due to InputBase#setKeyContextElement is an empty
        // implementation. Once there is a non-empty implementation in the future, this method
        // should also be adapted, otherwise the InputBase#setKeyContextElement will never be
        // called.
        return false;
    }
}
