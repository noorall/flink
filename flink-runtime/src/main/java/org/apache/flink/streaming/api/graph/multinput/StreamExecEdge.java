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

@Internal
public class StreamExecEdge {
    private StreamExecNode source;
    private int typeNumber;

    public StreamExecEdge(StreamExecNode source, int typeNumber) {
        this.source = source;
        this.typeNumber = typeNumber;
    }

    public StreamExecNode getSource() {
        return source;
    }

    public void setSource(StreamExecNode node) {
        this.source = node;
    }

    private boolean isBroken;

    public void broken() {
        isBroken = true;
    }

    public boolean isBroken() {
        return isBroken;
    }

    public int getTypeNumber() {
        return typeNumber;
    }

    public void setTypeNumber(int typeNumber) {
        this.typeNumber = typeNumber;
    }

    @Override
    public String toString() {
        return "StreamExecEdge{"
                + "source="
                + source
                + ", typeNumber="
                + typeNumber
                + ", isBroken="
                + isBroken
                + '}';
    }
}
