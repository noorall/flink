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

package org.apache.flink.table.runtime.operators.join;

import org.apache.flink.annotation.Internal;
import org.apache.flink.streaming.api.operators.AbstractStreamOperatorFactory;
import org.apache.flink.streaming.api.operators.AdaptiveJoin;
import org.apache.flink.streaming.api.operators.SimpleOperatorFactory;
import org.apache.flink.streaming.api.operators.StreamOperator;
import org.apache.flink.streaming.api.operators.SwitchBroadcastSide;
import org.apache.flink.table.runtime.generated.GeneratedClass;

import java.util.ArrayList;
import java.util.List;

/**
 * Adaptive join factory.
 *
 * @param <OUT> The output type of the operator
 */
@Internal
public class AdaptiveJoinOperatorFactory<OUT> extends SimpleOperatorFactory<OUT>
        implements AdaptiveJoin {

    private List<PotentialBroadcastSide> potentialBroadcastJoinSides;

    private GeneratedClass<? extends StreamOperator<OUT>> generatedClass;

    private AbstractStreamOperatorFactory<OUT> originalFactory;

    private AbstractStreamOperatorFactory<OUT> broadcastFactory;

    private boolean isBroadcastJoin;

    public AdaptiveJoinOperatorFactory(AbstractStreamOperatorFactory<OUT> originalFactory,
                                       AbstractStreamOperatorFactory<OUT> broadcastFactory,
                                       int maybeBroadcastJoinSide) {
        super(((SimpleOperatorFactory<OUT>)originalFactory).getOperator());
        this.originalFactory = originalFactory;
        this.broadcastFactory = broadcastFactory;
        potentialBroadcastJoinSides = new ArrayList<>();
        if (maybeBroadcastJoinSide == 0) {
            potentialBroadcastJoinSides.add(PotentialBroadcastSide.LEFT);
        } else if (maybeBroadcastJoinSide == 1) {
            potentialBroadcastJoinSides.add(PotentialBroadcastSide.RIGHT);
        } else if (maybeBroadcastJoinSide == 2) {
            potentialBroadcastJoinSides.add(PotentialBroadcastSide.LEFT);
            potentialBroadcastJoinSides.add(PotentialBroadcastSide.RIGHT);
        }
    }

    @Override
    public void markAsBroadcastJoin(PotentialBroadcastSide side) {
        StreamOperator<OUT> streamOperator;
        switch (side) {
            case LEFT:
                streamOperator = ((SimpleOperatorFactory<OUT>)broadcastFactory).getOperator();
                ((SwitchBroadcastSide) streamOperator)
                        .activateBroadcastJoin(true);
                isBroadcastJoin = true;
                break;
            case RIGHT:
                streamOperator = ((SimpleOperatorFactory<OUT>)broadcastFactory).getOperator();
                ((SwitchBroadcastSide) streamOperator)
                        .activateBroadcastJoin(false);
                isBroadcastJoin = true;
                break;
            default:
                throw new IllegalArgumentException("invalid: " + side);
        }
        setOperator(streamOperator);
    }

    @Override
    public List<PotentialBroadcastSide> getPotentialBroadcastJoinSides() {
        return potentialBroadcastJoinSides;
    }
}
