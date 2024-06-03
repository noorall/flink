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
import org.apache.flink.streaming.api.operators.AdaptiveJoin;
import org.apache.flink.streaming.api.operators.SimpleOperatorFactory;
import org.apache.flink.streaming.api.operators.StreamOperator;

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

    private final AdaptiveJoinOperator adaptiveJoinOperator;

    private final List<PotentialBroadcastSide> potentialBroadcastJoinSides;

    public AdaptiveJoinOperatorFactory(StreamOperator<OUT> operator, int maybeBroadcastJoinSide) {
        super(operator);
        adaptiveJoinOperator = (AdaptiveJoinOperator) operator;
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
        switch (side) {
            case LEFT:
                adaptiveJoinOperator.activateBroadcastJoin(true);
                break;
            case RIGHT:
                adaptiveJoinOperator.activateBroadcastJoin(false);
                break;
            default:
                throw new IllegalArgumentException("invalid: " + side);
        }
    }

    @Override
    public List<PotentialBroadcastSide> getPotentialBroadcastJoinSides() {
        return potentialBroadcastJoinSides;
    }
}
