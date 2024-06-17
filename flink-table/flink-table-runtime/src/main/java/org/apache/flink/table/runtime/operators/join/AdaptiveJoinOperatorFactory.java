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
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.AbstractStreamOperatorFactory;
import org.apache.flink.streaming.api.operators.AdaptiveJoin;
import org.apache.flink.streaming.api.operators.SetupableStreamOperator;
import org.apache.flink.streaming.api.operators.SimpleOperatorFactory;
import org.apache.flink.streaming.api.operators.StreamOperator;
import org.apache.flink.streaming.api.operators.StreamOperatorFactory;
import org.apache.flink.streaming.api.operators.StreamOperatorParameters;
import org.apache.flink.streaming.api.operators.SwitchBroadcastSide;
import org.apache.flink.table.runtime.generated.GeneratedClass;
import org.apache.flink.table.runtime.operators.CodeGenOperatorFactory;

import java.util.ArrayList;
import java.util.List;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Adaptive join factory.
 *
 * @param <OUT> The output type of the operator
 */
@Internal
public class AdaptiveJoinOperatorFactory<OUT> extends AbstractStreamOperatorFactory<OUT>
        implements AdaptiveJoin {
    private final List<PotentialBroadcastSide> potentialBroadcastJoinSides;

    private final StreamOperatorFactory<OUT> broadcastFactory;

    private StreamOperatorFactory<OUT> finalFactory;

    private boolean isBroadcastJoin;

    private boolean isLeftBuild;

    public AdaptiveJoinOperatorFactory(StreamOperatorFactory<OUT> originalFactory,
                                       StreamOperatorFactory<OUT> broadcastFactory,
                                       int maybeBroadcastJoinSide) {
        this.finalFactory = originalFactory;
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
        isBroadcastJoin = true;
        this.finalFactory = broadcastFactory;
        switch (side) {
            case LEFT:
                isLeftBuild = true;
                break;
            case RIGHT:
                isLeftBuild = false;
                break;
            default:
                throw new IllegalArgumentException("invalid: " + side);
        }
    }

    @Override
    public List<PotentialBroadcastSide> getPotentialBroadcastJoinSides() {
        return potentialBroadcastJoinSides;
    }

    @Override
    public <T extends StreamOperator<OUT>> T createStreamOperator(StreamOperatorParameters<OUT> parameters) {
        if (finalFactory instanceof AbstractStreamOperatorFactory) {
            ((AbstractStreamOperatorFactory) finalFactory).setProcessingTimeService(processingTimeService);
        }
        StreamOperator<OUT> operator = finalFactory.createStreamOperator(parameters);
        if (isBroadcastJoin && operator instanceof SwitchBroadcastSide) {
            ((SwitchBroadcastSide) operator)
                    .activateBroadcastJoin(isLeftBuild);
        }
        return (T) operator;
    }

    @Override
    public Class<? extends StreamOperator> getStreamOperatorClass(ClassLoader classLoader) {
        return finalFactory.getStreamOperatorClass(classLoader);
    }
}
