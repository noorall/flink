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

package org.apache.flink.streaming.api.operators;

import org.apache.flink.annotation.PublicEvolving;

import java.util.List;

/**
 * Provides interfaces for adaptive join operations within a Flink job. This handler allows for
 * specifying and retrieving the side(s) of a join operation that can be optimized as a broadcast
 * join.
 */
@PublicEvolving
public interface AdaptiveJoin {

    /**
     * Represents the side of a join operation that can potentially be optimized as a broadcast
     * join. The optimization can be applied on the LEFT side or the RIGHT side.
     */
    enum PotentialBroadcastSide {
        LEFT,
        RIGHT
    }

    /**
     * Sets the side of the join that can be optimized as a broadcast join.
     *
     * @param canBeBroadcastSide the side of the join that can be optimized; must not be {@code
     *     null}.
     */
    void markAsBroadcastJoin(PotentialBroadcastSide canBeBroadcastSide);

    /**
     * Returns the side of the join that can be optimized as a broadcast join.
     *
     * @return the broadcast side that has been set for optimization, or {@code BroadcastSide.NONE}
     *     if no optimization is set.
     */
    List<PotentialBroadcastSide> getPotentialBroadcastJoinSides();
}
