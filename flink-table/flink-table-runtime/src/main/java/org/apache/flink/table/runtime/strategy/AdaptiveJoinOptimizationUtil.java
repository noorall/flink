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

package org.apache.flink.table.runtime.strategy;

import org.apache.flink.streaming.api.graph.util.ImmutableStreamEdge;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

public class AdaptiveJoinOptimizationUtil {
    public static List<ImmutableStreamEdge> filterEdges(
            List<ImmutableStreamEdge> inEdges, int typeNumber) {
        return inEdges.stream()
                .filter(e -> e.getTypeNumber() == typeNumber)
                .collect(Collectors.toList());
    }

    public static long computeSkewThreshold(
            long mediaSize, double skewedFactor, long defaultSkewedThreshold) {
        return (long) Math.max(mediaSize * skewedFactor, defaultSkewedThreshold);
    }

    public static long median(long[] nums) {
        int len = nums.length;
        long[] sortedNums = LongStream.of(nums).sorted().toArray();
        if (len % 2 == 0) {
            return Math.max((sortedNums[len / 2] + sortedNums[len / 2 - 1]) / 2, 1L);
        } else {
            return Math.max(sortedNums[len / 2], 1L);
        }
    }
}
