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

package org.apache.flink.runtime.executiongraph;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

/** Utils for {@link IndexRange}. */
public class IndexRangeUtil {
    public static List<IndexRange> mergeIndexRanges(Collection<IndexRange> ranges) {
        if (ranges == null || ranges.isEmpty()) {
            return new ArrayList<>();
        }

        List<IndexRange> sortedRanges =
                ranges.stream()
                        .sorted(Comparator.comparingInt(IndexRange::getStartIndex))
                        .collect(Collectors.toList());

        List<IndexRange> merged = new ArrayList<>();
        IndexRange current = sortedRanges.get(0);

        for (int i = 1; i < ranges.size(); i++) {
            IndexRange next = sortedRanges.get(i);
            // <1,4>,<5,6>; <1,4>,<3,6>
            if (next.getStartIndex() <= current.getEndIndex() + 1) {
                current =
                        new IndexRange(
                                current.getStartIndex(),
                                Math.max(current.getEndIndex(), next.getEndIndex()));
            } else {
                merged.add(current);
                current = next;
            }
        }
        merged.add(current);

        return merged;
    }
}
