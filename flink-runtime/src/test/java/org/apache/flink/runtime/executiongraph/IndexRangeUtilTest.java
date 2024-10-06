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

import org.junit.jupiter.api.Test;

import java.util.Collection;
import java.util.List;

import static org.apache.flink.runtime.executiongraph.IndexRangeUtil.mergeIndexRanges;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Test for {@link IndexRangeUtil}. */
public class IndexRangeUtilTest {
    @Test
    void testMergeIndexRanges() {
        Collection<IndexRange> emptyList = List.of();
        List<IndexRange> emptyResult = mergeIndexRanges(emptyList);
        assertTrue(emptyResult.isEmpty());

        Collection<IndexRange> singleRangeList = List.of(new IndexRange(5, 10));
        List<IndexRange> singleRangeResult = mergeIndexRanges(singleRangeList);
        assertEquals(List.of(new IndexRange(5, 10)), singleRangeResult);

        Collection<IndexRange> overlappingRangesList =
                List.of(new IndexRange(5, 10), new IndexRange(8, 12));
        List<IndexRange> overlappingRangesResult = mergeIndexRanges(overlappingRangesList);
        assertEquals(List.of(new IndexRange(5, 12)), overlappingRangesResult);

        Collection<IndexRange> nonOverlappingRangesList =
                List.of(new IndexRange(1, 5), new IndexRange(10, 15));
        List<IndexRange> nonOverlappingRangesResult = mergeIndexRanges(nonOverlappingRangesList);
        assertEquals(
                List.of(new IndexRange(1, 5), new IndexRange(10, 15)), nonOverlappingRangesResult);

        Collection<IndexRange> touchingRangesList =
                List.of(new IndexRange(1, 5), new IndexRange(6, 10));
        List<IndexRange> touchingRangesResult = mergeIndexRanges(touchingRangesList);
        assertEquals(List.of(new IndexRange(1, 10)), touchingRangesResult);

        Collection<IndexRange> mixedRangesList =
                List.of(
                        new IndexRange(1, 3),
                        new IndexRange(2, 6),
                        new IndexRange(8, 10),
                        new IndexRange(15, 18),
                        new IndexRange(19, 20));
        List<IndexRange> mixedRangesResult = mergeIndexRanges(mixedRangesList);
        assertEquals(
                List.of(new IndexRange(1, 6), new IndexRange(8, 10), new IndexRange(15, 20)),
                mixedRangesResult);
    }
}
