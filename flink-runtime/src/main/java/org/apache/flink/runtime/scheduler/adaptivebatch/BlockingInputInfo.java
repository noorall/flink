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

package org.apache.flink.runtime.scheduler.adaptivebatch;

import org.apache.flink.runtime.jobgraph.DataDistributionType;

public class BlockingInputInfo implements Comparable<BlockingInputInfo> {
    private final BlockingResultInfo blockingResultInfo;
    private final DataDistributionType dataDistributionType;
    private final int typeNumber;
    private final int index;
    private final boolean isSplittable;

    public BlockingInputInfo(
            BlockingResultInfo blockingResultInfo,
            int typeNumber,
            DataDistributionType dataDistributionType,
            int index) {
        this.blockingResultInfo = blockingResultInfo;
        this.typeNumber = typeNumber;
        this.dataDistributionType = dataDistributionType;
        this.index = index;
    }

    public BlockingResultInfo getConsumedResultInfo() {
        return blockingResultInfo;
    }

    public boolean isPointWise() {
        return blockingResultInfo.isPointwise();
    }

    public boolean isBroadcast() {
        return blockingResultInfo.isBroadcast();
    }

    public int getTypeNumber() {
        return typeNumber;
    }

    public DataDistributionType getDataDistributionType() {
        return dataDistributionType;
    }

    public int getNumPartitions() {
        return blockingResultInfo.getNumPartitions();
    }

    public int getNumSubpartitions(int partitionIndex) {
        return blockingResultInfo.getNumSubpartitions(partitionIndex);
    }

    public int getIndex() {
        return index;
    }

    public boolean isSplittable() {
        return isSplittable;
    }

    @Override
    public int compareTo(BlockingInputInfo o) {
        return Integer.compare(o.getIndex(), this.getIndex());
    }
}
