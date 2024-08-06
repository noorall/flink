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

import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;

public class BlockingInputInfo {
    private final BlockingResultInfo blockingResultInfo;
    private final int inputTypeNumber;
    private final boolean existIntraInputCorrelation;
    private final boolean existInterInputsCorrelation;
    private final boolean useBalancedDistributionV2;

    public BlockingInputInfo(
            BlockingResultInfo blockingResultInfo,
            int inputTypeNumber,
            boolean existInterInputsCorrelation,
            boolean existIntraInputCorrelation,
            boolean useBalancedDistributionV2) {
        this.blockingResultInfo = blockingResultInfo;
        this.inputTypeNumber = inputTypeNumber;
        this.existInterInputsCorrelation = existInterInputsCorrelation;
        this.existIntraInputCorrelation = existIntraInputCorrelation;
        this.useBalancedDistributionV2 = useBalancedDistributionV2;
    }

    public BlockingResultInfo getConsumedResultInfo() {
        return blockingResultInfo;
    }

    public int getInputTypeNumber() {
        return inputTypeNumber;
    }

    public boolean existIntraInputCorrelation() {
        return existIntraInputCorrelation;
    }

    public boolean existInterInputsCorrelation() {
        return existInterInputsCorrelation;
    }

    public boolean useBalancedDistributionV2() {
        return useBalancedDistributionV2;
    }

    public boolean isPointWise() {
        return blockingResultInfo.isPointwise();
    }

    public boolean isBroadcast() {
        return blockingResultInfo.isBroadcast();
    }

    public int getNumPartitions() {
        return blockingResultInfo.getNumPartitions();
    }

    public int getNumSubpartitions(int partitionIndex) {
        return blockingResultInfo.getNumSubpartitions(partitionIndex);
    }

    public long getNumBytesProduced() {
        return blockingResultInfo.getNumBytesProduced();
    }

    public IntermediateDataSetID getResultId() {
        return blockingResultInfo.getResultId();
    }
}
