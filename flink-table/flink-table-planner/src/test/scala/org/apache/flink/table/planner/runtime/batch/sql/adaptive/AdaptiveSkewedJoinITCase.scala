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
package org.apache.flink.table.planner.runtime.batch.sql.adaptive

import org.apache.flink.table.api.config.OptimizerConfigOptions

import org.junit.jupiter.api.BeforeEach

/** IT cases for adaptive broadcast join. */
class AdaptiveSkewedJoinITCase extends AdaptiveJoinITCase {
  @BeforeEach
  override def before(): Unit = {
    super.before()
    tEnv.getConfig
      .set(
        OptimizerConfigOptions.TABLE_OPTIMIZER_ADAPTIVE_BROADCAST_JOIN_STRATEGY,
        OptimizerConfigOptions.AdaptiveBroadcastJoinStrategy.NONE)
  }

  override def checkResult(sql: String): Unit = {
    tEnv.getConfig
      .set(
        OptimizerConfigOptions.TABLE_OPTIMIZER_ADAPTIVE_SKEWED_JOIN_OPTIMIZATION_STRATEGY,
        OptimizerConfigOptions.AdaptiveSkewedJoinOptimizationStrategy.NONE)
    val expected = executeQuery(sql)
    tEnv.getConfig
      .set(
        OptimizerConfigOptions.TABLE_OPTIMIZER_ADAPTIVE_SKEWED_JOIN_OPTIMIZATION_STRATEGY,
        OptimizerConfigOptions.AdaptiveSkewedJoinOptimizationStrategy.AUTO)
    checkResult(sql, expected)
    tEnv.getConfig
      .set(
        OptimizerConfigOptions.TABLE_OPTIMIZER_ADAPTIVE_SKEWED_JOIN_OPTIMIZATION_STRATEGY,
        OptimizerConfigOptions.AdaptiveSkewedJoinOptimizationStrategy.FORCED
      )
    checkResult(sql, expected)
  }
}
