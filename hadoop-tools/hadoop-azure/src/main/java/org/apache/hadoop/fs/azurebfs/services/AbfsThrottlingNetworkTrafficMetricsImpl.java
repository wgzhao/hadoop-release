/**
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

package org.apache.hadoop.fs.azurebfs.services;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.azurebfs.contracts.services.AbfsThrottlingNetworkThroughputMetrics;
import org.apache.hadoop.fs.azurebfs.contracts.services.AbfsThrottlingNetworkTrafficMetrics;

@InterfaceAudience.Private
@InterfaceStability.Evolving
final class AbfsThrottlingNetworkTrafficMetricsImpl implements AbfsThrottlingNetworkTrafficMetrics {
  private long endTime;
  private final long startTime;
  private final AbfsThrottlingNetworkThroughputMetrics writeNetworkThroughputMetrics;
  private final AbfsThrottlingNetworkThroughputMetrics readNetworkThroughputMetrics;

  AbfsThrottlingNetworkTrafficMetricsImpl(long startTime) {
    this.startTime = startTime;

    this.writeNetworkThroughputMetrics = new AbfsThrottlingNetworkTrafficThroughputMetricsImpl();
    this.readNetworkThroughputMetrics = new AbfsThrottlingNetworkTrafficThroughputMetricsImpl();
  }

  @Override
  public AbfsThrottlingNetworkThroughputMetrics getWriteMetrics() {
    return this.writeNetworkThroughputMetrics;
  }

  @Override
  public AbfsThrottlingNetworkThroughputMetrics getReadMetrics() {
    return this.readNetworkThroughputMetrics;
  }

  @Override
  public long getStartTime() {
    return startTime;
  }

  @Override
  public long getEndTime() {
    return endTime;
  }

  @Override
  public synchronized void end() {
    if (endTime != 0) {
      return;
    }

    endTime = System.currentTimeMillis();
  }
}