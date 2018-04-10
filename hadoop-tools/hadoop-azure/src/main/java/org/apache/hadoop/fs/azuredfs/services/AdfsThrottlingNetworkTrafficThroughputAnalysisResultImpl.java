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

package org.apache.hadoop.fs.azuredfs.services;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import com.google.inject.Inject;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.azuredfs.contracts.services.AdfsThrottlingNetworkThroughputAnalysisResult;

@InterfaceAudience.Private
@InterfaceStability.Evolving
final class AdfsThrottlingNetworkTrafficThroughputAnalysisResultImpl implements AdfsThrottlingNetworkThroughputAnalysisResult {
  private final AtomicInteger sleepDuration;
  private final AtomicLong consecutiveNoErrorCount;

  @Inject
  AdfsThrottlingNetworkTrafficThroughputAnalysisResultImpl() {
    sleepDuration = new AtomicInteger();
    consecutiveNoErrorCount = new AtomicLong();
  }

  public void setSleepDuration(final int value) {
    sleepDuration.set(value);
  }

  public int getSleepDuration() {
    return sleepDuration.get();
  }

  public void resetConsecutiveNoErrorCount() {
    consecutiveNoErrorCount.set(0);
  }

  public long getConsecutiveNoErrorCount() {
    return consecutiveNoErrorCount.get();
  }

  public long incrementConsecutiveNoErrorCount() {
    return consecutiveNoErrorCount.incrementAndGet();
  }
}