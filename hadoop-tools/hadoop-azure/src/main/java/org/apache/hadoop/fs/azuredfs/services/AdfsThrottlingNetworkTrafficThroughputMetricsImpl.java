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

import java.util.concurrent.atomic.AtomicLong;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.azuredfs.contracts.services.AdfsThrottlingNetworkThroughputMetrics;

@InterfaceAudience.Private
@InterfaceStability.Evolving
final class AdfsThrottlingNetworkTrafficThroughputMetricsImpl implements AdfsThrottlingNetworkThroughputMetrics {
  private AtomicLong bytesFailed;
  private AtomicLong bytesSuccessful;
  private AtomicLong operationsFailed;
  private AtomicLong operationsSuccessful;

  AdfsThrottlingNetworkTrafficThroughputMetricsImpl() {
    this.bytesFailed = new AtomicLong();
    this.bytesSuccessful = new AtomicLong();
    this.operationsFailed = new AtomicLong();
    this.operationsSuccessful = new AtomicLong();
  }

  @Override
  public void addBytesTransferred(final long count, final boolean isFailedOperation) {
    if (isFailedOperation) {
      this.bytesFailed.addAndGet(count);
      this.operationsFailed.incrementAndGet();
    } else {
      this.bytesSuccessful.addAndGet(count);
      this.operationsSuccessful.incrementAndGet();
    }
  }

  @Override
  public AtomicLong getBytesFailed() {
    return bytesFailed;
  }

  @Override
  public AtomicLong getBytesSuccessful() {
    return bytesSuccessful;
  }

  @Override
  public AtomicLong getOperationsFailed() {
    return operationsFailed;
  }

  @Override
  public AtomicLong getOperationsSuccessful() {
    return operationsSuccessful;
  }
}