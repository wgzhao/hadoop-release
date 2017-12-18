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


package org.apache.hadoop.fs.azuredfs.contracts.services;

import java.util.concurrent.atomic.AtomicLong;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

/**
 * AdfsNetworkThroughputMetrics stores network throughput metrics.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public interface AdfsNetworkThroughputMetrics {
  /**
   * Updates metrics with results from the current storage operation.
   * @param count The count of bytes transferred.
   * @param isFailedOperation True if the operation failed; otherwise false.
   */
  void addBytesTransferred(long count, boolean isFailedOperation);

  /**
   * Gets number of communicated bytes for failed connections.
   * @return number of communicated bytes for a failed connection.
   */
  AtomicLong getBytesFailed();

  /**
   * Gets number of communicated bytes for successful connections.
   * @return number of communicated bytes for successful connections.
   */
  AtomicLong getBytesSuccessful();

  /**
   * Gets number of failed operations.
   * @return number of failed operations.
   */
  AtomicLong getOperationsFailed();

  /**
   * Gets number of successful operations.
   * @return number of successful operations.
   */
  AtomicLong getOperationsSuccessful();
}