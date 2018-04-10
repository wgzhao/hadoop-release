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

package org.apache.hadoop.fs.azurebfs.contracts.services;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

/**
 * AbfsThrottlingNetworkThroughputAnalysisResult stores Network throughput analysis result metadata.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public interface AbfsThrottlingNetworkThroughputAnalysisResult {
  /**
   * Getter for calculated sleep duration.
   * @return sleep duration value.
   */
  int getSleepDuration();

  /**
   * Setter for calculated sleep duration.
   * @param value
   */
  void setSleepDuration(int value);

  /**
   * Resets the number of consecutive no error counter.
   */
  void resetConsecutiveNoErrorCount();

  /**
   * Gets the number of consecutive no error counter.
   * @return the number of consecutive no error counter.
   */
  long getConsecutiveNoErrorCount();

  /**
   * Increments the number of consecutive no error counter.
   * @return the number of consecutive no error counter.
   */
  long incrementConsecutiveNoErrorCount();
}