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
 * AbfsThrottlingNetworkTrafficAnalysisService responsible for http client session traffic analysis.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public interface AbfsThrottlingNetworkTrafficAnalysisService extends InjectableService {
  /**
   * Subscribes an abfsHttpClientSession to be analyzed by AnalysisService.
   * @param abfsHttpClientSession
   */
  void subscribeForAnalysis(AbfsHttpClientSession abfsHttpClientSession);

  /**
   * Subscribes an abfsHttpClientSession to be analyzed by AnalysisService.
   * @param abfsHttpClientSession
   * @param analysisFrequencyInMs The frequency, in milliseconds, at which metrics are analyzed.
   */
  void subscribeForAnalysis(AbfsHttpClientSession abfsHttpClientSession, int analysisFrequencyInMs);

  /**
   * Unsubscribes an abfsHttpClientSession from AnalysisService analysis cycles.
   * @param abfsHttpClientSession
   */
  void unsubscribeFromAnalysis(AbfsHttpClientSession abfsHttpClientSession);

  /**
   * Gets cached throttling network throughput metrics for a specific abfsHttpClientSession.
   * @param abfsHttpClientSession
   * @return cached throttling network throughput metrics for a specific abfsHttpClientSession.
   */
  AbfsThrottlingNetworkTrafficMetrics getAbfsThrottlingNetworkThroughputMetrics(AbfsHttpClientSession abfsHttpClientSession);

  /**
   * Gets cached throttling network throughput analysis result for a specific abfsHttpClientSession.
   * @param abfsHttpClientSession
   * @return cached throttling network throughput analysis result for a specific abfsHttpClientSession.
   */
  AbfsThrottlingNetworkTrafficAnalysisResult getAbfsThrottlingNetworkTrafficAnalysisResult(AbfsHttpClientSession abfsHttpClientSession);
}