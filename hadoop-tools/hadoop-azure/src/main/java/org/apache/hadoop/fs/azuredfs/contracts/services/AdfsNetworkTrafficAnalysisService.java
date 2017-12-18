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

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

/**
 * AdfsNetworkTrafficAnalysisService responsible for http client session traffic analysis.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public interface AdfsNetworkTrafficAnalysisService extends InjectableService {
  /**
   * Subscribes an adfsHttpClientSession to be analyzed by AnalysisService.
   * @param adfsHttpClientSession
   */
  void subscribeForAnalysis(AdfsHttpClientSession adfsHttpClientSession);

  /**
   * Subscribes an adfsHttpClientSession to be analyzed by AnalysisService.
   * @param adfsHttpClientSession
   * @param analysisFrequencyInMs The frequency, in milliseconds, at which metrics are analyzed.
   */
  void subscribeForAnalysis(AdfsHttpClientSession adfsHttpClientSession, int analysisFrequencyInMs);

  /**
   * Unsubscribes an adfsHttpClientSession from AnalysisService analysis cycles.
   * @param adfsHttpClientSession
   */
  void unsubscribeFromAnalysis(AdfsHttpClientSession adfsHttpClientSession);

  /**
   * Gets cached network throughput metrics for a specific adfsHttpClientSession.
   * @param adfsHttpClientSession
   * @return cached network throughput metrics for a specific adfsHttpClientSession.
   */
  AdfsNetworkTrafficMetrics getAdfsNetworkThroughputMetrics(AdfsHttpClientSession adfsHttpClientSession);

  /**
   * Gets cached network throughput analysis result for a specific adfsHttpClientSession.
   * @param adfsHttpClientSession
   * @return cached network throughput analysis result for a specific adfsHttpClientSession.
   */
  AdfsNetworkTrafficAnalysisResult getAdfsNetworkTrafficAnalysisResult(AdfsHttpClientSession adfsHttpClientSession);
}