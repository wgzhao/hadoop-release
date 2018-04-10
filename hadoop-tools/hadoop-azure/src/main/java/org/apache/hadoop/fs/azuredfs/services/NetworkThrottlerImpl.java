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

import java.io.IOException;

import com.google.common.base.Preconditions;
import okhttp3.Interceptor;
import okhttp3.Request;
import okhttp3.Response;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.azuredfs.contracts.services.AdfsHttpClientSession;
import org.apache.hadoop.fs.azuredfs.contracts.services.AdfsThrottlingNetworkThroughputAnalysisResult;
import org.apache.hadoop.fs.azuredfs.contracts.services.AdfsThrottlingNetworkTrafficAnalysisResult;
import org.apache.hadoop.fs.azuredfs.contracts.services.AdfsThrottlingNetworkTrafficAnalysisService;
import org.apache.hadoop.fs.azuredfs.contracts.services.LoggingService;
import org.apache.hadoop.fs.azuredfs.utils.NetworkUtils;

@InterfaceAudience.Private
@InterfaceStability.Evolving
final class NetworkThrottlerImpl implements Interceptor {
  private final AdfsHttpClientSession adfsHttpClientSession;
  private final AdfsThrottlingNetworkTrafficAnalysisService adfsThrottlingNetworkTrafficAnalysisService;
  private final LoggingService loggingService;

  NetworkThrottlerImpl(
      final AdfsHttpClientSession adfsHttpClientSession,
      final AdfsThrottlingNetworkTrafficAnalysisService adfsThrottlingNetworkTrafficAnalysisService,
      final LoggingService loggingService) {
    Preconditions.checkNotNull(adfsThrottlingNetworkTrafficAnalysisService, "adfsThrottlingNetworkTrafficAnalysisService");
    Preconditions.checkNotNull(adfsHttpClientSession, "adfsHttpClientSession");
    Preconditions.checkNotNull(loggingService, "loggingService");

    this.adfsThrottlingNetworkTrafficAnalysisService = adfsThrottlingNetworkTrafficAnalysisService;
    this.adfsHttpClientSession = adfsHttpClientSession;
    this.loggingService = loggingService.get(NetworkThrottlerImpl.class);
  }

  @Override
  public Response intercept(final Chain chain)
      throws IOException {

    final Request request = chain.request();

    final AdfsThrottlingNetworkTrafficAnalysisResult adfsThrottlingNetworkTrafficAnalysisResult =
        this.adfsThrottlingNetworkTrafficAnalysisService.getAdfsThrottlingNetworkTrafficAnalysisResult(this.adfsHttpClientSession);

    final AdfsThrottlingNetworkThroughputAnalysisResult throttlingNetworkThroughputAnalysisResult;

    if (NetworkUtils.isWriteRequest(request)) {
      throttlingNetworkThroughputAnalysisResult = adfsThrottlingNetworkTrafficAnalysisResult.getWriteAnalysisResult();
    }
    else {
      throttlingNetworkThroughputAnalysisResult = adfsThrottlingNetworkTrafficAnalysisResult.getReadAnalysisResult();
    }

    final int sleepDuration = throttlingNetworkThroughputAnalysisResult.getSleepDuration();
    if (sleepDuration > 0) {
      try{
        this.loggingService.debug(
            "Request {0} must be throttled for {1} ms.", request.toString(), sleepDuration);
        Thread.sleep(sleepDuration);
      }
      catch (InterruptedException ex) {
        Thread.currentThread().interrupt();
      }
    }

    return chain.proceed(request);
  }
}