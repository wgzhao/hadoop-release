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

import com.google.common.base.Preconditions;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import okhttp3.Interceptor;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.azuredfs.contracts.exceptions.AzureDistributedFileSystemException;
import org.apache.hadoop.fs.azuredfs.contracts.services.AdfsHttpAuthorizationService;
import org.apache.hadoop.fs.azuredfs.contracts.services.AdfsHttpClientSession;
import org.apache.hadoop.fs.azuredfs.contracts.services.AdfsNetworkTrafficAnalysisService;
import org.apache.hadoop.fs.azuredfs.contracts.services.AdfsNetworkInterceptorFactory;
import org.apache.hadoop.fs.azuredfs.contracts.services.LoggingService;
import org.apache.hadoop.fs.azuredfs.contracts.services.TracingService;

@Singleton
@InterfaceAudience.Private
@InterfaceStability.Evolving
final class AdfsNetworkInterceptorFactoryImpl implements AdfsNetworkInterceptorFactory {
  private final AdfsHttpAuthorizationService adfsHttpAuthorizationService;
  private final AdfsNetworkTrafficAnalysisService adfsNetworkTrafficAnalysisService;
  private final LoggingService loggingService;
  private final TracingService tracingService;

  @Inject
  AdfsNetworkInterceptorFactoryImpl(
      final AdfsHttpAuthorizationService adfsHttpAuthorizationService,
      final AdfsNetworkTrafficAnalysisService adfsNetworkTrafficAnalysisService,
      final LoggingService loggingService,
      final TracingService tracingService) {
    Preconditions.checkNotNull(adfsHttpAuthorizationService, "adfsHttpAuthorizationService");
    Preconditions.checkNotNull(adfsNetworkTrafficAnalysisService, "adfsNetworkTrafficAnalysisService");
    Preconditions.checkNotNull(loggingService, "loggingService");
    Preconditions.checkNotNull(tracingService, "tracingService");

    this.adfsHttpAuthorizationService = adfsHttpAuthorizationService;
    this.adfsNetworkTrafficAnalysisService = adfsNetworkTrafficAnalysisService;
    this.loggingService = loggingService.get(AdfsNetworkInterceptorFactoryImpl.class);
    this.tracingService = tracingService;
  }

  @Override
  public Interceptor createNetworkAuthenticationProxy(final AdfsHttpClientSession adfsHttpClientSession) throws AzureDistributedFileSystemException {
    return new NetworkInterceptorImpl(
        adfsHttpClientSession,
        this.adfsHttpAuthorizationService,
        this.loggingService,
        this.tracingService);
  }

  @Override
  public Interceptor createNetworkThrottler(final AdfsHttpClientSession adfsHttpClientSession) throws AzureDistributedFileSystemException {
    return new NetworkThrottlerImpl(
        adfsHttpClientSession,
        this.adfsNetworkTrafficAnalysisService,
        loggingService);
  }

  @Override
  public Interceptor createNetworkThroughputMonitor(final AdfsHttpClientSession adfsHttpClientSession) throws AzureDistributedFileSystemException {
    NetworkThroughputMonitorImpl networkThroughputMonitor = new NetworkThroughputMonitorImpl(
        adfsHttpClientSession,
        this.adfsNetworkTrafficAnalysisService);
    return networkThroughputMonitor;
  }
}