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

import com.google.common.base.Preconditions;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import okhttp3.Interceptor;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.AzureBlobFileSystemException;
import org.apache.hadoop.fs.azurebfs.contracts.services.AbfsHttpAuthorizationService;
import org.apache.hadoop.fs.azurebfs.contracts.services.AbfsHttpClientSession;
import org.apache.hadoop.fs.azurebfs.contracts.services.AbfsInterceptorFactory;
import org.apache.hadoop.fs.azurebfs.contracts.services.AbfsThrottlingNetworkTrafficAnalysisService;
import org.apache.hadoop.fs.azurebfs.contracts.services.LoggingService;
import org.apache.hadoop.fs.azurebfs.contracts.services.TracingService;
import org.apache.hadoop.fs.azurebfs.contracts.services.AbfsRetryStrategy;

@Singleton
@InterfaceAudience.Private
@InterfaceStability.Evolving
final class AbfsInterceptorFactoryImpl implements AbfsInterceptorFactory {
  private final AbfsHttpAuthorizationService abfsHttpAuthorizationService;
  private final AbfsThrottlingNetworkTrafficAnalysisService abfsThrottlingNetworkTrafficAnalysisService;
  private final LoggingService loggingService;
  private final TracingService tracingService;

  @Inject
  AbfsInterceptorFactoryImpl(
      final AbfsHttpAuthorizationService abfsHttpAuthorizationService,
      final AbfsThrottlingNetworkTrafficAnalysisService abfsThrottlingNetworkTrafficAnalysisService,
      final LoggingService loggingService,
      final TracingService tracingService) {
    Preconditions.checkNotNull(abfsHttpAuthorizationService, "abfsHttpAuthorizationService");
    Preconditions.checkNotNull(abfsThrottlingNetworkTrafficAnalysisService, "abfsThrottlingNetworkTrafficAnalysisService");
    Preconditions.checkNotNull(loggingService, "loggingService");
    Preconditions.checkNotNull(tracingService, "tracingService");

    this.abfsHttpAuthorizationService = abfsHttpAuthorizationService;
    this.abfsThrottlingNetworkTrafficAnalysisService = abfsThrottlingNetworkTrafficAnalysisService;
    this.loggingService = loggingService.get(AbfsInterceptorFactory.class);
    this.tracingService = tracingService;
  }

  @Override
  public Interceptor createNetworkAuthenticationProxy(final AbfsHttpClientSession abfsHttpClientSession) throws AzureBlobFileSystemException {
    return new NetworkInterceptorImpl(
        abfsHttpClientSession,
        this.abfsHttpAuthorizationService,
        this.loggingService,
        this.tracingService);
  }

  @Override
  public Interceptor createNetworkThrottler(final AbfsHttpClientSession abfsHttpClientSession) throws AzureBlobFileSystemException {
    return new NetworkThrottlerImpl(
        abfsHttpClientSession,
        this.abfsThrottlingNetworkTrafficAnalysisService,
        loggingService);
  }

  @Override
  public Interceptor createNetworkThroughputMonitor(final AbfsHttpClientSession abfsHttpClientSession) throws AzureBlobFileSystemException {
    return new NetworkThroughputMonitorImpl(
        abfsHttpClientSession,
        this.abfsThrottlingNetworkTrafficAnalysisService);
  }

  @Override
  public Interceptor createRetryInterceptor(final AbfsRetryStrategy abfsRetryStrategy) throws AzureBlobFileSystemException {
    return new NetworkRetryImpl(
        abfsRetryStrategy,
        this.loggingService);
  }
}