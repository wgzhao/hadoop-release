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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.microsoft.rest.retry.RetryStrategy;
import okhttp3.Interceptor;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.azuredfs.constants.ConfigurationKeys;
import org.apache.hadoop.fs.azuredfs.constants.FileSystemUriSchemes;
import org.apache.hadoop.fs.azuredfs.contracts.exceptions.AzureDistributedFileSystemException;
import org.apache.hadoop.fs.azuredfs.contracts.services.AdfsHttpClient;
import org.apache.hadoop.fs.azuredfs.contracts.services.AdfsHttpClientFactory;
import org.apache.hadoop.fs.azuredfs.contracts.services.AdfsHttpClientSession;
import org.apache.hadoop.fs.azuredfs.contracts.services.AdfsHttpClientSessionFactory;
import org.apache.hadoop.fs.azuredfs.contracts.services.AdfsNetworkInterceptorFactory;
import org.apache.hadoop.fs.azuredfs.contracts.services.AdfsNetworkTrafficAnalysisService;
import org.apache.hadoop.fs.azuredfs.contracts.services.AdfsRetryStrategyFactory;
import org.apache.hadoop.fs.azuredfs.contracts.services.ConfigurationService;
import org.apache.hadoop.fs.azuredfs.contracts.services.LoggingService;
import org.apache.http.client.utils.URIBuilder;

@Singleton
@InterfaceAudience.Private
@InterfaceStability.Evolving
class AdfsHttpClientFactoryImpl implements AdfsHttpClientFactory {
  private final ConfigurationService configurationService;
  private final AdfsHttpClientSessionFactory adfsHttpClientSessionFactory;
  private final AdfsRetryStrategyFactory adfsRetryStrategyFactory;
  private final AdfsNetworkInterceptorFactory adfsNetworkInterceptorFactory;
  private final AdfsNetworkTrafficAnalysisService adfsNetworkTrafficAnalysisService;
  private final LoggingService loggingService;
  private final boolean autoThrottleEnabled;

  @Inject
  AdfsHttpClientFactoryImpl(
      final ConfigurationService configurationService,
      final AdfsHttpClientSessionFactory adfsHttpClientSessionFactory,
      final AdfsNetworkTrafficAnalysisService adfsNetworkTrafficAnalysisService,
      final AdfsRetryStrategyFactory adfsRetryStrategyFactory,
      final AdfsNetworkInterceptorFactory adfsNetworkInterceptorFactory,
      final LoggingService loggingService) {

    Preconditions.checkNotNull(configurationService, "configurationService");
    Preconditions.checkNotNull(adfsHttpClientSessionFactory, "adfsHttpClientSessionFactory");
    Preconditions.checkNotNull(adfsRetryStrategyFactory, "adfsRetryStrategyFactory");
    Preconditions.checkNotNull(adfsNetworkInterceptorFactory, "adfsNetworkInterceptorFactory");
    Preconditions.checkNotNull(adfsNetworkTrafficAnalysisService, "adfsNetworkTrafficAnalysisService");
    Preconditions.checkNotNull(loggingService, "loggingService");

    this.configurationService = configurationService;
    this.adfsHttpClientSessionFactory = adfsHttpClientSessionFactory;
    this.adfsRetryStrategyFactory = adfsRetryStrategyFactory;
    this.adfsNetworkInterceptorFactory = adfsNetworkInterceptorFactory;
    this.adfsNetworkTrafficAnalysisService = adfsNetworkTrafficAnalysisService;
    this.loggingService = loggingService;
    this.autoThrottleEnabled = this.configurationService.getConfiguration().getBoolean(ConfigurationKeys.FS_AZURE_AUTOTHROTTLING_ENABLE, false);
  }

  @Override
  public AdfsHttpClient create(final FileSystem fs) throws AzureDistributedFileSystemException {
    this.loggingService.debug(
        "Creating AdfsHttpClient for filesystem: {0}", fs.getUri());

    final AdfsHttpClientSession adfsHttpClientSession =
        this.adfsHttpClientSessionFactory.create(fs);

    final URIBuilder uriBuilder =
        getURIBuilder(adfsHttpClientSession.getHostName());

    final Interceptor networkInterceptor =
        this.adfsNetworkInterceptorFactory.createNetworkAuthenticationProxy(adfsHttpClientSession);

    final RetryStrategy retryStrategy =
        this.adfsRetryStrategyFactory.create();

    if (autoThrottleEnabled) {
      this.loggingService.debug(
          "Auto throttle is enabled, creating AdfsMonitoredHttpClientImpl for filesystem: {0}", adfsHttpClientSession.getFileSystem());

      final Interceptor networkThrottler =
          this.adfsNetworkInterceptorFactory.createNetworkThrottler(adfsHttpClientSession);

      final Interceptor networkThroughputMonitor =
          this.adfsNetworkInterceptorFactory.createNetworkThroughputMonitor(adfsHttpClientSession);

      return new AdfsMonitoredHttpClientImpl(
          uriBuilder.toString(),
          configurationService,
          networkInterceptor,
          networkThrottler,
          networkThroughputMonitor,
          adfsHttpClientSession,
          adfsNetworkTrafficAnalysisService,
          retryStrategy,
          loggingService);
    }

    this.loggingService.debug(
        "Auto throttle is disabled, creating AdfsUnMonitoredHttpClientImpl for filesystem: {0}", adfsHttpClientSession.getFileSystem());

    return new AdfsUnMonitoredHttpClientImpl(
        uriBuilder.toString(),
        configurationService,
        networkInterceptor,
        adfsHttpClientSession,
        retryStrategy,
        loggingService);
  }

  @VisibleForTesting
  URIBuilder getURIBuilder(final String hostName) {
    final boolean isSecure = this.configurationService.isSecureMode();

    String scheme = FileSystemUriSchemes.HTTP_SCHEME;

    if (isSecure) {
      scheme = FileSystemUriSchemes.HTTPS_SCHEME;
    }

    final URIBuilder uriBuilder = new URIBuilder();
    uriBuilder.setScheme(scheme);
    uriBuilder.setHost(hostName);

    return uriBuilder;
  }
}