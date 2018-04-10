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
import org.apache.hadoop.fs.azuredfs.contracts.services.AdfsInterceptorFactory;
import org.apache.hadoop.fs.azuredfs.contracts.services.AdfsThrottlingNetworkTrafficAnalysisService;
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
  private final AdfsInterceptorFactory adfsInterceptorFactory;
  private final AdfsThrottlingNetworkTrafficAnalysisService adfsThrottlingNetworkTrafficAnalysisService;
  private final LoggingService loggingService;
  private final boolean autoThrottleEnabled;

  @Inject
  AdfsHttpClientFactoryImpl(
      final ConfigurationService configurationService,
      final AdfsHttpClientSessionFactory adfsHttpClientSessionFactory,
      final AdfsThrottlingNetworkTrafficAnalysisService adfsThrottlingNetworkTrafficAnalysisService,
      final AdfsRetryStrategyFactory adfsRetryStrategyFactory,
      final AdfsInterceptorFactory adfsInterceptorFactory,
      final LoggingService loggingService) {

    Preconditions.checkNotNull(configurationService, "configurationService");
    Preconditions.checkNotNull(adfsHttpClientSessionFactory, "adfsHttpClientSessionFactory");
    Preconditions.checkNotNull(adfsRetryStrategyFactory, "adfsRetryStrategyFactory");
    Preconditions.checkNotNull(adfsInterceptorFactory, "adfsInterceptorFactory");
    Preconditions.checkNotNull(adfsThrottlingNetworkTrafficAnalysisService, "adfsThrottlingNetworkTrafficAnalysisService");
    Preconditions.checkNotNull(loggingService, "loggingService");

    this.configurationService = configurationService;
    this.adfsHttpClientSessionFactory = adfsHttpClientSessionFactory;
    this.adfsRetryStrategyFactory = adfsRetryStrategyFactory;
    this.adfsInterceptorFactory = adfsInterceptorFactory;
    this.adfsThrottlingNetworkTrafficAnalysisService = adfsThrottlingNetworkTrafficAnalysisService;
    this.loggingService = loggingService.get(AdfsHttpClientFactory.class);
    this.autoThrottleEnabled = this.configurationService.getConfiguration().getBoolean(ConfigurationKeys.FS_AZURE_AUTOTHROTTLING_ENABLE, true);
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
        this.adfsInterceptorFactory.createNetworkAuthenticationProxy(adfsHttpClientSession);

    final Interceptor retryInterceptor =
        this.adfsInterceptorFactory.createRetryInterceptor(this.adfsRetryStrategyFactory.create());

    if (autoThrottleEnabled) {
      this.loggingService.debug(
          "Auto throttle is enabled, creating AdfsMonitoredHttpClientImpl for filesystem: {0}", adfsHttpClientSession.getFileSystem());

      final Interceptor networkThrottler =
          this.adfsInterceptorFactory.createNetworkThrottler(adfsHttpClientSession);

      final Interceptor networkThroughputMonitor =
          this.adfsInterceptorFactory.createNetworkThroughputMonitor(adfsHttpClientSession);

      return new AdfsMonitoredHttpClientImpl(
          uriBuilder.toString(),
          configurationService,
          networkInterceptor,
          networkThrottler,
          networkThroughputMonitor,
          retryInterceptor,
          adfsHttpClientSession,
          adfsThrottlingNetworkTrafficAnalysisService,
          loggingService);
    }

    this.loggingService.debug(
        "Auto throttle is disabled, creating AdfsUnMonitoredHttpClientImpl for filesystem: {0}", adfsHttpClientSession.getFileSystem());

    return new AdfsUnMonitoredHttpClientImpl(
        uriBuilder.toString(),
        configurationService,
        networkInterceptor,
        retryInterceptor,
        adfsHttpClientSession,
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