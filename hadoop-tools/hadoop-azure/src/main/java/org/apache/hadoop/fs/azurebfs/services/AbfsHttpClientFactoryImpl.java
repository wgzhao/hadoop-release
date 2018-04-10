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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import okhttp3.Interceptor;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys;
import org.apache.hadoop.fs.azurebfs.constants.FileSystemUriSchemes;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.AzureBlobFileSystemException;
import org.apache.hadoop.fs.azurebfs.contracts.services.AbfsHttpClient;
import org.apache.hadoop.fs.azurebfs.contracts.services.AbfsHttpClientFactory;
import org.apache.hadoop.fs.azurebfs.contracts.services.AbfsHttpClientSession;
import org.apache.hadoop.fs.azurebfs.contracts.services.AbfsHttpClientSessionFactory;
import org.apache.hadoop.fs.azurebfs.contracts.services.AbfsInterceptorFactory;
import org.apache.hadoop.fs.azurebfs.contracts.services.AbfsRetryStrategyFactory;
import org.apache.hadoop.fs.azurebfs.contracts.services.AbfsThrottlingNetworkTrafficAnalysisService;
import org.apache.hadoop.fs.azurebfs.contracts.services.ConfigurationService;
import org.apache.hadoop.fs.azurebfs.contracts.services.LoggingService;
import org.apache.http.client.utils.URIBuilder;

@Singleton
@InterfaceAudience.Private
@InterfaceStability.Evolving
class AbfsHttpClientFactoryImpl implements AbfsHttpClientFactory {
  private final ConfigurationService configurationService;
  private final AbfsHttpClientSessionFactory abfsHttpClientSessionFactory;
  private final AbfsRetryStrategyFactory abfsRetryStrategyFactory;
  private final AbfsInterceptorFactory abfsInterceptorFactory;
  private final AbfsThrottlingNetworkTrafficAnalysisService abfsThrottlingNetworkTrafficAnalysisService;
  private final LoggingService loggingService;
  private final boolean autoThrottleEnabled;

  @Inject
  AbfsHttpClientFactoryImpl(
      final ConfigurationService configurationService,
      final AbfsHttpClientSessionFactory abfsHttpClientSessionFactory,
      final AbfsThrottlingNetworkTrafficAnalysisService abfsThrottlingNetworkTrafficAnalysisService,
      final AbfsRetryStrategyFactory abfsRetryStrategyFactory,
      final AbfsInterceptorFactory abfsInterceptorFactory,
      final LoggingService loggingService) {

    Preconditions.checkNotNull(configurationService, "configurationService");
    Preconditions.checkNotNull(abfsHttpClientSessionFactory, "abfsHttpClientSessionFactory");
    Preconditions.checkNotNull(abfsRetryStrategyFactory, "abfsRetryStrategyFactory");
    Preconditions.checkNotNull(abfsInterceptorFactory, "abfsInterceptorFactory");
    Preconditions.checkNotNull(abfsThrottlingNetworkTrafficAnalysisService, "abfsThrottlingNetworkTrafficAnalysisService");
    Preconditions.checkNotNull(loggingService, "loggingService");

    this.configurationService = configurationService;
    this.abfsHttpClientSessionFactory = abfsHttpClientSessionFactory;
    this.abfsRetryStrategyFactory = abfsRetryStrategyFactory;
    this.abfsInterceptorFactory = abfsInterceptorFactory;
    this.abfsThrottlingNetworkTrafficAnalysisService = abfsThrottlingNetworkTrafficAnalysisService;
    this.loggingService = loggingService.get(AbfsHttpClientFactory.class);
    this.autoThrottleEnabled = this.configurationService.getConfiguration().getBoolean(ConfigurationKeys.FS_AZURE_AUTOTHROTTLING_ENABLE, true);
  }

  @Override
  public AbfsHttpClient create(final FileSystem fs) throws AzureBlobFileSystemException {
    this.loggingService.debug(
        "Creating AbfsHttpClient for filesystem: {0}", fs.getUri());

    final AbfsHttpClientSession abfsHttpClientSession =
        this.abfsHttpClientSessionFactory.create(fs);

    final URIBuilder uriBuilder =
        getURIBuilder(abfsHttpClientSession.getHostName());

    final Interceptor networkInterceptor =
        this.abfsInterceptorFactory.createNetworkAuthenticationProxy(abfsHttpClientSession);

    final Interceptor retryInterceptor =
        this.abfsInterceptorFactory.createRetryInterceptor(this.abfsRetryStrategyFactory.create());

    if (autoThrottleEnabled) {
      this.loggingService.debug(
          "Auto throttle is enabled, creating AbfsMonitoredHttpClientImpl for filesystem: {0}", abfsHttpClientSession.getFileSystem());

      final Interceptor networkThrottler =
          this.abfsInterceptorFactory.createNetworkThrottler(abfsHttpClientSession);

      final Interceptor networkThroughputMonitor =
          this.abfsInterceptorFactory.createNetworkThroughputMonitor(abfsHttpClientSession);

      return new AbfsMonitoredHttpClientImpl(
          uriBuilder.toString(),
          configurationService,
          networkInterceptor,
          networkThrottler,
          networkThroughputMonitor,
          retryInterceptor,
          abfsHttpClientSession,
          abfsThrottlingNetworkTrafficAnalysisService,
          loggingService);
    }

    this.loggingService.debug(
        "Auto throttle is disabled, creating AbfsUnMonitoredHttpClientImpl for filesystem: {0}", abfsHttpClientSession.getFileSystem());

    return new AbfsUnMonitoredHttpClientImpl(
        uriBuilder.toString(),
        configurationService,
        networkInterceptor,
        retryInterceptor,
        abfsHttpClientSession,
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