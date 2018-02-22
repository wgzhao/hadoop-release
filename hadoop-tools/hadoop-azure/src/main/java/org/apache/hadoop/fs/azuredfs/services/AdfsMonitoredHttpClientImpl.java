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

import java.util.concurrent.TimeUnit;

import com.google.common.base.Preconditions;
import com.microsoft.rest.RestClient;
import com.microsoft.rest.ServiceResponseBuilder;
import com.microsoft.rest.retry.RetryStrategy;
import com.microsoft.rest.serializer.JacksonAdapter;
import okhttp3.Interceptor;
import okhttp3.OkHttpClient;
import retrofit2.Retrofit;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.azuredfs.constants.FileSystemConfigurations;
import org.apache.hadoop.fs.azuredfs.contracts.exceptions.TimeoutException;
import org.apache.hadoop.fs.azuredfs.contracts.services.AdfsHttpClientSession;
import org.apache.hadoop.fs.azuredfs.contracts.services.AdfsNetworkTrafficAnalysisService;
import org.apache.hadoop.fs.azuredfs.contracts.services.ConfigurationService;
import org.apache.hadoop.fs.azuredfs.contracts.services.LoggingService;

/**
 * Adfs monitored http client implementation.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
final class AdfsMonitoredHttpClientImpl extends AdfsHttpClientBaseImpl {
  private final AdfsNetworkTrafficAnalysisService adfsNetworkTrafficAnalysisService;
  public AdfsMonitoredHttpClientImpl(
      final String baseUrl,
      final ConfigurationService configurationService,
      final Interceptor networkInterceptor,
      final Interceptor networkThrottler,
      final Interceptor networkThroughputMonitor,
      final AdfsHttpClientSession adfsHttpClientSession,
      final AdfsNetworkTrafficAnalysisService adfsNetworkTrafficAnalysisService,
      final RetryStrategy retryStrategy,
      final LoggingService loggingService) {
    super(adfsHttpClientSession,
        loggingService,
        new RestClient.Builder(
            (new OkHttpClient.Builder())
                .writeTimeout(FileSystemConfigurations.FS_AZURE_DEFAULT_CONNECTION_WRITE_TIMEOUT, TimeUnit.SECONDS),
            new Retrofit.Builder())
        .withBaseUrl(baseUrl)
        .withNetworkInterceptor(networkInterceptor)
        .withNetworkInterceptor(networkThrottler)
        .withNetworkInterceptor(networkThroughputMonitor)
        .withResponseBuilderFactory(new ServiceResponseBuilder.Factory())
        .withSerializerAdapter(new JacksonAdapter())
        .withRetryStrategy(retryStrategy)
        .withMaxIdleConnections(configurationService.getMaxConcurrentReadThreads() + configurationService.getMaxConcurrentWriteThreads())
        .withConnectionTimeout(FileSystemConfigurations.FS_AZURE_DEFAULT_CONNECTION_TIMEOUT, TimeUnit.SECONDS)
        .withReadTimeout(FileSystemConfigurations.FS_AZURE_DEFAULT_CONNECTION_READ_TIMEOUT, TimeUnit.SECONDS)
        .build());

    Preconditions.checkNotNull(adfsNetworkTrafficAnalysisService, "adfsNetworkTrafficAnalysisService");
    this.adfsNetworkTrafficAnalysisService = adfsNetworkTrafficAnalysisService;
    this.adfsNetworkTrafficAnalysisService.subscribeForAnalysis(adfsHttpClientSession);
  }

  @Override
  public void close() throws TimeoutException {
    super.close();
    this.adfsNetworkTrafficAnalysisService.unsubscribeFromAnalysis(this.getSession());
  }
}