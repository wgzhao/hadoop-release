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
import com.google.inject.Inject;
import com.google.inject.Singleton;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.azuredfs.constants.FileSystemUriSchemes;
import org.apache.hadoop.fs.azuredfs.constants.TestConfigurationKeys;
import org.apache.hadoop.fs.azuredfs.contracts.services.AdfsHttpClientSessionFactory;
import org.apache.hadoop.fs.azuredfs.contracts.services.AdfsNetworkTrafficAnalysisService;
import org.apache.hadoop.fs.azuredfs.contracts.services.AdfsNetworkInterceptorFactory;
import org.apache.hadoop.fs.azuredfs.contracts.services.AdfsRetryStrategyFactory;
import org.apache.hadoop.fs.azuredfs.contracts.services.ConfigurationService;
import org.apache.hadoop.fs.azuredfs.utils.UriUtils;
import org.apache.http.client.utils.URIBuilder;

@Singleton
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class MockAdfsHttpClientFactoryImpl extends AdfsHttpClientFactoryImpl {
  private final ConfigurationService configurationService;

  @Inject
  MockAdfsHttpClientFactoryImpl(
      final ConfigurationService configurationService,
      final AdfsHttpClientSessionFactory adfsHttpClientSessionFactory,
      final AdfsNetworkTrafficAnalysisService adfsNetworkTrafficAnalysisService,
      final AdfsRetryStrategyFactory adfsRetryStrategyFactory,
      final AdfsNetworkInterceptorFactory adfsNetworkInterceptorFactory) {
    super(configurationService, adfsHttpClientSessionFactory, adfsNetworkTrafficAnalysisService, adfsRetryStrategyFactory, adfsNetworkInterceptorFactory);

    this.configurationService = configurationService;
  }

  @VisibleForTesting
  URIBuilder getURIBuilder(final String hostName) {
    final URIBuilder uriBuilder = new URIBuilder();

    final String testHost = this.configurationService.getConfiguration().get(TestConfigurationKeys.FS_AZURE_TEST_HOST_NAME);
    final String testAccount = this.configurationService.getConfiguration().get(TestConfigurationKeys.FS_AZURE_TEST_ACCOUNT_NAME);

    String scheme = FileSystemUriSchemes.HTTP_SCHEME;

    uriBuilder.setScheme(scheme);
    uriBuilder.setHost(testHost);
    uriBuilder.setPort(8889);

    uriBuilder.setPath("/" + UriUtils.extractRawAccountFromAccountName(testAccount) + "/");

    return uriBuilder;
  }
}