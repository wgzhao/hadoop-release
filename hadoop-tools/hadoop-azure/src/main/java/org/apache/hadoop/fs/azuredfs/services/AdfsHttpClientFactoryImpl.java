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

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.azuredfs.constants.FileSystemConfigurations;
import org.apache.hadoop.fs.azuredfs.constants.FileSystemUriSchemes;
import org.apache.hadoop.fs.azuredfs.contracts.exceptions.AzureDistributedFileSystemException;
import org.apache.hadoop.fs.azuredfs.contracts.services.AdfsHttpAuthorizationService;
import org.apache.hadoop.fs.azuredfs.contracts.services.AdfsHttpClientFactory;
import org.apache.hadoop.fs.azuredfs.contracts.services.AdfsHttpClientSession;
import org.apache.hadoop.fs.azuredfs.contracts.services.AdfsHttpClientSessionFactory;
import org.apache.hadoop.fs.azuredfs.contracts.services.AdfsHttpClient;
import org.apache.hadoop.fs.azuredfs.contracts.services.ConfigurationService;
import org.apache.http.client.utils.URIBuilder;

@Singleton
@InterfaceAudience.Private
@InterfaceStability.Evolving
class AdfsHttpClientFactoryImpl implements AdfsHttpClientFactory {
  private final ConfigurationService configurationService;
  private final AdfsHttpAuthorizationService adfsHttpAuthorizationService;
  private final AdfsHttpClientSessionFactory adfsHttpClientSessionFactory;

  @Inject
  AdfsHttpClientFactoryImpl(
      final ConfigurationService configurationService,
      final AdfsHttpClientSessionFactory adfsHttpClientSessionFactory,
      final AdfsHttpAuthorizationService adfsHttpAuthorizationService) {

    Preconditions.checkNotNull(configurationService, "configurationService");
    Preconditions.checkNotNull(adfsHttpAuthorizationService, "adfsHttpAuthorizationService");
    Preconditions.checkNotNull(adfsHttpClientSessionFactory, "adfsHttpClientSessionFactory");

    this.configurationService = configurationService;
    this.adfsHttpAuthorizationService = adfsHttpAuthorizationService;
    this.adfsHttpClientSessionFactory = adfsHttpClientSessionFactory;
  }

  @Override
  public AdfsHttpClient create(final FileSystem fs) throws AzureDistributedFileSystemException {
    final AdfsHttpClientSession adfsHttpClientSession =
        this.adfsHttpClientSessionFactory.create(fs);

    final URIBuilder uriBuilder =
        getURIBuilder(adfsHttpClientSession.getStorageCredentialsAccountAndKey().getAccountName());

    final NetworkInterceptorImpl networkInterceptor =
        new NetworkInterceptorImpl(
            adfsHttpClientSession.getStorageCredentialsAccountAndKey(),
            this.adfsHttpAuthorizationService);

    return new AdfsHttpClientImpl(
        uriBuilder.toString(),
        networkInterceptor,
        adfsHttpClientSession);
  }

  @VisibleForTesting
  URIBuilder getURIBuilder(final String accountName) {
    final boolean isSecure = this.configurationService.isSecureMode();

    String scheme = FileSystemUriSchemes.HTTP_SCHEME;

    if (isSecure) {
      scheme = FileSystemUriSchemes.HTTPS_SCHEME;
    }

    final URIBuilder uriBuilder = new URIBuilder();
    uriBuilder.setScheme(scheme);
    uriBuilder.setHost(accountName + FileSystemConfigurations.FS_AZURE_DEFAULT_HOST);

    return uriBuilder;
  }
}