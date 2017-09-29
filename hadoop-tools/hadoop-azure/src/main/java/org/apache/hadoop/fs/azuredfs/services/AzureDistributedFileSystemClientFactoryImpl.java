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
import com.microsoft.azure.dfs.client.generated.AzureDistributedFileSystemClient;
import com.microsoft.azure.dfs.client.generated.implementation.AzureDistributedFileSystemClientImpl;
import com.microsoft.rest.RestClient;
import com.microsoft.rest.ServiceResponseBuilder;
import com.microsoft.rest.serializer.JacksonAdapter;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.azuredfs.constants.FileSystemConfigurations;
import org.apache.hadoop.fs.azuredfs.constants.FileSystemUriSchemes;
import org.apache.hadoop.fs.azuredfs.contracts.services.AzureAuthorizationService;
import org.apache.hadoop.fs.azuredfs.contracts.services.AzureDistributedFileSystemClientFactory;
import org.apache.hadoop.fs.azuredfs.contracts.services.ConfigurationService;
import org.apache.http.client.utils.URIBuilder;

@Singleton
@InterfaceAudience.Private
@InterfaceStability.Evolving
final class AzureDistributedFileSystemClientFactoryImpl implements AzureDistributedFileSystemClientFactory {
  private final ConfigurationService configurationService;
  private final AzureAuthorizationService azureAuthorizationService;

  @Inject
  AzureDistributedFileSystemClientFactoryImpl(
      final ConfigurationService configurationService,
      final AzureAuthorizationService azureAuthorizationService) {

    Preconditions.checkNotNull("configurationService", configurationService);
    Preconditions.checkNotNull("azureAuthorizationService", azureAuthorizationService);

    this.configurationService = configurationService;
    this.azureAuthorizationService = azureAuthorizationService;
  }

  @Override
  public AzureDistributedFileSystemClient create() {
    String accountName = this.configurationService.getStorageAccountName();
    boolean isSecure = this.configurationService.isSecureMode();

    String scheme = FileSystemUriSchemes.HTTP_SCHEME;

    if (isSecure) {
      scheme = FileSystemUriSchemes.HTTPS_SCHEME;
    }

    final URIBuilder uriBuilder = new URIBuilder();
    uriBuilder.setScheme(scheme);
    uriBuilder.setHost(accountName + FileSystemConfigurations.FS_AZURE_DEFAULT_HOST);

    final NetworkInterceptorImpl networkInterceptor = new NetworkInterceptorImpl(this.azureAuthorizationService);

    final RestClient restClient = new RestClient.Builder()
        .withBaseUrl(uriBuilder.toString())
        .withNetworkInterceptor(networkInterceptor)
        .withResponseBuilderFactory(new ServiceResponseBuilder.Factory())
        .withSerializerAdapter(new JacksonAdapter())
        .build();

    AzureDistributedFileSystemClientImpl azureDistributedFileSystemClientImpl = new AzureDistributedFileSystemClientImpl(restClient);
    return azureDistributedFileSystemClientImpl.withTimeout(FileSystemConfigurations.FS_AZURE_DEFAULT_CONNECTION_TIMEOUT);
  }
}