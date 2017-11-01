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

import java.net.URI;
import java.net.URISyntaxException;

import com.google.common.base.Preconditions;
import com.google.inject.Inject;
import com.google.inject.Singleton;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.azuredfs.constants.FileSystemUriSchemes;
import org.apache.hadoop.fs.azuredfs.constants.TestConfigurationKeys;
import org.apache.hadoop.fs.azuredfs.contracts.services.ConfigurationService;
import org.apache.http.client.utils.URIBuilder;

@Singleton
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class MockAdfsBlobHandlerImpl extends AdfsBlobHandlerImpl {
  private final ConfigurationService configurationService;

  @Inject
  MockAdfsBlobHandlerImpl(
      final ConfigurationService configurationService) {
    Preconditions.checkNotNull(configurationService, "configurationService");
    this.configurationService = configurationService;
  }

  @Override
  public URI getBlobEndpointUriFromBlobUriAndAccountName(URI blobUri, String accountName) throws URISyntaxException {
    final URIBuilder uriBuilder = new URIBuilder();
    final String host = this.configurationService.getConfiguration().get(TestConfigurationKeys.FS_AZURE_TEST_HOST_NAME);

    String scheme = FileSystemUriSchemes.HTTP_SCHEME;

    uriBuilder.setScheme(scheme);
    uriBuilder.setHost(host);
    uriBuilder.setPort(8885);

    uriBuilder.setPath("/" + this.configurationService.getConfiguration().get(TestConfigurationKeys.FS_AZURE_TEST_ACCOUNT_NAME) + "/");
    return uriBuilder.build();
  }
}