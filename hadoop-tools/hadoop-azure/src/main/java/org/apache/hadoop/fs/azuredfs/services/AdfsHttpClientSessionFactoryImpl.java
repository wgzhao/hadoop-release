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

import com.google.common.base.Preconditions;
import com.google.inject.Inject;
import com.google.inject.Singleton;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.azuredfs.constants.FileSystemUriSchemes;
import org.apache.hadoop.fs.azuredfs.contracts.exceptions.AzureDistributedFileSystemException;
import org.apache.hadoop.fs.azuredfs.contracts.exceptions.InvalidUriAuthorityException;
import org.apache.hadoop.fs.azuredfs.contracts.exceptions.InvalidUriException;
import org.apache.hadoop.fs.azuredfs.contracts.services.AdfsHttpClientSession;
import org.apache.hadoop.fs.azuredfs.contracts.services.AdfsHttpClientSessionFactory;
import org.apache.hadoop.fs.azuredfs.contracts.services.ConfigurationService;
import org.apache.hadoop.fs.azuredfs.contracts.services.LoggingService;

@Singleton
@InterfaceAudience.Private
@InterfaceStability.Evolving
final class AdfsHttpClientSessionFactoryImpl implements AdfsHttpClientSessionFactory {
  private static final String AZURE_DISTRIBUTED_FILE_SYSTEM_AUTHORITY_DELIMITER = "@";
  private final ConfigurationService configurationService;
  private final LoggingService loggingService;

  @Inject
  AdfsHttpClientSessionFactoryImpl(
      final ConfigurationService configurationService,
      final LoggingService loggingService) {
    Preconditions.checkNotNull(configurationService, "configurationService");
    Preconditions.checkNotNull(loggingService, "loggingService");
    this.configurationService = configurationService;
    this.loggingService = loggingService.get(AdfsHttpClientSessionFactory.class);
  }

  @Override
  public AdfsHttpClientSession create(final FileSystem fs) throws AzureDistributedFileSystemException {
    this.loggingService.debug(
        "Creating AdfsHttpClientSession for filesystem: {0}", fs.getUri());

    final URI uri = fs.getUri();
    final String authority = uri.getRawAuthority();
    if (null == authority) {
      throw new InvalidUriAuthorityException(uri.toString());
    }

    if (!authority.contains(AZURE_DISTRIBUTED_FILE_SYSTEM_AUTHORITY_DELIMITER)) {
      throw new InvalidUriAuthorityException(uri.toString());
    }

    final String[] authorityParts = authority.split(AZURE_DISTRIBUTED_FILE_SYSTEM_AUTHORITY_DELIMITER, 2);

    if (authorityParts.length < 2 || "".equals(authorityParts[0])) {
      final String errMsg = String
          .format("URI '%s' has a malformed authority, expected container name. "
                  + "Authority takes the form "+ FileSystemUriSchemes.ADFS_SCHEME + "://[<container name>@]<account name>",
              uri.toString());
      throw new InvalidUriException(errMsg);
    }

    String fileSystemName = authorityParts[0];
    String accountName = authorityParts[1];

    return new AdfsHttpClientSessionImpl(
        accountName,
        this.configurationService.getStorageAccountKey(accountName),
        fileSystemName);
  }
}