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

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.azuredfs.contracts.services.AzureDistributedFileSystemClientFactory;
import org.apache.hadoop.fs.azuredfs.contracts.services.AzureDistributedFileSystemService;

@Singleton
@InterfaceAudience.Private
@InterfaceStability.Evolving
final class AzureDistributedFileSystemServiceImpl implements AzureDistributedFileSystemService {
  private final AzureDistributedFileSystemClient azureDistributedFileSystemClient;

  @Inject
  AzureDistributedFileSystemServiceImpl(
      final AzureDistributedFileSystemClientFactory azureDistributedFileSystemClientFactory) {

    Preconditions.checkNotNull("azureDistributedFileSystemClientFactory", azureDistributedFileSystemClientFactory);
    this.azureDistributedFileSystemClient = azureDistributedFileSystemClientFactory.create();
  }

  @Override
  public void createFilesystem(String filesystem, String resource) {
    this.azureDistributedFileSystemClient.createFilesystem(filesystem, resource);
  }
}