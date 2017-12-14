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

import java.io.InputStream;
import java.io.OutputStream;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.inject.Inject;
import com.google.inject.Singleton;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.azuredfs.AzureDistributedFileSystem;
import org.apache.hadoop.fs.azuredfs.constants.ConfigurationKeys;
import org.apache.hadoop.fs.azuredfs.constants.FileSystemConfigurations;
import org.apache.hadoop.fs.azuredfs.contracts.services.AdfsBufferPool;
import org.apache.hadoop.fs.azuredfs.contracts.services.AdfsHttpService;
import org.apache.hadoop.fs.azuredfs.contracts.services.AdfsStreamFactory;
import org.apache.hadoop.fs.azuredfs.contracts.services.ConfigurationService;

@Singleton
@InterfaceAudience.Private
@InterfaceStability.Evolving
class AdfsStreamFactoryImpl implements AdfsStreamFactory {
  private final AdfsHttpService adfsHttpService;
  private final AdfsBufferPool adfsBufferPool;
  private final ConfigurationService configurationService;

  @Inject
  AdfsStreamFactoryImpl(
      final ConfigurationService configurationService,
      final AdfsHttpService adfsHttpService,
      final AdfsBufferPool adfsBufferPool) {
    Preconditions.checkNotNull(adfsHttpService, "adfsHttpService");
    Preconditions.checkNotNull(adfsBufferPool, "adfsBufferPool");
    Preconditions.checkNotNull(configurationService, "configurationService");

    this.adfsHttpService = adfsHttpService;
    this.adfsBufferPool = adfsBufferPool;
    this.configurationService = configurationService;
  }

  @Override
  public InputStream createReadStream(final AzureDistributedFileSystem azureDistributedFileSystem, final Path path, final long fileLength) {
    int bufferSize = getBufferSize(ConfigurationKeys.AZURE_READ_BUFFER_SIZE, FileSystemConfigurations.DEFAULT_READ_BUFFER_SIZE);
    return new AdfsInputStream(
        this.adfsHttpService,
        azureDistributedFileSystem,
        path,
        fileLength,
        bufferSize);
  }

  @Override
  public OutputStream createWriteStream(final AzureDistributedFileSystem azureDistributedFileSystem, final Path path, final long offset) {
    int bufferSize = getBufferSize(ConfigurationKeys.AZURE_WRITE_BUFFER_SIZE, FileSystemConfigurations.DEFAULT_WRITE_BUFFER_SIZE);
    return new AdfsOutputStream(
        this.adfsHttpService,
        this.adfsBufferPool,
        azureDistributedFileSystem,
        path,
        offset,
        bufferSize);
  }

  @VisibleForTesting
  int getBufferSize(final String userDefinedSize, final int defaulSize) {
    final int bufferSize = this.configurationService.getConfiguration().getInt(userDefinedSize, defaulSize);

    if (bufferSize < FileSystemConfigurations.MIN_BUFFER_SIZE) {
      return FileSystemConfigurations.MIN_BUFFER_SIZE;
    } else if (bufferSize > FileSystemConfigurations.MAX_BUFFER_SIZE) {
      return FileSystemConfigurations.MAX_BUFFER_SIZE;
    }

    return bufferSize;
  }
}