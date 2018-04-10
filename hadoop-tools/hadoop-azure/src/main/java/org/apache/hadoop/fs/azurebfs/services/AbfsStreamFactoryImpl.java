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

import java.io.InputStream;
import java.io.OutputStream;

import com.google.common.base.Preconditions;
import com.google.inject.Inject;
import com.google.inject.Singleton;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.azurebfs.AzureBlobFileSystem;
import org.apache.hadoop.fs.azurebfs.contracts.services.AbfsBufferPool;
import org.apache.hadoop.fs.azurebfs.contracts.services.AbfsHttpService;
import org.apache.hadoop.fs.azurebfs.contracts.services.AbfsStatisticsService;
import org.apache.hadoop.fs.azurebfs.contracts.services.AbfsStreamFactory;
import org.apache.hadoop.fs.azurebfs.contracts.services.ConfigurationService;
import org.apache.hadoop.fs.azurebfs.contracts.services.LoggingService;
import org.apache.hadoop.fs.azurebfs.contracts.services.TracingService;

@Singleton
@InterfaceAudience.Private
@InterfaceStability.Evolving
class AbfsStreamFactoryImpl implements AbfsStreamFactory {
  private final AbfsHttpService abfsHttpService;
  private final AbfsBufferPool abfsBufferPool;
  private final ConfigurationService configurationService;
  private final AbfsStatisticsService abfsStatisticsService;
  private final TracingService tracingService;
  private final LoggingService loggingService;

  @Inject
  AbfsStreamFactoryImpl(
      final ConfigurationService configurationService,
      final AbfsHttpService abfsHttpService,
      final AbfsBufferPool abfsBufferPool,
      final AbfsStatisticsService abfsStatisticsService,
      final LoggingService loggingService,
      final TracingService tracingService) {
    Preconditions.checkNotNull(abfsHttpService, "abfsHttpService");
    Preconditions.checkNotNull(abfsBufferPool, "abfsBufferPool");
    Preconditions.checkNotNull(abfsStatisticsService, "abfsStatisticsService");
    Preconditions.checkNotNull(configurationService, "configurationService");
    Preconditions.checkNotNull(loggingService, "loggingService");
    Preconditions.checkNotNull(tracingService, "tracingService");

    this.abfsHttpService = abfsHttpService;
    this.abfsBufferPool = abfsBufferPool;
    this.abfsStatisticsService = abfsStatisticsService;
    this.configurationService = configurationService;
    this.loggingService = loggingService.get(AbfsStreamFactory.class);
    this.tracingService = tracingService;
  }

  @Override
  public InputStream createReadStream(
    final AzureBlobFileSystem azureBlobFileSystem,
    final Path path,
    final long fileLength,
    final String version) {
    return new AbfsInputStream(
        this.abfsBufferPool,
        this.abfsHttpService,
        azureBlobFileSystem,
        abfsStatisticsService,
        tracingService,
        loggingService,
        path,
        fileLength,
        configurationService.getReadBufferSize(),
        version);
  }

  @Override
  public OutputStream createWriteStream(final AzureBlobFileSystem azureBlobFileSystem, final Path path, final long offset) {
    return new AbfsOutputStream(
        this.abfsHttpService,
        this.abfsBufferPool,
        azureBlobFileSystem,
        abfsStatisticsService,
        tracingService,
        loggingService,
        path,
        offset,
        configurationService.getWriteBufferSize());
  }
}