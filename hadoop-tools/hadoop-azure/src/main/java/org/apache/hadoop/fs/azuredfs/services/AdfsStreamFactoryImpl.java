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

import com.google.common.base.Preconditions;
import com.google.inject.Inject;
import com.google.inject.Singleton;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.azuredfs.AzureDistributedFileSystem;
import org.apache.hadoop.fs.azuredfs.contracts.services.AdfsBufferPool;
import org.apache.hadoop.fs.azuredfs.contracts.services.AdfsHttpService;
import org.apache.hadoop.fs.azuredfs.contracts.services.AdfsStatisticsService;
import org.apache.hadoop.fs.azuredfs.contracts.services.AdfsStreamFactory;
import org.apache.hadoop.fs.azuredfs.contracts.services.ConfigurationService;
import org.apache.hadoop.fs.azuredfs.contracts.services.LoggingService;
import org.apache.hadoop.fs.azuredfs.contracts.services.TracingService;

@Singleton
@InterfaceAudience.Private
@InterfaceStability.Evolving
class AdfsStreamFactoryImpl implements AdfsStreamFactory {
  private final AdfsHttpService adfsHttpService;
  private final AdfsBufferPool adfsBufferPool;
  private final ConfigurationService configurationService;
  private final AdfsStatisticsService adfsStatisticsService;
  private final TracingService tracingService;
  private final LoggingService loggingService;

  @Inject
  AdfsStreamFactoryImpl(
      final ConfigurationService configurationService,
      final AdfsHttpService adfsHttpService,
      final AdfsBufferPool adfsBufferPool,
      final AdfsStatisticsService adfsStatisticsService,
      final LoggingService loggingService,
      final TracingService tracingService) {
    Preconditions.checkNotNull(adfsHttpService, "adfsHttpService");
    Preconditions.checkNotNull(adfsBufferPool, "adfsBufferPool");
    Preconditions.checkNotNull(adfsStatisticsService, "adfsStatisticsService");
    Preconditions.checkNotNull(configurationService, "configurationService");
    Preconditions.checkNotNull(loggingService, "loggingService");
    Preconditions.checkNotNull(tracingService, "tracingService");

    this.adfsHttpService = adfsHttpService;
    this.adfsBufferPool = adfsBufferPool;
    this.adfsStatisticsService = adfsStatisticsService;
    this.configurationService = configurationService;
    this.loggingService = loggingService.get(AdfsStreamFactory.class);
    this.tracingService = tracingService;
  }

  @Override
  public InputStream createReadStream(
    final AzureDistributedFileSystem azureDistributedFileSystem,
    final Path path,
    final long fileLength,
    final String version) {
    return new AdfsInputStream(
        this.adfsBufferPool,
        this.adfsHttpService,
        azureDistributedFileSystem,
        adfsStatisticsService,
        tracingService,
        loggingService,
        path,
        fileLength,
        configurationService.getReadBufferSize(),
        version);
  }

  @Override
  public OutputStream createWriteStream(final AzureDistributedFileSystem azureDistributedFileSystem, final Path path, final long offset) {
    return new AdfsOutputStream(
        this.adfsHttpService,
        this.adfsBufferPool,
        azureDistributedFileSystem,
        adfsStatisticsService,
        tracingService,
        loggingService,
        path,
        offset,
        configurationService.getWriteBufferSize());
  }
}