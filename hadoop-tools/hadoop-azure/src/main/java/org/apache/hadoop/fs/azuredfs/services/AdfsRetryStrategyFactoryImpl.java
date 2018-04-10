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

import javax.inject.Inject;
import javax.inject.Singleton;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.azuredfs.contracts.exceptions.AzureDistributedFileSystemException;
import org.apache.hadoop.fs.azuredfs.contracts.services.AdfsRetryStrategy;
import org.apache.hadoop.fs.azuredfs.contracts.services.ConfigurationService;
import org.apache.hadoop.fs.azuredfs.contracts.services.AdfsRetryStrategyFactory;

@Singleton
@InterfaceAudience.Public
@InterfaceStability.Evolving
class AdfsRetryStrategyFactoryImpl implements AdfsRetryStrategyFactory{
  private final ConfigurationService configurationService;

  @Inject
  public AdfsRetryStrategyFactoryImpl(final ConfigurationService configurationService) {
    this.configurationService = configurationService;
  }

  @Override
  public AdfsRetryStrategy create() throws AzureDistributedFileSystemException {
    final AdfsRetryStrategy adfsRetryStrategy = new AdfsExponentialBackoffRetryStrategyImpl(
        configurationService.getMaxIoRetries(),
        configurationService.getMinBackoffIntervalMilliseconds(),
        configurationService.getMaxBackoffIntervalMilliseconds(),
        configurationService.getBackoffIntervalMilliseconds());
    return adfsRetryStrategy;
  }
}
