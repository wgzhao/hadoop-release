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

import javax.inject.Inject;
import javax.inject.Singleton;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.AzureBlobFileSystemException;
import org.apache.hadoop.fs.azurebfs.contracts.services.AbfsRetryStrategy;
import org.apache.hadoop.fs.azurebfs.contracts.services.ConfigurationService;
import org.apache.hadoop.fs.azurebfs.contracts.services.AbfsRetryStrategyFactory;

@Singleton
@InterfaceAudience.Public
@InterfaceStability.Evolving
class AbfsRetryStrategyFactoryImpl implements AbfsRetryStrategyFactory {
  private final ConfigurationService configurationService;

  @Inject
  AbfsRetryStrategyFactoryImpl(final ConfigurationService configurationService) {
    this.configurationService = configurationService;
  }

  @Override
  public AbfsRetryStrategy create() throws AzureBlobFileSystemException {
    final AbfsRetryStrategy abfsRetryStrategy = new AbfsExponentialBackoffRetryStrategyImpl(
        configurationService.getMaxIoRetries(),
        configurationService.getMinBackoffIntervalMilliseconds(),
        configurationService.getMaxBackoffIntervalMilliseconds(),
        configurationService.getBackoffIntervalMilliseconds());
    return abfsRetryStrategy;
  }
}
