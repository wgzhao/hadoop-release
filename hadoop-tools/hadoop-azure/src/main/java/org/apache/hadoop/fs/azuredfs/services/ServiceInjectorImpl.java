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

import com.google.inject.AbstractModule;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.azuredfs.contracts.services.LoggingService;
import org.apache.hadoop.fs.azuredfs.contracts.services.TracingService;
import org.apache.hadoop.fs.azuredfs.contracts.services.AzureAuthorizationService;
import org.apache.hadoop.fs.azuredfs.contracts.services.AzureDistributedFileSystemClientFactory;
import org.apache.hadoop.fs.azuredfs.contracts.services.AzureDistributedFileSystemService;
import org.apache.hadoop.fs.azuredfs.contracts.services.ConfigurationService;

/**
 * This class is responsible to configure all the services used by Azure Distributed Filesystem.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
final class ServiceInjectorImpl extends AbstractModule {
  private final Configuration configuration;

  ServiceInjectorImpl(Configuration configuration) {
    this.configuration = configuration;
  }

  @Override
  protected void configure() {
    bind(Configuration.class).toInstance(this.configuration);
    bind(ConfigurationService.class).to(ConfigurationServiceImpl.class);
    bind(AzureAuthorizationService.class).to(AzureAuthorizationServiceImpl.class);
    bind(AzureDistributedFileSystemService.class).to(AzureDistributedFileSystemServiceImpl.class);
    bind(AzureDistributedFileSystemClientFactory.class).to(AzureDistributedFileSystemClientFactoryImpl.class);

    bind(LoggingService.class).to(LoggingServiceImpl.class).asEagerSingleton();
    bind(TracingService.class).to(TracingServiceImpl.class).asEagerSingleton();
  }
}