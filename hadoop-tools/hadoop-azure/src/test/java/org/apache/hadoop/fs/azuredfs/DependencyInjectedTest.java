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

package org.apache.hadoop.fs.azuredfs;

import org.junit.Before;

import org.apache.hadoop.fs.azuredfs.contracts.services.AzureAuthorizationService;
import org.apache.hadoop.fs.azuredfs.contracts.services.AzureDistributedFileSystemService;
import org.apache.hadoop.fs.azuredfs.contracts.services.ConfigurationService;
import org.apache.hadoop.fs.azuredfs.contracts.services.InjectableService;
import org.apache.hadoop.fs.azuredfs.contracts.services.LoggingService;
import org.apache.hadoop.fs.azuredfs.contracts.services.TracingService;
import org.apache.hadoop.fs.azuredfs.services.MockServiceProviderImpl;
import org.apache.hadoop.fs.azuredfs.services.Mocks.MockLoggingServiceImpl;
import org.apache.hadoop.fs.azuredfs.services.Mocks.MockTracingServiceImpl;
import org.apache.hadoop.fs.azuredfs.services.mocks.MockAzureAuthorizationServiceImpl;
import org.apache.hadoop.fs.azuredfs.services.mocks.MockAzureDistributedFileSystemImpl;
import org.apache.hadoop.fs.azuredfs.services.mocks.MockConfigurationServiceImpl;

public abstract class DependencyInjectedTest {
  protected final MockServiceProviderImpl serviceProvider;

  protected DependencyInjectedTest() {
    this.serviceProvider = new MockServiceProviderImpl();

    this.serviceProvider.bind(ConfigurationService.class, MockConfigurationServiceImpl.class);
    this.serviceProvider.bind(AzureAuthorizationService.class, MockAzureAuthorizationServiceImpl.class);
    this.serviceProvider.bind(AzureDistributedFileSystemService.class, MockAzureDistributedFileSystemImpl.class);
    this.serviceProvider.bind(TracingService.class, MockTracingServiceImpl.class);
    this.serviceProvider.bind(LoggingService.class, MockLoggingServiceImpl.class);
  }

  @Before
  public void initialize() {
    this.serviceProvider.initialize();
  }

  protected <T extends InjectableService> void replaceDependency(Class<T> tInterface, Class<? extends T> tClazz) {
    this.serviceProvider.bind(tInterface, tClazz);
  }
}
