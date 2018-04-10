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

import java.util.HashMap;
import java.util.Map;

import com.google.inject.AbstractModule;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.azurebfs.contracts.services.AbfsBufferPool;
import org.apache.hadoop.fs.azurebfs.contracts.services.AbfsHttpAuthorizationService;
import org.apache.hadoop.fs.azurebfs.contracts.services.AbfsHttpClientFactory;
import org.apache.hadoop.fs.azurebfs.contracts.services.AbfsHttpClientSessionFactory;
import org.apache.hadoop.fs.azurebfs.contracts.services.AbfsHttpService;
import org.apache.hadoop.fs.azurebfs.contracts.services.AbfsThrottlingNetworkTrafficAnalysisService;
import org.apache.hadoop.fs.azurebfs.contracts.services.AbfsInterceptorFactory;
import org.apache.hadoop.fs.azurebfs.contracts.services.AbfsStatisticsService;
import org.apache.hadoop.fs.azurebfs.contracts.services.AbfsStreamFactory;
import org.apache.hadoop.fs.azurebfs.contracts.services.ConfigurationService;
import org.apache.hadoop.fs.azurebfs.contracts.services.LoggingService;
import org.apache.hadoop.fs.azurebfs.contracts.services.TracingService;
import org.apache.hadoop.fs.azurebfs.contracts.services.AbfsRetryStrategyFactory;

/**
 * This class is responsible to configure all the services used by Azure Blob File System.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
class ServiceInjectorImpl extends AbstractModule {
  private final Configuration configuration;
  private final Map<Class, Class> providers;
  private final Map<Class, Object> instances;

  ServiceInjectorImpl(Configuration configuration) {
    this.providers = new HashMap<>();
    this.instances = new HashMap<>();
    this.configuration = configuration;

    this.instances.put(Configuration.class, this.configuration);

    this.providers.put(ConfigurationService.class, ConfigurationServiceImpl.class);
    this.providers.put(AbfsHttpAuthorizationService.class, AbfsHttpAuthorizationServiceImpl.class);

    this.providers.put(AbfsHttpService.class, AbfsHttpServiceImpl.class);
    this.providers.put(AbfsHttpClientFactory.class, AbfsHttpClientFactoryImpl.class);

    this.providers.put(AbfsHttpClientSessionFactory.class, AbfsHttpClientSessionFactoryImpl.class);
    this.providers.put(AbfsStreamFactory.class, AbfsStreamFactoryImpl.class);

    this.providers.put(AbfsBufferPool.class, AbfsBufferPoolImpl.class);
    this.providers.put(LoggingService.class, LoggingServiceImpl.class);

    this.providers.put(TracingService.class, TracingServiceImpl.class);

    this.providers.put(AbfsRetryStrategyFactory.class, AbfsRetryStrategyFactoryImpl.class);
    this.providers.put(AbfsInterceptorFactory.class, AbfsInterceptorFactoryImpl.class);

    this.providers.put(AbfsThrottlingNetworkTrafficAnalysisService.class, AbfsThrottlingNetworkTrafficAnalysisServiceImpl.class);
    this.providers.put(AbfsStatisticsService.class, AbfsStatisticsServiceImpl.class);
  }

  @Override
  protected void configure() {
    for (Map.Entry<Class, Object> entrySet : this.instances.entrySet()) {
      bind(entrySet.getKey()).toInstance(entrySet.getValue());
    }

    for (Map.Entry<Class, Class> entrySet : this.providers.entrySet()) {
      bind(entrySet.getKey()).to(entrySet.getValue());
    }
  }

  protected Configuration getConfiguration() {
    return this.configuration;
  }

  protected Map<Class, Class> getProviders() {
    return this.providers;
  }

  protected Map<Class, Object> getInstances() {
    return this.instances;
  }
}