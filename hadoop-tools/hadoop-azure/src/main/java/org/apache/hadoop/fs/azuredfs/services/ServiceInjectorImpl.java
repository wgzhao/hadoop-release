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

import java.util.HashMap;
import java.util.Map;

import com.google.inject.AbstractModule;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.azuredfs.contracts.services.AdfsBlobHandler;
import org.apache.hadoop.fs.azuredfs.contracts.services.AdfsBufferPool;
import org.apache.hadoop.fs.azuredfs.contracts.services.AdfsHttpAuthorizationService;
import org.apache.hadoop.fs.azuredfs.contracts.services.AdfsHttpClientFactory;
import org.apache.hadoop.fs.azuredfs.contracts.services.AdfsHttpClientSessionFactory;
import org.apache.hadoop.fs.azuredfs.contracts.services.AdfsHttpService;
import org.apache.hadoop.fs.azuredfs.contracts.services.AdfsNetworkTrafficAnalysisService;
import org.apache.hadoop.fs.azuredfs.contracts.services.AdfsNetworkInterceptorFactory;
import org.apache.hadoop.fs.azuredfs.contracts.services.AdfsStatisticsService;
import org.apache.hadoop.fs.azuredfs.contracts.services.AdfsStreamFactory;
import org.apache.hadoop.fs.azuredfs.contracts.services.ConfigurationService;
import org.apache.hadoop.fs.azuredfs.contracts.services.LoggingService;
import org.apache.hadoop.fs.azuredfs.contracts.services.TracingService;
import org.apache.hadoop.fs.azuredfs.contracts.services.AdfsRetryStrategyFactory;
/**
 * This class is responsible to configure all the services used by Azure Distributed Filesystem.
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
    this.providers.put(AdfsHttpAuthorizationService.class, AdfsHttpAuthorizationServiceImpl.class);

    this.providers.put(AdfsHttpService.class, AdfsHttpServiceImpl.class);
    this.providers.put(AdfsHttpClientFactory.class, AdfsHttpClientFactoryImpl.class);

    this.providers.put(AdfsHttpClientSessionFactory.class, AdfsHttpClientSessionFactoryImpl.class);
    this.providers.put(AdfsStreamFactory.class, AdfsStreamFactoryImpl.class);

    this.providers.put(AdfsBufferPool.class, AdfsBufferPoolImpl.class);
    this.providers.put(LoggingService.class, LoggingServiceImpl.class);

    this.providers.put(TracingService.class, TracingServiceImpl.class);
    this.providers.put(AdfsBlobHandler.class, AdfsBlobHandlerImpl.class);

    this.providers.put(AdfsRetryStrategyFactory.class, AdfsRetryStrategyFactoryImpl.class);
    this.providers.put(AdfsNetworkInterceptorFactory.class, AdfsNetworkInterceptorFactoryImpl.class);

    this.providers.put(AdfsNetworkTrafficAnalysisService.class, AdfsNetworkTrafficAnalysisServiceImpl.class);
    this.providers.put(AdfsStatisticsService.class, AdfsStatisticsServiceImpl.class);
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