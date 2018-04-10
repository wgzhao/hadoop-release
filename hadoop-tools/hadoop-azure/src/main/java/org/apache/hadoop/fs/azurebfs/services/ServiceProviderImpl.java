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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.inject.Guice;
import com.google.inject.Injector;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.ServiceResolutionException;
import org.apache.hadoop.fs.azurebfs.contracts.services.InjectableService;
import org.apache.hadoop.fs.azurebfs.contracts.services.ServiceProvider;

/**
 * Dependency injected services provider.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public final class ServiceProviderImpl implements ServiceProvider {
  private static ServiceProviderImpl serviceProvider;
  private final Injector serviceInjector;

  private ServiceProviderImpl(final Configuration configuration) {
    this.serviceInjector = Guice.createInjector(new ServiceInjectorImpl(Preconditions.checkNotNull(configuration, "configuration")));
  }

  @VisibleForTesting
  private ServiceProviderImpl(final Injector serviceInjector) {
    Preconditions.checkNotNull(serviceInjector, "serviceInjector");
    this.serviceInjector = serviceInjector;
  }

  /**
   * Create an instance or returns existing instance of service provider.
   * This method must be marked as synchronized to ensure thread-safety.
   * @param configuration hadoop configuration.
   * @return ServiceProvider the service provider instance.
   */
  public static synchronized ServiceProvider create(final Configuration configuration) {
    if (serviceProvider == null) {
      serviceProvider = new ServiceProviderImpl(configuration);
    }

    return serviceProvider;
  }

  /**
   * Returns current instance of service provider.
   * @return ServiceProvider the service provider instance.
   */
  public static ServiceProvider instance() {
    return serviceProvider;
  }

  @VisibleForTesting
  static synchronized ServiceProvider create(Injector serviceInjector) {
    serviceProvider = new ServiceProviderImpl(serviceInjector);
    return serviceProvider;
  }

  /**
   * Returns an instance of resolved injectable service by class name.
   * The injectable service must be configured first to be resolvable.
   * @param clazz the injectable service which is expected to be returned.
   * @param <T> The type of injectable service.
   * @return T instance
   * @throws ServiceResolutionException if the service is not resolvable.
   */
  @Override
  public <T extends InjectableService> T get(final Class<T> clazz) throws ServiceResolutionException {
    try {
      return this.serviceInjector.getInstance(clazz);
    } catch (Exception ex) {
      throw new ServiceResolutionException(clazz.getSimpleName(), ex);
    }
  }
}