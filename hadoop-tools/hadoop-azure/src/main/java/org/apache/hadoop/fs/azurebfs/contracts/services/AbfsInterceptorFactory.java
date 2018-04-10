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

package org.apache.hadoop.fs.azurebfs.contracts.services;

import okhttp3.Interceptor;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.AzureBlobFileSystemException;

/**
 * AbfsInterceptorFactory responsible for creating interceptors.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public interface AbfsInterceptorFactory extends InjectableService {
  /**
   * Create a network authentication proxy for a given abfsHttpClientSession.
   * @param abfsHttpClientSession
   * @return an instance of network authentication proxy.
   * @throws AzureBlobFileSystemException
   */
  Interceptor createNetworkAuthenticationProxy(AbfsHttpClientSession abfsHttpClientSession) throws AzureBlobFileSystemException;

  /**
   * Create a network throughput monitor for a given abfsHttpClientSession.
   * @param abfsHttpClientSession
   * @return an instance of network throughput monitor.
   * @throws AzureBlobFileSystemException
   */
  Interceptor createNetworkThroughputMonitor(AbfsHttpClientSession abfsHttpClientSession) throws AzureBlobFileSystemException;

  /**
   * Create a network throttler for a given abfsHttpClientSession.
   * @param abfsHttpClientSession
   * @return an instance of network throttler.
   * @throws AzureBlobFileSystemException
   */
  Interceptor createNetworkThrottler(AbfsHttpClientSession abfsHttpClientSession) throws AzureBlobFileSystemException;

  /**
   * Create a retry interceptor for a given abfsHttpClientSession.
   * @param abfsRetryStrategy
   * @return an instance of retry interceptor.
   * @throws AzureBlobFileSystemException
   */
  Interceptor createRetryInterceptor(AbfsRetryStrategy abfsRetryStrategy)
      throws AzureBlobFileSystemException;
}