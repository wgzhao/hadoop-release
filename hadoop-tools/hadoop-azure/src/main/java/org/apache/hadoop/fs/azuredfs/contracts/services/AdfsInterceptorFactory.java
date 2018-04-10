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

package org.apache.hadoop.fs.azuredfs.contracts.services;

import okhttp3.Interceptor;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.azuredfs.contracts.exceptions.AzureDistributedFileSystemException;

/**
 * AdfsInterceptorFactory responsible for creating interceptors.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public interface AdfsInterceptorFactory extends InjectableService {
  /**
   * Create a network authentication proxy for a given adfsHttpClientSession.
   * @param adfsHttpClientSession
   * @return an instance of network authentication proxy.
   * @throws AzureDistributedFileSystemException
   */
  Interceptor createNetworkAuthenticationProxy(AdfsHttpClientSession adfsHttpClientSession) throws AzureDistributedFileSystemException;

  /**
   * Create a network throughput monitor for a given adfsHttpClientSession.
   * @param adfsHttpClientSession
   * @return an instance of network throughput monitor.
   * @throws AzureDistributedFileSystemException
   */
  Interceptor createNetworkThroughputMonitor(AdfsHttpClientSession adfsHttpClientSession) throws AzureDistributedFileSystemException;

  /**
   * Create a network throttler for a given adfsHttpClientSession.
   * @param adfsHttpClientSession
   * @return an instance of network throttler.
   * @throws AzureDistributedFileSystemException
   */
  Interceptor createNetworkThrottler(AdfsHttpClientSession adfsHttpClientSession) throws AzureDistributedFileSystemException;

  /**
   * Create a retry interceptor for a given adfsHttpClientSession.
   * @param adfsRetryStrategy
   * @return an instance of retry interceptor.
   * @throws AzureDistributedFileSystemException
   */
  Interceptor createRetryInterceptor(AdfsRetryStrategy adfsRetryStrategy)
      throws AzureDistributedFileSystemException;
}