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

import com.google.common.base.Preconditions;
import com.microsoft.azure.storage.StorageException;
import okhttp3.Interceptor;
import okhttp3.Request;
import okhttp3.Response;

import java.io.IOException;
import java.security.InvalidKeyException;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.azuredfs.contracts.services.AzureAuthorizationService;

/**
 * This class is responsible to configure all the services used by Azure Distributed Filesystem.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
final class NetworkInterceptorImpl implements Interceptor {
  private final AzureAuthorizationService azureAuthorizationService;

  NetworkInterceptorImpl(
      final AzureAuthorizationService azureAuthorizationService) {
    Preconditions.checkNotNull("azureAuthorizationService", azureAuthorizationService);
    this.azureAuthorizationService = azureAuthorizationService;
  }

  @Override
  public Response intercept(final Chain chain)
      throws IOException {

    Request request = chain.request();

    try {
      request = this.azureAuthorizationService.updateRequestWithAuthorizationHeader(request);
    } catch (StorageException | InvalidKeyException exception) {
      throw new IOException(exception);
    }

    return chain.proceed(request);
  }
}