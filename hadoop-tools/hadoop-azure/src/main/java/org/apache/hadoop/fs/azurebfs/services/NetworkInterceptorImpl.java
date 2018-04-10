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

import java.io.IOException;
import java.security.InvalidKeyException;

import com.google.common.base.Preconditions;
import com.microsoft.azure.storage.StorageException;
import okhttp3.Interceptor;
import okhttp3.Request;
import okhttp3.Response;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.azurebfs.contracts.services.AbfsHttpAuthorizationService;
import org.apache.hadoop.fs.azurebfs.contracts.services.AbfsHttpClientSession;
import org.apache.hadoop.fs.azurebfs.contracts.services.LoggingService;
import org.apache.hadoop.fs.azurebfs.contracts.services.TracingService;
import org.apache.htrace.core.TraceScope;

@InterfaceAudience.Private
@InterfaceStability.Evolving
final class NetworkInterceptorImpl implements Interceptor {
  private final AbfsHttpAuthorizationService abfsHttpAuthorizationService;
  private final AbfsHttpClientSession abfsHttpClientSession;
  private final TracingService tracingService;
  private final LoggingService loggingService;

  NetworkInterceptorImpl(
      final AbfsHttpClientSession abfsHttpClientSession,
      final AbfsHttpAuthorizationService abfsHttpAuthorizationService,
      final LoggingService loggingService,
      final TracingService tracingService) {
    Preconditions.checkNotNull(abfsHttpAuthorizationService, "abfsHttpAuthorizationService");
    Preconditions.checkNotNull(abfsHttpClientSession, "abfsHttpClientSession");
    Preconditions.checkNotNull(tracingService, "tracingService");
    Preconditions.checkNotNull(loggingService, "loggingService");

    this.abfsHttpAuthorizationService = abfsHttpAuthorizationService;
    this.abfsHttpClientSession = abfsHttpClientSession;
    this.tracingService = tracingService;
    this.loggingService = loggingService.get(NetworkInterceptorImpl.class);
  }

  @Override
  public Response intercept(final Chain chain)
      throws IOException {

    Request request = chain.request();

    final String scopeDescription = "Request METHOD: " + request.method() + " Url: " + request.url();
    final TraceScope traceScope = this.tracingService.traceBegin(scopeDescription);

    try {
      request = this.abfsHttpAuthorizationService.updateRequestWithAuthorizationHeader(
          request,
          this.abfsHttpClientSession.getStorageCredentialsAccountAndKey());
    } catch (StorageException | InvalidKeyException exception) {
      throw new IOException(exception);
    }

    this.loggingService.debug("Sending request {0}", request.toString());
    final Response response = chain.proceed(request);
    this.tracingService.traceEnd(traceScope);
    this.loggingService.debug("Received response {0} for request {1}", response.toString(), request.toString());

    return response;
  }
}