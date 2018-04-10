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

import java.lang.reflect.Field;

import com.google.common.base.Preconditions;
import com.microsoft.azure.bfs.rest.client.generated.implementation.AzureBlobFileSystemRestClientImpl;
import com.microsoft.rest.RestClient;
import com.microsoft.rest.retry.RetryStrategy;
import okhttp3.Response;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.TimeoutException;
import org.apache.hadoop.fs.azurebfs.contracts.services.AbfsHttpClient;
import org.apache.hadoop.fs.azurebfs.contracts.services.AbfsHttpClientSession;
import org.apache.hadoop.fs.azurebfs.contracts.services.LoggingService;

import static org.apache.hadoop.util.Time.now;

/**
 * Abfs http client base implementation.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
abstract class AbfsHttpClientBaseImpl extends AzureBlobFileSystemRestClientImpl implements AbfsHttpClient {
  private final AbfsHttpClientSession abfsHttpClientSession;
  private final LoggingService loggingService;
  private static final int TIMEOUT_MILISECONDS = 5000;
  private static final int MAX_REQUESTS_PER_HOST = 200;

  AbfsHttpClientBaseImpl(
      final AbfsHttpClientSession abfsHttpClientSession,
      final LoggingService loggingService,
      final RestClient.Builder restClient) {
  // UpdateBuilder is created due to bug created: https://github.com/Azure/autorest-clientruntime-for-java/issues/410
    super(updateBuilder(restClient).build());

    Preconditions.checkNotNull(abfsHttpClientSession, "abfsHttpClientSession");
    Preconditions.checkNotNull(loggingService, "loggingService");

    this.abfsHttpClientSession = abfsHttpClientSession;
    this.loggingService = loggingService.get(AbfsHttpClientBaseImpl.class);

    this.restClient().httpClient().dispatcher().setMaxRequestsPerHost(MAX_REQUESTS_PER_HOST);
    this.restClient().httpClient().dispatcher().setMaxRequests(MAX_REQUESTS_PER_HOST);

    this.withDnsSuffix(this.abfsHttpClientSession.getHostName());
    this.withAccountName(this.abfsHttpClientSession.getStorageCredentialsAccountAndKey().getAccountName());
  }

  @Override
  public AbfsHttpClientSession getSession() {
    return this.abfsHttpClientSession;
  }

  @Override
  public void close() throws TimeoutException {
    this.loggingService.debug("Closing AbfsHttpClientBaseImpl for filesystem: {0}", this.abfsHttpClientSession.getFileSystem());
    this.getSession().endSession();
    this.restClient().close();

    long deadline = now() + TIMEOUT_MILISECONDS;
    while (this.restClient().httpClient().dispatcher().queuedCallsCount() > 0
        || this.restClient().httpClient().dispatcher().runningCallsCount() > 0) {
      if (now() > deadline) {
        this.loggingService.debug(
            "Closing abfsHttpClientSession timed out for filesystem: {0}",
            this.abfsHttpClientSession.getFileSystem());

        throw new TimeoutException("Closing abfsHttpClientSession timed out.");
      }
    }
  }

  /**
   * This is a temporary fix to overwrite and disable the default retry strategy to be built in the restClient. The
   * RetryHandler in package - com.microsoft.rest.retry has a bug that prevents customized retry strategy to be set
   * to the retry handler, and another bug which causes immediate retry without wait. The correct exponential backoff
   * retry is added in httpClient build.
  **/
  private static RestClient.Builder updateBuilder(final RestClient.Builder builder) {
    try {
      final Class<?> bClass = builder.getClass();
      final Field retryStrategyField = bClass.getDeclaredField("retryStrategy");
      retryStrategyField.setAccessible(true);
      retryStrategyField.set(builder, new RetryStrategy("DummyRetryStrategy", false) {
        @Override
        public boolean shouldRetry(int retryCount, Response response) {
          return false;
        }
      });
      return builder;
    } catch (Exception ex) {
      throw new IllegalStateException();
    }
  }
}