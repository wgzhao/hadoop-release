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
import com.microsoft.azure.dfs.rest.client.generated.implementation.AzureDistributedFileSystemRestClientImpl;
import com.microsoft.rest.RestClient;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.azuredfs.contracts.exceptions.TimeoutException;
import org.apache.hadoop.fs.azuredfs.contracts.services.AdfsHttpClient;
import org.apache.hadoop.fs.azuredfs.contracts.services.AdfsHttpClientSession;
import org.apache.hadoop.fs.azuredfs.contracts.services.LoggingService;

import static org.apache.hadoop.util.Time.now;

/**
 * Adfs http client base implementation.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
abstract class AdfsHttpClientBaseImpl extends AzureDistributedFileSystemRestClientImpl implements AdfsHttpClient {
  private final AdfsHttpClientSession adfsHttpClientSession;
  private final LoggingService loggingService;
  private static final int TIMEOUT_MILISECONDS = 5000;
  private static final int MAX_REQUESTS_PER_HOST = 200;

  AdfsHttpClientBaseImpl(
      final AdfsHttpClientSession adfsHttpClientSession,
      final LoggingService loggingService,
      final RestClient restClient) {
    super(restClient);

    Preconditions.checkNotNull(adfsHttpClientSession, "adfsHttpClientSession");
    Preconditions.checkNotNull(loggingService, "loggingService");

    this.adfsHttpClientSession = adfsHttpClientSession;
    this.loggingService = loggingService;

    restClient.httpClient().dispatcher().setMaxRequestsPerHost(MAX_REQUESTS_PER_HOST);
    restClient.httpClient().dispatcher().setMaxRequests(MAX_REQUESTS_PER_HOST);
  }

  @Override
  public AdfsHttpClientSession getSession() {
    return this.adfsHttpClientSession;
  }

  @Override
  public void close() throws TimeoutException {
    this.loggingService.debug("Closing AdfsHttpClientBaseImpl for filesystem: {0}", this.adfsHttpClientSession.getFileSystem());
    this.getSession().endSession();
    this.restClient().close();

    long deadline = now() + TIMEOUT_MILISECONDS;
    while (this.restClient().httpClient().dispatcher().queuedCallsCount() > 0
        || this.restClient().httpClient().dispatcher().runningCallsCount() > 0) {
      if (now() > deadline) {
        this.loggingService.debug(
            "Closing adfsHttpClientSession timed out for filesystem: {0}",
            this.adfsHttpClientSession.getFileSystem());

        throw new TimeoutException("Closing adfsHttpClientSession timed out.");
      }
    }
  }
}