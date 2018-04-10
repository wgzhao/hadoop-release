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

import java.io.IOException;

import com.google.common.base.Preconditions;
import okhttp3.Interceptor;
import okhttp3.Request;
import okhttp3.Response;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.azuredfs.contracts.services.AdfsRetryStrategy;
import org.apache.hadoop.fs.azuredfs.contracts.services.LoggingService;
import org.apache.hadoop.util.ThreadUtil;

@InterfaceAudience.Private
@InterfaceStability.Evolving
final class NetworkRetryImpl implements Interceptor {
    private static final long MAX_RETRY_INTERVAL = 60 * 1000;

    private final AdfsRetryStrategy adfsRetryStrategy;
    private final LoggingService loggingService;

    NetworkRetryImpl(
        final AdfsRetryStrategy adfsRetryStrategy,
        final LoggingService loggingService) {
        Preconditions.checkNotNull(adfsRetryStrategy, "AdfsRetryStrategy");
        Preconditions.checkNotNull(loggingService, "LoggingService");

        this.adfsRetryStrategy = adfsRetryStrategy;
        this.loggingService = loggingService;
    }

    @Override
    public Response intercept(Chain chain) throws IOException {
        Request request = chain.request();
        // try the request
        Response response = chain.proceed(request);

        int retryCount = 0;
        while (adfsRetryStrategy.shouldRetry(retryCount, response)) {
            retryCount++;
            if (response.body() != null) {
                response.body().close();
            }

            final long retryInterval = adfsRetryStrategy.getRetryInterval(retryCount);

            if(retryInterval > MAX_RETRY_INTERVAL) {
                this.loggingService.warning("Retry interval {0} larger than {1} will lead to stale state in sleepAtLeaseIgnoreInterrupts",
                    retryInterval, MAX_RETRY_INTERVAL);
            }
            // sleep for a defined time interval
            ThreadUtil.sleepAtLeastIgnoreInterrupts(retryInterval);
            // retry the request
            response = chain.proceed(request);
        }
        // otherwise just pass the original response on
        return response;
    }
}