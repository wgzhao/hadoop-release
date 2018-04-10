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

import okhttp3.Response;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

/**
 * AdfsRetryStrategy
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public interface AdfsRetryStrategy {
    /**
     * Returns name of the current retry strategy
     * @return name of the current retry strategy
     * */
    String name();

    /**
     * Returns if a request should be retried based on the retry count, current response,
     * and the current strategy.
     *
     * @param retryCount The current retry attempt count.
     * @param response The exception that caused the retry conditions to occur.
     * @return true if the request should be retried; false otherwise.
     */
    boolean shouldRetry(int retryCount, Response response);

    /**
     * Returns the specified sleep time needed for the next retry
     *
     * @param retries The current retry attempt count.
     * @return retry Interval for next retry;
     * */
    long getRetryInterval(int retries);
}