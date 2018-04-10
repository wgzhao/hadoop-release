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

import java.io.IOException;
import java.security.InvalidKeyException;

import com.microsoft.azure.storage.StorageCredentialsAccountAndKey;
import com.microsoft.azure.storage.StorageException;
import okhttp3.Request;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

/**
 * Azure authorization service is used to update the requests with proper authorization artifacts.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public interface AbfsHttpAuthorizationService extends InjectableService {
  /**
   * Updates the provided request object with proper authorization headers.
   * @param request the request to be updated.
   * @param storageCredentialsAccountAndKey storage account name and account key.
   * @throws InvalidKeyException if there is an invalid header key.
   * @throws StorageException if provided storage account info is invalid.
   * @throws IOException if the request cannot be modified or updated.
   */
  Request updateRequestWithAuthorizationHeader(
      Request request,
      StorageCredentialsAccountAndKey storageCredentialsAccountAndKey) throws IOException, InvalidKeyException, StorageException;
}