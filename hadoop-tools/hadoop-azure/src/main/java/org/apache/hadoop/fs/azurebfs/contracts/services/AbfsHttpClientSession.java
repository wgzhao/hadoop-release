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

import com.microsoft.azure.storage.StorageCredentialsAccountAndKey;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

/**
 * AbfsHttpClientSession holds AbfsHttpClient session info.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public interface AbfsHttpClientSession {
  /**
   * Returns Azure storage credentials associated with the current session.
   * @return Azure storage credentials associated with the current session.
   */
  StorageCredentialsAccountAndKey getStorageCredentialsAccountAndKey();

  /**
   * Returns the file system name associated with the current session.
   * @return the file system name associated with the current session.
   */
  String getFileSystem();

  /**
   * Returns the host name.
   * @return the host name.
   */
  String getHostName();

  /**
   * Returns session state
   * @return session state.
   */
  AbfsHttpClientSessionState getSessionState();

  /**
   * Ends the session
   */
  void endSession();
}