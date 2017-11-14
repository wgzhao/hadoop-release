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

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;

/**
 * Configuration service collects required Azure Hadoop configurations and provides it to the consumers.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public interface ConfigurationService extends InjectableService {
  /**
   * Checks if ADFS is running from Emulator;
   * @return is emulator mode.
   */
  boolean isEmulator();

  /**
   * Retrieves storage secure mode from Hadoop configuration;
   * @return storage secure mode;
   */
  boolean isSecureMode();

  /**
   * Retrieves storage account key for provided account name from Hadoop configuration.
   * @param accountName the account name to retrieve the key.
   * @return storage account key;
   */
  String getStorageAccountKey(String accountName);

  /**
   * Returns Hadoop configuration.
   * @return Hadoop configuration.
   */
  Configuration getConfiguration();
}