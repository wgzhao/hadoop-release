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

package org.apache.hadoop.fs.azuredfs.constants;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

/**
 * Responsible to keep all the Azure Distributed Filesystem configurations keys in Hadoop configuration file.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public final class ConfigurationKeys {
  public static final String FS_AZURE_ACCOUNT_KEY_PROPERTY_NAME = "fs.azure.account.key.";
  public static final String FS_AZURE_ACCOUNT_KEY_SUFFIX = ".dfs.core.windows.net";
  public static final String FS_AZURE_SECURE_MODE = "fs.azure.secure.mode";

  // Supporting WASB
  public static final String FS_AZURE_WASB_ACCOUNT_KEY_SUFFIX = ".blob.core.windows.net";

  // Remove this and use common azure storage emulator property for public release.
  public static final String FS_AZURE_EMULATOR_ENABLED = "fs.azure.adfs.emulator.enabled";

  private ConfigurationKeys() {}
}