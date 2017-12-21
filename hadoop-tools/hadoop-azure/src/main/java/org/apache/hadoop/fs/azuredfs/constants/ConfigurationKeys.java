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
  public static final String FS_AZURE_SECURE_MODE = "fs.azure.secure.mode";

  // Retry strategy defined by the user
  public static final String AZURE_MIN_BACKOFF_INTERVAL = "fs.azure.io.retry.min.backoff.interval";
  public static final String AZURE_MAX_BACKOFF_INTERVAL = "fs.azure.io.retry.max.backoff.interval";
  public static final String AZURE_BACKOFF_INTERVAL = "fs.azure.io.retry.backoff.interval";
  public static final String AZURE_MAX_IO_RETRIES = "fs.azure.io.retry.max.retries";

  // Remove this and use common azure storage emulator property for public release.
  public static final String FS_AZURE_EMULATOR_ENABLED = "fs.azure.adfs.emulator.enabled";

  // Read and write buffer sizes defined by the user
  public static final String AZURE_WRITE_BUFFER_SIZE = "fs.azure.write.request.size";
  public static final String AZURE_READ_BUFFER_SIZE = "fs.azure.read.request.size";
  public static final String AZURE_BLOCK_SIZE_PROPERTY_NAME = "fs.azure.block.size";
  public static final String AZURE_BLOCK_LOCATION_HOST_PROPERTY_NAME = "fs.azure.block.location.impersonatedhost";
  public static final String AZURE_CONCURRENT_CONNECTION_VALUE_OUT = "fs.azure.concurrentRequestCount.out";

  public static final String FS_AZURE_AUTOTHROTTLING_ENABLE = "fs.azure.autothrottling.enable";

  private ConfigurationKeys() {}
}
