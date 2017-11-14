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
 * Responsible to keep all the Azure Distributed Filesystem related configurations.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public final class FileSystemConfigurations {
  public static final String USER_HOME_DIRECTORY_PREFIX = "/user";
  public static final int FS_AZURE_DEFAULT_CONNECTION_TIMEOUT = 90;
  public static final int FS_AZURE_DEFAULT_CONNECTION_READ_TIMEOUT = 90;
  public static final String HDI_IS_FOLDER = "hdi_isfolder";

  public static final String ADFS_EMULATOR_TARGET_STORAGE_VERSION = "2017-11-09";
  public static final String ADFS_TARGET_STORAGE_VERSION = "2017-04-17";

  public static final long MAX_AZURE_BLOCK_SIZE = 512 * 1024 * 1024L;
  public static final String AZURE_BLOCK_LOCATION_HOST_DEFAULT = "localhost";

  private FileSystemConfigurations() {}
}