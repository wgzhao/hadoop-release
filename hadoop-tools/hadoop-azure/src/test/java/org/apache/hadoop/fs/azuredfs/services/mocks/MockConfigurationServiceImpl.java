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

package org.apache.hadoop.fs.azuredfs.services.mocks;

import org.apache.commons.codec.binary.Base64;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.azuredfs.constants.ConfigurationKeys;
import org.apache.hadoop.fs.azuredfs.contracts.services.ConfigurationService;

import java.net.URISyntaxException;

public final class MockConfigurationServiceImpl implements ConfigurationService {
  private String storageAccountName;
  private String storageAccountKey;
  private String fileSystem;
  private boolean isSecure;
  private final Configuration configuration;

  public MockConfigurationServiceImpl() throws URISyntaxException {
    this.configuration = new Configuration();
    this.configuration.addResource("azure-adfs-test.xml");

    this.storageAccountName = this.configuration.get(ConfigurationKeys.FS_AZURE_ACCOUNT_NAME);
    this.storageAccountKey = this.configuration.get(ConfigurationKeys.FS_AZURE_ACCOUNT_KEY);
    this.fileSystem = this.configuration.get(ConfigurationKeys.FS_AZURE_ACCOUNT_FILESYSTEM);
    this.isSecure = this.configuration.getBoolean(ConfigurationKeys.FS_AZURE_SECURE_MODE, false);

    if (this.storageAccountName == null) {
      this.storageAccountName = "accountName";
      this.configuration.set(ConfigurationKeys.FS_AZURE_ACCOUNT_NAME, this.storageAccountName);
    }

    if (this.storageAccountKey == null) {
      String accountKey = new String(Base64.encodeBase64("accountKey".getBytes()));
      this.storageAccountKey = accountKey;
      this.configuration.set(ConfigurationKeys.FS_AZURE_ACCOUNT_KEY, this.storageAccountKey);
    }

    if (this.fileSystem == null) {
      this.fileSystem = "testContainer";
      this.configuration.set(ConfigurationKeys.FS_AZURE_ACCOUNT_FILESYSTEM, this.fileSystem);
    }
  }

  public final String getStorageAccountName() {
    return this.storageAccountName;
  }

  public final String getStorageAccountKey() {
    return this.storageAccountKey;
  }

  public final String getFileSystem() {
    return this.fileSystem;
  }

  public final boolean isSecureMode() { return this.isSecure; }

  public final Configuration getConfiguration() { return this.configuration; }
}