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

import java.net.URISyntaxException;

import com.google.inject.Inject;
import com.google.inject.Singleton;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.azuredfs.constants.ConfigurationKeys;
import org.apache.hadoop.fs.azuredfs.constants.TestConfigurationKeys;
import org.apache.hadoop.fs.azuredfs.contracts.services.ConfigurationService;

@Singleton
public final class MockConfigurationServiceImpl implements ConfigurationService {
  private final Configuration configuration;
  private boolean isSecure;

  @Inject
  public MockConfigurationServiceImpl(Configuration configuration) throws URISyntaxException {
    this.configuration = configuration;
    this.isSecure = this.configuration.getBoolean(ConfigurationKeys.FS_AZURE_SECURE_MODE, false);
  }

  public final boolean isSecureMode() {
    return this.isSecure;
  }

  @Override
  public String getStorageAccountKey(String accountName) {
    return configuration.get(
        TestConfigurationKeys.FS_AZURE_TEST_ACCOUNT_KEY_PREFIX
            + accountName
            + TestConfigurationKeys.FS_AZURE_TEST_ACCOUNT_KEY_SUFFIX);
  }

  public final Configuration getConfiguration() {
    return this.configuration;
  }
}