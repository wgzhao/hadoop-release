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

import com.google.inject.Inject;
import com.google.inject.Singleton;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.azuredfs.constants.ConfigurationKeys;
import org.apache.hadoop.fs.azuredfs.contracts.services.ConfigurationService;

@Singleton
@InterfaceAudience.Private
@InterfaceStability.Evolving
final class ConfigurationServiceImpl implements ConfigurationService {
  private final Configuration configuration;
  private final boolean isSecure;

  @Inject
  ConfigurationServiceImpl(final Configuration configuration) {
    this.configuration = configuration;
    this.isSecure = this.configuration.getBoolean(ConfigurationKeys.FS_AZURE_SECURE_MODE, false);
  }

  @Override
  public boolean isEmulator() {
    return this.getConfiguration().getBoolean(ConfigurationKeys.FS_AZURE_EMULATOR_ENABLED, false);
  }

  @Override
  public boolean isSecureMode() {
    return this.isSecure;
  }

  @Override
  public String getStorageAccountKey(final String accountName) {
    String accountKey = this.configuration.get(
        ConfigurationKeys.FS_AZURE_ACCOUNT_KEY_PROPERTY_NAME
            + accountName);

    return accountKey;
  }

  @Override
  public Configuration getConfiguration() {
    return this.configuration;
  }
}
