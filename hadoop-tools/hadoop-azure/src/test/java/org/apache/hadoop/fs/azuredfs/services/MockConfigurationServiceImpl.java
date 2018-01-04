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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.azuredfs.constants.ConfigurationKeys;
import org.apache.hadoop.fs.azuredfs.constants.FileSystemConfigurations;
import org.apache.hadoop.fs.azuredfs.constants.TestConfigurationKeys;
import org.apache.hadoop.fs.azuredfs.contracts.exceptions.InvalidConfigurationValueException;

public final class MockConfigurationServiceImpl extends ConfigurationServiceImpl {
  private final Configuration configuration;
  private boolean isSecure;
  private int writeBufferSize;
  private int readBufferSize;
  private int minBackoffInterval;
  private int maxBackoffInterval;
  private int maxIoRetries;
  private int backoffInterval;
  private long azureBlockSize;
  private String azureBlockLocationHost;

  @Inject
  public MockConfigurationServiceImpl(Configuration configuration) throws URISyntaxException, InvalidConfigurationValueException, IllegalAccessException {
    super(configuration);
    this.configuration = configuration;
    this.isSecure = this.configuration.getBoolean(ConfigurationKeys.FS_AZURE_SECURE_MODE, false);
    this.writeBufferSize = this.configuration.getInt(ConfigurationKeys.AZURE_WRITE_BUFFER_SIZE, FileSystemConfigurations.DEFAULT_WRITE_BUFFER_SIZE);
    this.readBufferSize = this.configuration.getInt(ConfigurationKeys.AZURE_READ_BUFFER_SIZE, FileSystemConfigurations.DEFAULT_READ_BUFFER_SIZE);
  }

  @Override
  public boolean isEmulator() {
    return this.configuration.getBoolean(ConfigurationKeys.FS_AZURE_EMULATOR_ENABLED, false);
  }

  public final boolean isSecureMode() {
    return this.isSecure;
  }

  @Override
  public String getStorageAccountKey(String accountName) {
    return configuration.get(
        TestConfigurationKeys.FS_AZURE_TEST_ACCOUNT_KEY_PREFIX
            + accountName);
  }

  public final Configuration getConfiguration() {
    return this.configuration;
  }

  @Override
  public int getWriteBufferSize() { return this.writeBufferSize; }

  @Override
  public int getReadBufferSize() { return this.readBufferSize; }

  @Override
  public int getMinBackoffIntervalMilliseconds() { return this.minBackoffInterval; }

  @Override
  public int getMaxBackoffIntervalMilliseconds() { return this.maxBackoffInterval; }

  @Override
  public int getBackoffIntervalMilliseconds() { return this.backoffInterval; }

  @Override
  public int getMaxIoRetries() { return this.maxIoRetries; }

  @Override
  public long getAzureBlockSize() { return this.azureBlockSize; }

  @Override
  public String getAzureBlockLocationHost() { return null; }

  @Override
  public int getMaxConcurrentThreads() { return FileSystemConfigurations.MAX_CONCURRENT_THREADS; }
}