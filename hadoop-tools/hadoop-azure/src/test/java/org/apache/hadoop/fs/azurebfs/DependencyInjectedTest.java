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

package org.apache.hadoop.fs.azurebfs;

import java.net.URI;
import java.util.UUID;
import java.util.concurrent.Callable;

import org.junit.After;
import org.junit.Before;
import org.mockito.internal.util.MockUtil;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.azure.AzureNativeFileSystemStore;
import org.apache.hadoop.fs.azure.NativeAzureFileSystem;
import org.apache.hadoop.fs.azure.metrics.AzureFileSystemInstrumentation;
import org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys;
import org.apache.hadoop.fs.azurebfs.constants.FileSystemUriSchemes;
import org.apache.hadoop.fs.azurebfs.constants.TestConfigurationKeys;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.AzureServiceErrorResponseException;
import org.apache.hadoop.fs.azurebfs.contracts.services.AbfsHttpClientFactory;
import org.apache.hadoop.fs.azurebfs.contracts.services.AbfsHttpClientSessionFactory;
import org.apache.hadoop.fs.azurebfs.contracts.services.AbfsHttpService;
import org.apache.hadoop.fs.azurebfs.contracts.services.ConfigurationService;
import org.apache.hadoop.fs.azurebfs.services.AbfsHttpClientTestUtils;
import org.apache.hadoop.fs.azurebfs.services.MockAbfsHttpClientFactoryImpl;
import org.apache.hadoop.fs.azurebfs.services.MockServiceInjectorImpl;
import org.apache.hadoop.fs.azurebfs.services.MockServiceProviderImpl;
import org.apache.hadoop.fs.azurebfs.services.ServiceProviderImpl;

import static org.apache.hadoop.fs.azurebfs.contracts.services.AzureServiceErrorCode.FILE_SYSTEM_NOT_FOUND;
import static org.apache.hadoop.test.LambdaTestUtils.intercept;
import static org.junit.Assert.assertEquals;

public abstract class DependencyInjectedTest {
  protected final MockServiceInjectorImpl mockServiceInjector;
  private final Configuration configuration;
  private final String fileSystemName;
  private final String testUrl;
  private final boolean isEmulator;
  private NativeAzureFileSystem wasb;

  protected DependencyInjectedTest() throws Exception {
    fileSystemName = UUID.randomUUID().toString();
    configuration = new Configuration();
    configuration.addResource("azure-bfs-test.xml");

    final String abfsUrl = this.getFileSystemName() + "@" + this.getAccountName();
    final URI defaultUri = new URI(FileSystemUriSchemes.ABFS_SCHEME, abfsUrl, null, null, null);
    this.testUrl = defaultUri.toString();
    configuration.set(CommonConfigurationKeysPublic.FS_DEFAULT_NAME_KEY, defaultUri.toString());
    configuration.setBoolean(ConfigurationKeys.AZURE_CREATE_REMOTE_FILESYSTEM_DURING_INITIALIZATION, true);
    this.mockServiceInjector = new MockServiceInjectorImpl(configuration);

    this.isEmulator = this.configuration.getBoolean(ConfigurationKeys.FS_AZURE_EMULATOR_ENABLED, false);
  }

  @Before
  public void initialize() throws Exception {
    if (this.isEmulator) {
      this.mockServiceInjector.replaceProvider(AbfsHttpClientFactory.class, MockAbfsHttpClientFactoryImpl.class);
      this.mockServiceInjector.replaceInstance(AbfsHttpClientSessionFactory.class,
          AbfsHttpClientTestUtils.createAbfsHttpClientSessionFactory(this.getAccountName(), this.getAccountKey(), this.fileSystemName));
    }

    MockServiceProviderImpl.Create(this.mockServiceInjector);

    if (!this.isEmulator) {
      final URI wasbUri = new URI(abfsUrlToWasbUrl(this.getTestUrl()));
      final AzureNativeFileSystemStore azureNativeFileSystemStore = new AzureNativeFileSystemStore();
      azureNativeFileSystemStore.initialize(
          wasbUri,
          this.getConfiguration(),
          new AzureFileSystemInstrumentation(this.getConfiguration()));

      this.wasb = new NativeAzureFileSystem(azureNativeFileSystemStore);
      this.wasb.initialize(wasbUri, configuration);
    }
  }

  @After
  public void testCleanup() throws Exception {
    if (this.wasb != null) {
      this.wasb.close();
    }

    FileSystem.closeAll();

    final AzureBlobFileSystem fs = this.getFileSystem();
    final AbfsHttpService abfsHttpService = ServiceProviderImpl.instance().get(AbfsHttpService.class);
    abfsHttpService.deleteFilesystem(fs);

    if (!(new MockUtil().isMock(abfsHttpService))) {
      AzureServiceErrorResponseException ex = intercept(
          AzureServiceErrorResponseException.class,
          new Callable<Void>() {
            @Override
            public Void call() throws Exception {
              abfsHttpService.getFilesystemProperties(fs);
              return null;
            }
          });

      assertEquals(FILE_SYSTEM_NOT_FOUND.getStatusCode(), ex.getStatusCode());
    }
  }

  public AzureBlobFileSystem getFileSystem() throws Exception {
    final Configuration configuration = ServiceProviderImpl.instance().get(ConfigurationService.class).getConfiguration();
    final AzureBlobFileSystem fs = (AzureBlobFileSystem) FileSystem.get(configuration);
    return fs;
  }

  protected NativeAzureFileSystem getWasbFileSystem() {
    return this.wasb;
  }

  protected String getHostName() {
    return configuration.get(TestConfigurationKeys.FS_AZURE_TEST_HOST_NAME);
  }

  protected String getTestUrl() {
    return testUrl;
  }

  protected String getFileSystemName() {
    return fileSystemName;
  }

  protected String getAccountName() {
    return configuration.get(TestConfigurationKeys.FS_AZURE_TEST_ACCOUNT_NAME);
  }

  protected String getAccountKey() {
    return configuration.get(
        TestConfigurationKeys.FS_AZURE_TEST_ACCOUNT_KEY_PREFIX
            + getAccountName());
  }

  protected Configuration getConfiguration() {
    return this.configuration;
  }

  protected boolean isEmulator() { return isEmulator; }

  protected static String wasbUrlToAbfsUrl(String wasbUrl) {
    String data = wasbUrl.replace("wasb://", "abfs://");
    data = data.replace(".blob.", ".dfs.");
    return data;
  }

  protected static String abfsUrlToWasbUrl(String abfsUrl) {
    String data = abfsUrl.replace("abfs://", "wasb://");
    data = data.replace(".dfs.", ".blob.");
    return data;
  }
}
