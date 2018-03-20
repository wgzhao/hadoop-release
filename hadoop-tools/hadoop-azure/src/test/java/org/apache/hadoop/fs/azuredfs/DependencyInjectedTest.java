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

package org.apache.hadoop.fs.azuredfs;

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
import org.apache.hadoop.fs.azuredfs.constants.ConfigurationKeys;
import org.apache.hadoop.fs.azuredfs.constants.FileSystemUriSchemes;
import org.apache.hadoop.fs.azuredfs.constants.TestConfigurationKeys;
import org.apache.hadoop.fs.azuredfs.contracts.exceptions.AzureServiceErrorResponseException;
import org.apache.hadoop.fs.azuredfs.contracts.services.AdfsHttpClientFactory;
import org.apache.hadoop.fs.azuredfs.contracts.services.AdfsHttpClientSessionFactory;
import org.apache.hadoop.fs.azuredfs.contracts.services.AdfsHttpService;
import org.apache.hadoop.fs.azuredfs.contracts.services.ConfigurationService;
import org.apache.hadoop.fs.azuredfs.services.AdfsHttpClientTestUtils;
import org.apache.hadoop.fs.azuredfs.services.MockAdfsHttpClientFactoryImpl;
import org.apache.hadoop.fs.azuredfs.services.MockServiceInjectorImpl;
import org.apache.hadoop.fs.azuredfs.services.MockServiceProviderImpl;
import org.apache.hadoop.fs.azuredfs.services.ServiceProviderImpl;

import static org.apache.hadoop.fs.azuredfs.contracts.services.AzureServiceErrorCode.FILE_SYSTEM_NOT_FOUND;
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
    configuration.addResource("azure-adfs-test.xml");

    final String adfsUrl = this.getFileSystemName() + "@" + this.getAccountName();
    final URI defaultUri = new URI(FileSystemUriSchemes.ADFS_SCHEME, adfsUrl, null, null, null);
    this.testUrl = defaultUri.toString();
    configuration.set(CommonConfigurationKeysPublic.FS_DEFAULT_NAME_KEY, defaultUri.toString());
    configuration.setBoolean(ConfigurationKeys.AZURE_CREATE_REMOTE_FILESYSTEM_DURING_INITIALIZATION, true);
    this.mockServiceInjector = new MockServiceInjectorImpl(configuration);

    this.isEmulator = this.configuration.getBoolean(ConfigurationKeys.FS_AZURE_EMULATOR_ENABLED, false);
  }

  @Before
  public void initialize() throws Exception {
    if (this.isEmulator) {
      this.mockServiceInjector.replaceProvider(AdfsHttpClientFactory.class, MockAdfsHttpClientFactoryImpl.class);
      this.mockServiceInjector.replaceInstance(AdfsHttpClientSessionFactory.class,
          AdfsHttpClientTestUtils.createAdfsHttpClientSessionFactory(this.getAccountName(), this.getAccountKey(), this.fileSystemName));
    }

    MockServiceProviderImpl.Create(this.mockServiceInjector);

    if (!this.isEmulator) {
      final URI wasbUri = new URI(adfsUrlToWasbUrl(this.getTestUrl()));
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

    final AzureDistributedFileSystem fs = this.getFileSystem();
    final AdfsHttpService adfsHttpService = ServiceProviderImpl.instance().get(AdfsHttpService.class);
    adfsHttpService.deleteFilesystem(fs);

    if (!(new MockUtil().isMock(adfsHttpService))) {
      AzureServiceErrorResponseException ex = intercept(
          AzureServiceErrorResponseException.class,
          new Callable<Void>() {
            @Override
            public Void call() throws Exception {
              adfsHttpService.getFilesystemProperties(fs);
              return null;
            }
          });

      assertEquals(FILE_SYSTEM_NOT_FOUND.getStatusCode(), ex.getStatusCode());
    }
  }

  public AzureDistributedFileSystem getFileSystem() throws Exception {
    final Configuration configuration = ServiceProviderImpl.instance().get(ConfigurationService.class).getConfiguration();
    final AzureDistributedFileSystem fs = (AzureDistributedFileSystem) FileSystem.get(configuration);
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

  protected static String wasbUrlToAdfsUrl(String wasbUrl) {
    String data = wasbUrl.replace("wasb://", "adfs://");
    data = data.replace(".blob.", ".dfs.");
    return data;
  }

  protected static String adfsUrlToWasbUrl(String adfsUrl) {
    String data = adfsUrl.replace("adfs://", "wasb://");
    data = data.replace(".dfs.", ".blob.");
    return data;
  }
}
