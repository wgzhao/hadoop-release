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

import org.junit.Assert;
import org.junit.Test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.AbstractFileSystem;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.azuredfs.constants.FileSystemUriSchemes;
import org.apache.hadoop.fs.azuredfs.contracts.services.ConfigurationService;

public class FileSystemRegistrationTests extends DependencyInjectedTest {

  @Test
  public void ensureAzureDistributedFileSystemIsDefaultFileSystem() throws Exception {
    Configuration configuration = this.serviceProvider.get(ConfigurationService.class).getConfiguration();
    final URI defaultUri = new URI(FileSystemUriSchemes.ADFS_SCHEME, "test@test.com", null, null, null);
    configuration.set(CommonConfigurationKeysPublic.FS_DEFAULT_NAME_KEY, defaultUri.toString());

    FileSystem fs = FileSystem.get(configuration);
    Assert.assertTrue(fs instanceof AzureDistributedFileSystem);

    AbstractFileSystem afs = FileContext.getFileContext(configuration).getDefaultFileSystem();
    Assert.assertTrue(afs instanceof Adfs);
  }

  @Test
  public void ensureSecureAzureDistributedIsDefaultFileSystem() throws Exception {
    Configuration configuration = this.serviceProvider.get(ConfigurationService.class).getConfiguration();
    final URI defaultUri = new URI(FileSystemUriSchemes.ADFS_SECURE_SCHEME, "test@test.com", null, null, null);
    configuration.set(CommonConfigurationKeysPublic.FS_DEFAULT_NAME_KEY, defaultUri.toString());

    FileSystem fs = FileSystem.get(configuration);
    Assert.assertTrue(fs instanceof SecureAzureDistributedFileSystem);

    AbstractFileSystem afs = FileContext.getFileContext(configuration).getDefaultFileSystem();
    Assert.assertTrue(afs instanceof Adfss);
  }
}