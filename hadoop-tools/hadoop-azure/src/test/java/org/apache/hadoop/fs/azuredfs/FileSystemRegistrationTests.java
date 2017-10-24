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

import org.apache.hadoop.fs.AbstractFileSystem;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.azuredfs.constants.FileSystemUriSchemes;
import org.apache.hadoop.fs.azuredfs.contracts.services.AdfsHttpService;
import org.apache.hadoop.fs.azuredfs.services.MockAdfsHttpImpl;

public class FileSystemRegistrationTests extends DependencyInjectedTest {
  public FileSystemRegistrationTests() throws Exception {
    super();

    this.mockServiceInjector.replaceProvider(AdfsHttpService.class, MockAdfsHttpImpl.class);
  }

  @Test
  public void ensureAzureDistributedFileSystemIsDefaultFileSystem() throws Exception {
    FileSystem fs = FileSystem.get(this.getConfiguration());
    Assert.assertTrue(fs instanceof AzureDistributedFileSystem);

    AbstractFileSystem afs = FileContext.getFileContext(this.getConfiguration()).getDefaultFileSystem();
    Assert.assertTrue(afs instanceof Adfs);
  }

  @Test
  public void ensureSecureAzureDistributedIsDefaultFileSystem() throws Exception {
    final URI defaultUri = new URI(FileSystemUriSchemes.ADFS_SECURE_SCHEME, this.getTestUrl(), null, null, null);
    this.getConfiguration().set(CommonConfigurationKeysPublic.FS_DEFAULT_NAME_KEY, defaultUri.toString());

    FileSystem fs = FileSystem.get(this.getConfiguration());
    Assert.assertTrue(fs instanceof SecureAzureDistributedFileSystem);

    AbstractFileSystem afs = FileContext.getFileContext(this.getConfiguration()).getDefaultFileSystem();
    Assert.assertTrue(afs instanceof Adfss);
  }
}