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

import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.azuredfs.constants.FileSystemUriSchemes;
import org.apache.hadoop.fs.azuredfs.contracts.services.AdfsHttpService;
import org.apache.hadoop.fs.azuredfs.contracts.services.ServiceProvider;
import org.apache.hadoop.fs.azuredfs.services.MockAdfsHttpImpl;
import org.apache.hadoop.fs.azuredfs.services.ServiceProviderImpl;

public class FileSystemInitializationTests extends DependencyInjectedTest {
  public FileSystemInitializationTests() throws Exception {
    super();

    this.mockServiceInjector.replaceProvider(AdfsHttpService.class, MockAdfsHttpImpl.class);
  }

  @Test
  public void ensureAzureDistributedFileSystemIsInitialized() throws Exception {
    ((MockAdfsHttpImpl)ServiceProviderImpl.instance().get(AdfsHttpService.class)).fileStatus = new FileStatus(0, true, 0, 0, 0, new Path("/blah"));
    final FileSystem fs = FileSystem.get(this.getConfiguration());

    Assert.assertEquals(fs.getUri(), new URI(FileSystemUriSchemes.ADFS_SCHEME, this.getTestUrl(), null, null, null));
    Assert.assertNotNull(fs.getWorkingDirectory());
  }

  @Test
  public void ensureSecureAzureDistributedFileSystemIsInitialized() throws Exception {
    ((MockAdfsHttpImpl)ServiceProviderImpl.instance().get(AdfsHttpService.class)).fileStatus = new FileStatus(0, true, 0, 0, 0, new Path("/blah"));
    final URI defaultUri = new URI(FileSystemUriSchemes.ADFS_SECURE_SCHEME, this.getTestUrl(), null, null, null);
    this.getConfiguration().set(CommonConfigurationKeysPublic.FS_DEFAULT_NAME_KEY, defaultUri.toString());

    final FileSystem fs = FileSystem.get(this.getConfiguration());
    Assert.assertEquals(fs.getUri(), new URI(FileSystemUriSchemes.ADFS_SECURE_SCHEME, this.getTestUrl(), null, null, null));
    Assert.assertNotNull(fs.getWorkingDirectory());
  }
}