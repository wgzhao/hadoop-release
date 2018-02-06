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

import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.azuredfs.AzureDistributedFileSystem;
import org.apache.hadoop.fs.azuredfs.DependencyInjectedTest;
import org.apache.hadoop.fs.azuredfs.contracts.services.AdfsHttpService;
import org.apache.hadoop.fs.azuredfs.contracts.services.ConfigurationService;

import static org.junit.Assert.assertEquals;

public class TestAdfsHttpService extends DependencyInjectedTest {
  public TestAdfsHttpService() throws Exception {
    super();
  }

  @Test
  public void testReadWriteBytesToFileAndEnsureThreadPoolCleanup() throws Exception {
    Configuration configuration = ServiceProviderImpl.instance().get(ConfigurationService.class).getConfiguration();
    final AzureDistributedFileSystem fs = (AzureDistributedFileSystem) FileSystem.get(configuration);
    testWriteOneByteToFileAndEnsureThreadPoolCleanup();

    FSDataInputStream inputStream = fs.open(new Path("testfile"), 4 * 1024 * 1024);
    int i = inputStream.read();

    assertEquals(100, i);
  }

  @Test
  public void testWriteOneByteToFileAndEnsureThreadPoolCleanup() throws Exception {
    Configuration configuration = ServiceProviderImpl.instance().get(ConfigurationService.class).getConfiguration();
    final AzureDistributedFileSystem fs = (AzureDistributedFileSystem) FileSystem.get(configuration);
    fs.create(new Path("testfile"));
    FSDataOutputStream stream = fs.create(new Path("testfile"));

    stream.write(100);
    stream.close();

    FileStatus fileStatus = fs.getFileStatus(new Path("testfile"));
    assertEquals(1, fileStatus.getLen());
  }

  @After
  public void ensurePoolCleanup() throws Exception {
    Configuration configuration = ServiceProviderImpl.instance().get(ConfigurationService.class).getConfiguration();
    final AzureDistributedFileSystem fs = (AzureDistributedFileSystem) FileSystem.get(configuration);
    final AdfsHttpService adfsHttpService = ServiceProviderImpl.instance().get(AdfsHttpService.class);

    if (!(adfsHttpService instanceof MockAdfsHttpImpl)) {
      adfsHttpService.closeFileSystem(fs);
      Assert.assertFalse(((AdfsHttpServiceImpl) adfsHttpService).threadPoolsAreRunning(fs));
    }
  }
}
