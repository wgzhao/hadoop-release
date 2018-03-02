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

import java.util.Hashtable;

import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.azuredfs.AzureDistributedFileSystem;
import org.apache.hadoop.fs.azuredfs.DependencyInjectedTest;
import org.apache.hadoop.fs.azuredfs.contracts.services.AdfsHttpService;

import static org.junit.Assert.assertEquals;

public class ITestAdfsHttpServiceImpl extends DependencyInjectedTest {
  public ITestAdfsHttpServiceImpl() throws Exception {
    super();
  }

  @Test
  public void testReadWriteBytesToFileAndEnsureThreadPoolCleanup() throws Exception {
    final AzureDistributedFileSystem fs = this.getFileSystem();
    testWriteOneByteToFileAndEnsureThreadPoolCleanup();

    FSDataInputStream inputStream = fs.open(new Path("testfile"), 4 * 1024 * 1024);
    int i = inputStream.read();

    assertEquals(100, i);
  }

  @Test
  public void testWriteOneByteToFileAndEnsureThreadPoolCleanup() throws Exception {
    final AzureDistributedFileSystem fs = this.getFileSystem();
    FSDataOutputStream stream = fs.create(new Path("testfile"));

    stream.write(100);
    stream.close();

    FileStatus fileStatus = fs.getFileStatus(new Path("testfile"));
    assertEquals(1, fileStatus.getLen());
  }

  @Test
  public void testBase64FileSystemProperties() throws Exception {
    final AzureDistributedFileSystem fs = this.getFileSystem();
    final Hashtable<String, String> properties = new Hashtable<>();
    properties.put("key", "{ value: value }");
    ServiceProviderImpl.instance().get(AdfsHttpService.class).setFilesystemProperties(
        fs, properties);
    Hashtable<String, String> fetchedProperties = ServiceProviderImpl.instance().get(AdfsHttpService.class).getFilesystemProperties(fs);

    Assert.assertEquals(properties, fetchedProperties);
  }

  @Test
  public void testBase64PathProperties() throws Exception {
    final AzureDistributedFileSystem fs = this.getFileSystem();
    final Hashtable<String, String> properties = new Hashtable<>();
    properties.put("key", "{ value: valueTest }");
    fs.create(new Path("/testpath"));
    ServiceProviderImpl.instance().get(AdfsHttpService.class).setPathProperties(
        fs, new Path("/testpath"), properties);
    Hashtable<String, String> fetchedProperties =
        ServiceProviderImpl.instance().get(AdfsHttpService.class).getPathProperties(fs, new Path("/testpath"));

    Assert.assertEquals(properties, fetchedProperties);
  }

  @Test (expected = Exception.class)
  public void testBase64InvalidFileSystemProperties() throws Exception {
    final AzureDistributedFileSystem fs = this.getFileSystem();
    final Hashtable<String, String> properties = new Hashtable<>();
    properties.put("key", "{ value: value歲 }");
    ServiceProviderImpl.instance().get(AdfsHttpService.class).setFilesystemProperties(
        fs, properties);
    Hashtable<String, String> fetchedProperties = ServiceProviderImpl.instance().get(AdfsHttpService.class).getFilesystemProperties(fs);

    Assert.assertEquals(properties, fetchedProperties);
  }

  @Test (expected = Exception.class)
  public void testBase64InvalidPathProperties() throws Exception {
    final AzureDistributedFileSystem fs = this.getFileSystem();
    final Hashtable<String, String> properties = new Hashtable<>();
    properties.put("key", "{ value: valueTest兩 }");
    fs.create(new Path("/testpath"));
    ServiceProviderImpl.instance().get(AdfsHttpService.class).setPathProperties(
        fs, new Path("/testpath"), properties);
    Hashtable<String, String> fetchedProperties =
        ServiceProviderImpl.instance().get(AdfsHttpService.class).getPathProperties(fs, new Path("/testpath"));

    Assert.assertEquals(properties, fetchedProperties);
  }

  @After
  public void ensurePoolCleanup() throws Exception {
    final AzureDistributedFileSystem fs = this.getFileSystem();
    final AdfsHttpService adfsHttpService = ServiceProviderImpl.instance().get(AdfsHttpService.class);

    adfsHttpService.closeFileSystem(fs);
    Assert.assertFalse(((AdfsHttpServiceImpl) adfsHttpService).threadPoolsAreRunning(fs));
  }
}
