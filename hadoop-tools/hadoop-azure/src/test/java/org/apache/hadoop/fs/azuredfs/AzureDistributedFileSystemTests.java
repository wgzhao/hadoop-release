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

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.azuredfs.contracts.exceptions.AzureServiceErrorResponseException;
import org.apache.hadoop.fs.azuredfs.contracts.services.AdfsHttpService;
import org.apache.hadoop.fs.azuredfs.contracts.services.ConfigurationService;
import org.apache.hadoop.fs.azuredfs.services.ServiceProviderImpl;

import static org.apache.hadoop.fs.azuredfs.contracts.services.AzureServiceErrorCode.FILE_SYSTEM_NOT_FOUND;
import static org.apache.hadoop.test.LambdaTestUtils.intercept;

public class AzureDistributedFileSystemTests extends DependencyInjectedTest {
  public AzureDistributedFileSystemTests() throws Exception {
    super();
  }

  @Test
  public void ensureFileCreated() throws Exception {
    final AzureDistributedFileSystem fs = (AzureDistributedFileSystem) FileSystem.get(this.getConfiguration());
    fs.create(new Path("testfile"));

    FileStatus[] fileStatuses = fs.listStatus(new Path("testfile"));
    Assert.assertEquals(1, fileStatuses.length);
  }

  @Test
  public void ensureFileIsRenamed() throws Exception {
    final AzureDistributedFileSystem fs = (AzureDistributedFileSystem) FileSystem.get(this.getConfiguration());
    fs.create(new Path("testfile"));
    fs.rename(new Path("testfile"), new Path("testfile2"));

    FileStatus[] fileStatuses = fs.listStatus(new Path("testfile2"));
    Assert.assertEquals(1, fileStatuses.length);
  }

  @Test
  public void ensureFileIsDeleted() throws Exception {
    Configuration configuration = ServiceProviderImpl.instance().get(ConfigurationService.class).getConfiguration();
    final AzureDistributedFileSystem fs = (AzureDistributedFileSystem) FileSystem.get(configuration);
    fs.create(new Path("testfile"));
    fs.delete(new Path("testfile"), false);

    FileStatus[] fileStatuses = fs.listStatus(new Path("testfile"));
    Assert.assertEquals(0, fileStatuses.length);
  }

  @Test
  public void writeOneByteToFile() throws Exception {
    Configuration configuration = ServiceProviderImpl.instance().get(ConfigurationService.class).getConfiguration();
    final AzureDistributedFileSystem fs = (AzureDistributedFileSystem) FileSystem.get(configuration);
    fs.create(new Path("testfile"));
    FSDataOutputStream stream = fs.create(new Path("testfile"));

    stream.write(100);
    stream.close();

    FileStatus[] fileStatuses = fs.listStatus(new Path("testfile"));
    Assert.assertEquals(1, fileStatuses.length);
    Assert.assertEquals(1, fileStatuses[0].getLen());
  }

  @Test
  public void readWriteBytesToFile() throws Exception {
    Configuration configuration = ServiceProviderImpl.instance().get(ConfigurationService.class).getConfiguration();
    final AzureDistributedFileSystem fs = (AzureDistributedFileSystem) FileSystem.get(configuration);
    writeOneByteToFile();

    FSDataInputStream inputStream = fs.open(new Path("testfile"), 4 * 1024 * 1024);
    int i = inputStream.read();

    Assert.assertEquals(100, i);
  }

  @Test
  public void readWriteHeavyBytesToFile() throws Exception {
    Configuration configuration = ServiceProviderImpl.instance().get(ConfigurationService.class).getConfiguration();
    final AzureDistributedFileSystem fs = (AzureDistributedFileSystem) FileSystem.get(configuration);
    fs.create(new Path("testfile"));
    final FSDataOutputStream stream = fs.create(new Path("testfile"));

    final byte[] b = new byte[5 * 10240000];
    new Random().nextBytes(b);
    stream.write(b);
    stream.close();

    final byte[] r = new byte[5 * 10240000];
    FSDataInputStream inputStream = fs.open(new Path("testfile"), 4 * 1024 * 1024);
    int result = inputStream.read(r);

    Assert.assertNotEquals(-1, result);
    Assert.assertArrayEquals(r, b);
  }

  @Test
  public void writeHeavyBytesToFile() throws Exception {
    Configuration configuration = ServiceProviderImpl.instance().get(ConfigurationService.class).getConfiguration();
    final AzureDistributedFileSystem fs = (AzureDistributedFileSystem) FileSystem.get(configuration);
    fs.create(new Path("testfile"));
    final FSDataOutputStream stream = fs.create(new Path("testfile"));
    ExecutorService es = Executors.newCachedThreadPool();

    List<Future<Void>> tasks = new ArrayList<>();
    for (int i = 0; i < 100; i++) {
      Callable<Void> callable = new Callable<Void>() {
        @Override
        public Void call() throws Exception {
          final byte[] b = new byte[5 * 1024000];
          new Random().nextBytes(b);
          stream.write(b);
          return null;
        }
      };

      tasks.add(es.submit(callable));
    }

    for (Future<Void> task : tasks) {
      task.get();
    }

    stream.close();

    FileStatus[] fileStatuses = fs.listStatus(new Path("testfile"));
    Assert.assertEquals(1, fileStatuses.length);
    Assert.assertEquals(512000000, fileStatuses[0].getLen());
  }

  @Test
  public void deleteDirectory() throws Exception {
    Configuration configuration = ServiceProviderImpl.instance().get(ConfigurationService.class).getConfiguration();
    final AzureDistributedFileSystem fs = (AzureDistributedFileSystem) FileSystem.get(configuration);
    fs.mkdirs(new Path("testfile"));
    fs.mkdirs(new Path("testfile/test1"));
    fs.mkdirs(new Path("testfile/test1/test2"));

    fs.delete(new Path("testfile"), true);
    FileStatus[] fileStatuses = fs.listStatus(new Path("testfile"));
    Assert.assertEquals(0, fileStatuses.length);
  }

  @Test
  public void renameDirectory() throws Exception {
    Configuration configuration = ServiceProviderImpl.instance().get(ConfigurationService.class).getConfiguration();
    final AzureDistributedFileSystem fs = (AzureDistributedFileSystem) FileSystem.get(configuration);
    fs.mkdirs(new Path("testfile"));
    fs.mkdirs(new Path("testfile/test1"));
    fs.mkdirs(new Path("testfile/test1/test2"));
    fs.mkdirs(new Path("testfile/test1/test2/test3"));

    fs.rename(new Path("testfile/test1"), new Path("testfile/test10"));
    FileStatus[] fileStatuses = fs.listStatus(new Path("testfile1/"));
    Assert.assertEquals(0, fileStatuses.length);
  }

  @After
  public void TestCleanup() throws Exception {
    FileSystem.closeAll();

    Configuration configuration = ServiceProviderImpl.instance().get(ConfigurationService.class).getConfiguration();
    final AzureDistributedFileSystem fs = (AzureDistributedFileSystem) FileSystem.get(configuration);

    final AdfsHttpService adfsHttpService = ServiceProviderImpl.instance().get(AdfsHttpService.class);
    adfsHttpService.deleteFilesystem(fs);

    AzureServiceErrorResponseException ex = intercept(
        AzureServiceErrorResponseException.class,
        new Callable<Void>() {
          @Override
          public Void call() throws Exception {
            adfsHttpService.getFilesystemProperties(fs);
            return null;
          }
        });

    Assert.assertEquals(FILE_SYSTEM_NOT_FOUND.getStatusCode(), ex.getStatusCode());
  }
}