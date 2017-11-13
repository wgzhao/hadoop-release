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

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
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
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.azuredfs.contracts.exceptions.AzureServiceErrorResponseException;
import org.apache.hadoop.fs.azuredfs.contracts.services.AdfsHttpService;
import org.apache.hadoop.fs.azuredfs.contracts.services.ConfigurationService;
import org.apache.hadoop.fs.azuredfs.services.ServiceProviderImpl;

import static org.apache.hadoop.fs.azuredfs.contracts.services.AzureServiceErrorCode.FILE_SYSTEM_NOT_FOUND;
import static org.apache.hadoop.test.LambdaTestUtils.intercept;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class AzureDistributedFileSystemTests extends DependencyInjectedTest {
  public AzureDistributedFileSystemTests() throws Exception {
    super();
  }

  @Test(expected = IOException.class)
  public void appendDirShouldFail() throws Exception {
    final AzureDistributedFileSystem fs = (AzureDistributedFileSystem) FileSystem.get(this.getConfiguration());
    fs.mkdirs(new Path("testfile"));
    fs.append(new Path("testfile"), 0);
  }

  @Test
  public void testCopyFromLocalFileSystem() throws Exception {
    final AzureDistributedFileSystem fs = (AzureDistributedFileSystem) FileSystem.get(this.getConfiguration());
    Path localFilePath = new Path(System.getProperty("test.build.data",
        "azure_test"));
    FileSystem localFs = FileSystem.get(new Configuration());
    localFs.delete(localFilePath, true);
    try {
      writeString(localFs, localFilePath, "Testing");
      Path dstPath = new Path("copiedFromLocal");
      assertTrue(FileUtil.copy(localFs, localFilePath, fs, dstPath, false,
          fs.getConf()));
      assertTrue(fs.exists(dstPath));
      assertEquals("Testing", readString(fs, dstPath));
      fs.delete(dstPath, true);
    } finally {
      localFs.delete(localFilePath, true);
    }
  }

  @Test
  public void ensureFileCreated() throws Exception {
    final AzureDistributedFileSystem fs = (AzureDistributedFileSystem) FileSystem.get(this.getConfiguration());
    fs.create(new Path("testfile"));

    FileStatus fileStatus = fs.getFileStatus(new Path("testfile"));
    Assert.assertNotNull(fileStatus);
  }

  @Test
  public void ensureFileIsRenamed() throws Exception {
    final AzureDistributedFileSystem fs = (AzureDistributedFileSystem) FileSystem.get(this.getConfiguration());
    fs.create(new Path("testfile"));
    fs.rename(new Path("testfile"), new Path("testfile2"));

    FileStatus fileStatus = fs.getFileStatus(new Path("testfile2"));
    Assert.assertNotNull(fileStatus);

    Assert.assertNull(tryGetFileStatus(fs, new Path("testfile")));
  }

  @Test
  public void ensureFileIsDeleted() throws Exception {
    Configuration configuration = ServiceProviderImpl.instance().get(ConfigurationService.class).getConfiguration();
    final AzureDistributedFileSystem fs = (AzureDistributedFileSystem) FileSystem.get(configuration);
    fs.create(new Path("testfile"));
    fs.delete(new Path("testfile"), false);

    Assert.assertNull(tryGetFileStatus(fs, new Path("testfile")));
  }

  @Test
  public void writeOneByteToFile() throws Exception {
    Configuration configuration = ServiceProviderImpl.instance().get(ConfigurationService.class).getConfiguration();
    final AzureDistributedFileSystem fs = (AzureDistributedFileSystem) FileSystem.get(configuration);
    fs.create(new Path("testfile"));
    FSDataOutputStream stream = fs.create(new Path("testfile"));

    stream.write(100);
    stream.close();

    FileStatus fileStatus = fs.getFileStatus(new Path("testfile"));
    Assert.assertEquals(1, fileStatus.getLen());
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

    FileStatus fileStatus = fs.getFileStatus(new Path("testfile"));
    Assert.assertEquals(512000000, fileStatus.getLen());
  }

  @Test
  public void deleteDirectory() throws Exception {
    Configuration configuration = ServiceProviderImpl.instance().get(ConfigurationService.class).getConfiguration();
    final AzureDistributedFileSystem fs = (AzureDistributedFileSystem) FileSystem.get(configuration);
    fs.mkdirs(new Path("testfile"));
    fs.mkdirs(new Path("testfile/test1"));
    fs.mkdirs(new Path("testfile/test1/test2"));

    fs.delete(new Path("testfile"), true);
    Assert.assertNull(tryGetFileStatus(fs, new Path("testfile")));
  }

  @Test
  public void renameDirectory() throws Exception {
    Configuration configuration = ServiceProviderImpl.instance().get(ConfigurationService.class).getConfiguration();
    final AzureDistributedFileSystem fs = (AzureDistributedFileSystem) FileSystem.get(configuration);
    fs.mkdirs(new Path("testDir"));
    fs.mkdirs(new Path("testDir/test1"));
    fs.mkdirs(new Path("testDir/test1/test2"));
    fs.mkdirs(new Path("testDir/test1/test2/test3"));

    fs.rename(new Path("testDir/test1"), new Path("testDir/test10"));
    Assert.assertNull(tryGetFileStatus(fs, new Path("testDir/test1")));
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

  private FileStatus tryGetFileStatus(FileSystem fs, Path path) {
    try {
      return fs.getFileStatus(path);
    }
    catch (IOException ex) {
      return null;
    }
  }

  private String readString(FileSystem fs, Path testFile) throws IOException {
    FSDataInputStream inputStream = fs.open(testFile);
    String ret = readString(inputStream);
    inputStream.close();
    return ret;
  }

  private String readString(FSDataInputStream inputStream) throws IOException {
    BufferedReader reader = new BufferedReader(new InputStreamReader(
        inputStream));
    final int BUFFER_SIZE = 1024;
    char[] buffer = new char[BUFFER_SIZE];
    int count = reader.read(buffer, 0, BUFFER_SIZE);
    if (count > BUFFER_SIZE) {
      throw new IOException("Exceeded buffer size");
    }
    inputStream.close();
    return new String(buffer, 0, count);
  }

  private void writeString(FileSystem fs, Path path, String value)
      throws IOException {
    FSDataOutputStream outputStream = fs.create(path, true);
    writeString(outputStream, value);
  }

  private void writeString(FSDataOutputStream outputStream, String value)
      throws IOException {
    BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(
        outputStream));
    writer.write(value);
    writer.close();
  }
}