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
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.junit.Assert;
import org.junit.Test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.azuredfs.contracts.services.AdfsHttpService;
import org.apache.hadoop.fs.azuredfs.contracts.services.ConfigurationService;
import org.apache.hadoop.fs.azuredfs.services.ServiceProviderImpl;

import static org.junit.Assert.*;

public class AzureDistributedFileSystemTests extends DependencyInjectedTest {
  public AzureDistributedFileSystemTests() throws Exception {
    super();
  }

  @Test(expected = IOException.class)
  public void testAppendDirShouldFail() throws Exception {
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
  public void testEnsureFileCreated() throws Exception {
    final AzureDistributedFileSystem fs = (AzureDistributedFileSystem) FileSystem.get(this.getConfiguration());
    fs.create(new Path("testfile"));

    FileStatus fileStatus = fs.getFileStatus(new Path("testfile"));
    assertNotNull(fileStatus);
  }

  @Test(expected = FileNotFoundException.class)
  public void testEnsureFileIsRenamed() throws Exception {
    final AzureDistributedFileSystem fs = (AzureDistributedFileSystem) FileSystem.get(this.getConfiguration());
    fs.create(new Path("testfile"));
    fs.rename(new Path("testfile"), new Path("testfile2"));

    FileStatus fileStatus = fs.getFileStatus(new Path("testfile2"));
    assertNotNull(fileStatus);

    fs.getFileStatus(new Path("testfile"));
  }

  @Test(expected = FileNotFoundException.class)
  public void testEnsureFileIsDeleted() throws Exception {
    Configuration configuration = ServiceProviderImpl.instance().get(ConfigurationService.class).getConfiguration();
    final AzureDistributedFileSystem fs = (AzureDistributedFileSystem) FileSystem.get(configuration);
    fs.create(new Path("testfile"));
    fs.delete(new Path("testfile"), false);

    fs.getFileStatus(new Path("testfile"));
  }

  @Test
  public void testWriteOneByteToFile() throws Exception {
    Configuration configuration = ServiceProviderImpl.instance().get(ConfigurationService.class).getConfiguration();
    final AzureDistributedFileSystem fs = (AzureDistributedFileSystem) FileSystem.get(configuration);
    fs.create(new Path("testfile"));
    FSDataOutputStream stream = fs.create(new Path("testfile"));

    stream.write(100);
    stream.close();

    FileStatus fileStatus = fs.getFileStatus(new Path("testfile"));
    assertEquals(1, fileStatus.getLen());
  }

  @Test
  public void ensureStatusWorksForRoot() throws Exception {
    Configuration configuration = ServiceProviderImpl.instance().get(ConfigurationService.class).getConfiguration();
    final AzureDistributedFileSystem fs = (AzureDistributedFileSystem) FileSystem.get(configuration);

    fs.getFileStatus(new Path("/"));
    fs.listStatus(new Path("/"));
  }

  @Test
  public void testReadWriteBytesToFile() throws Exception {
    Configuration configuration = ServiceProviderImpl.instance().get(ConfigurationService.class).getConfiguration();
    final AzureDistributedFileSystem fs = (AzureDistributedFileSystem) FileSystem.get(configuration);
    testWriteOneByteToFile();

    FSDataInputStream inputStream = fs.open(new Path("testfile"), 4 * 1024 * 1024);
    int i = inputStream.read();

    assertEquals(100, i);
  }

  @Test
  public void testReadWriteHeavyBytesToFile() throws Exception {
    Configuration configuration = ServiceProviderImpl.instance().get(ConfigurationService.class).getConfiguration();
    final AzureDistributedFileSystem fs = (AzureDistributedFileSystem) FileSystem.get(configuration);
    final FSDataOutputStream stream = fs.create(new Path("testfile"));

    final byte[] b = new byte[5 * 10240000];
    new Random().nextBytes(b);
    stream.write(b);
    stream.close();

    final byte[] r = new byte[5 * 10240000];
    FSDataInputStream inputStream = fs.open(new Path("testfile"), 4 * 1024 * 1024);
    int result = inputStream.read(r);

    assertNotEquals(-1, result);
    assertArrayEquals(r, b);
  }

  @Test
  public void testAdfsOutputStreamAsyncFlushWithRetainUncommitedData() throws Exception {
    Configuration configuration = ServiceProviderImpl.instance().get(ConfigurationService.class).getConfiguration();
    final AzureDistributedFileSystem fs = (AzureDistributedFileSystem) FileSystem.get(configuration);
    final FSDataOutputStream stream = fs.create(new Path("/testfile"));

    final byte[] b = new byte[5 * 1024000];
    new Random().nextBytes(b);

    for (int i = 0; i < 2; i++) {
      stream.write(b);

      for (int j = 0; j < 200; j++) {
        stream.flush();
        Thread.sleep(10);
      }
    }

    stream.close();

    final byte[] r = new byte[5 * 1024000];
    FSDataInputStream inputStream = fs.open(new Path("/testfile"), 4 * 1024 * 1024);

    while (inputStream.available() != 0) {
      int result = inputStream.read(r);

      assertNotEquals(-1, result);
      assertArrayEquals(r, b);
    }
  }

  @Test
  public void testAdfsOutputStreamSyncFlush() throws Exception {
    Configuration configuration = ServiceProviderImpl.instance().get(ConfigurationService.class).getConfiguration();
    final AzureDistributedFileSystem fs = (AzureDistributedFileSystem) FileSystem.get(configuration);
    final FSDataOutputStream stream = fs.create(new Path("/testfile"));

    final byte[] b = new byte[5 * 10240000];
    new Random().nextBytes(b);
    stream.write(b);

    for (int i = 0; i < 200; i++) {
      stream.hsync();
      stream.hflush();
      Thread.sleep(10);
    }
    stream.close();

    final byte[] r = new byte[5 * 10240000];
    FSDataInputStream inputStream = fs.open(new Path("/testfile"), 4 * 1024 * 1024);
    int result = inputStream.read(r);

    assertNotEquals(-1, result);
    assertArrayEquals(r, b);
  }

  @Test
  public void testListPath() throws Exception {
    Configuration configuration = ServiceProviderImpl.instance().get(ConfigurationService.class).getConfiguration();
    final AzureDistributedFileSystem fs = (AzureDistributedFileSystem) FileSystem.get(configuration);
    final List<Future> tasks = new ArrayList<>();

    ExecutorService es = Executors.newFixedThreadPool(10);
    for (int i = 0; i < 6000; i++) {
      final Path fileName = new Path("/test" + i);
      Callable<Void> callable = new Callable<Void>() {
        @Override
        public Void call() throws Exception {
          fs.create(fileName);
          return null;
        }
      };

      tasks.add(es.submit(callable));
    }

    for (Future<Void> task : tasks) {
      task.get();
    }

    es.shutdownNow();
    FileStatus[] files = fs.listStatus(new Path("/"));
    Assert.assertEquals(files.length, 6000 + 1 /* user directory */);
  }

  @Test
  public void testReadWriteHeavyBytesToFileWithSmallerChunks() throws Exception {
    Configuration configuration = ServiceProviderImpl.instance().get(ConfigurationService.class).getConfiguration();
    final AzureDistributedFileSystem fs = (AzureDistributedFileSystem) FileSystem.get(configuration);
    fs.create(new Path("testfile"));
    final FSDataOutputStream stream = fs.create(new Path("testfile"));

    final byte[] b = new byte[5 * 1024000];
    new Random().nextBytes(b);
    stream.write(b);
    stream.close();

    final byte[] r = new byte[5 * 1024000];
    FSDataInputStream inputStream = fs.open(new Path("testfile"), 4 * 1024 * 1024);
    int offset = 0;
    while(inputStream.read(r, offset, 100) > 0) {
      offset += 100;
    }

    assertArrayEquals(r, b);
  }

  @Test
  public void testWriteHeavyBytesToFile() throws Exception {
    Configuration configuration = ServiceProviderImpl.instance().get(ConfigurationService.class).getConfiguration();
    final AzureDistributedFileSystem fs = (AzureDistributedFileSystem) FileSystem.get(configuration);
    fs.create(new Path("testfile"));
    final FSDataOutputStream stream = fs.create(new Path("testfile"));
    ExecutorService es = Executors.newFixedThreadPool(10);

    final byte[] b = new byte[2 * 10240000];
    new Random().nextBytes(b);
    List<Future<Void>> tasks = new ArrayList<>();
    for (int i = 0; i < 100; i++) {
      Callable<Void> callable = new Callable<Void>() {
        @Override
        public Void call() throws Exception {
          stream.write(b);
          return null;
        }
      };

      tasks.add(es.submit(callable));
    }

    for (Future<Void> task : tasks) {
      task.get();
    }

    tasks.clear();
    stream.close();

    es.shutdownNow();
    FileStatus fileStatus = fs.getFileStatus(new Path("testfile"));
    assertEquals(2048000000, fileStatus.getLen());
  }

  @Test
  public void testBase64FileSystemProperties() throws Exception {
    Configuration configuration = ServiceProviderImpl.instance().get(ConfigurationService.class).getConfiguration();
    final Hashtable<String, String> properties = new Hashtable<>();
    properties.put("key", "{ value: value }");
    final AzureDistributedFileSystem fs = (AzureDistributedFileSystem) FileSystem.get(configuration);
    ServiceProviderImpl.instance().get(AdfsHttpService.class).setFilesystemProperties(
        fs, properties);
    Hashtable<String, String> fetchedProperties = ServiceProviderImpl.instance().get(AdfsHttpService.class).getFilesystemProperties(fs);

    Assert.assertEquals(properties, fetchedProperties);
  }

  @Test
  public void testBase64PathProperties() throws Exception {
    Configuration configuration = ServiceProviderImpl.instance().get(ConfigurationService.class).getConfiguration();
    final Hashtable<String, String> properties = new Hashtable<>();
    properties.put("key", "{ value: valueTest }");
    final AzureDistributedFileSystem fs = (AzureDistributedFileSystem) FileSystem.get(configuration);
    fs.create(new Path("/testpath"));
    ServiceProviderImpl.instance().get(AdfsHttpService.class).setPathProperties(
        fs, new Path("/testpath"), properties);
    Hashtable<String, String> fetchedProperties =
        ServiceProviderImpl.instance().get(AdfsHttpService.class).getPathProperties(fs, new Path("/testpath"));

    Assert.assertEquals(properties, fetchedProperties);
  }

  @Test (expected = Exception.class)
  public void testBase64InvalidFileSystemProperties() throws Exception {
    Configuration configuration = ServiceProviderImpl.instance().get(ConfigurationService.class).getConfiguration();
    final Hashtable<String, String> properties = new Hashtable<>();
    properties.put("key", "{ value: value歲 }");
    final AzureDistributedFileSystem fs = (AzureDistributedFileSystem) FileSystem.get(configuration);
    ServiceProviderImpl.instance().get(AdfsHttpService.class).setFilesystemProperties(
        fs, properties);
    Hashtable<String, String> fetchedProperties = ServiceProviderImpl.instance().get(AdfsHttpService.class).getFilesystemProperties(fs);

    Assert.assertEquals(properties, fetchedProperties);
  }

  @Test (expected = Exception.class)
  public void testBase64InvalidPathProperties() throws Exception {
    Configuration configuration = ServiceProviderImpl.instance().get(ConfigurationService.class).getConfiguration();
    final Hashtable<String, String> properties = new Hashtable<>();
    properties.put("key", "{ value: valueTest兩 }");
    final AzureDistributedFileSystem fs = (AzureDistributedFileSystem) FileSystem.get(configuration);
    fs.create(new Path("/testpath"));
    ServiceProviderImpl.instance().get(AdfsHttpService.class).setPathProperties(
        fs, new Path("/testpath"), properties);
    Hashtable<String, String> fetchedProperties =
        ServiceProviderImpl.instance().get(AdfsHttpService.class).getPathProperties(fs, new Path("/testpath"));

    Assert.assertEquals(properties, fetchedProperties);
  }

  @Test(expected = FileNotFoundException.class)
  public void testDeleteDirectory() throws Exception {
    final Configuration configuration = ServiceProviderImpl.instance().get(ConfigurationService.class).getConfiguration();
    final AzureDistributedFileSystem fs = (AzureDistributedFileSystem) FileSystem.get(configuration);
    fs.mkdirs(new Path("testfile"));
    fs.mkdirs(new Path("testfile/test1"));
    fs.mkdirs(new Path("testfile/test1/test2"));

    fs.delete(new Path("testfile"), true);
    fs.getFileStatus(new Path("testfile"));
  }

  @Test(expected = FileNotFoundException.class)
  public void testRenameDirectory() throws Exception {
    Configuration configuration = ServiceProviderImpl.instance().get(ConfigurationService.class).getConfiguration();
    final AzureDistributedFileSystem fs = (AzureDistributedFileSystem) FileSystem.get(configuration);
    fs.mkdirs(new Path("testDir"));
    fs.mkdirs(new Path("testDir/test1"));
    fs.mkdirs(new Path("testDir/test1/test2"));
    fs.mkdirs(new Path("testDir/test1/test2/test3"));

    fs.rename(new Path("testDir/test1"), new Path("testDir/test10"));
    fs.getFileStatus(new Path("testDir/test1"));
  }

  @Test(expected = FileNotFoundException.class)
  public void testRenameRootDirectory() throws Exception {
    Configuration configuration = ServiceProviderImpl.instance().get(ConfigurationService.class).getConfiguration();
    final AzureDistributedFileSystem fs = (AzureDistributedFileSystem) FileSystem.get(configuration);
    final List<Future> tasks = new ArrayList<>();

    ExecutorService es = Executors.newFixedThreadPool(10);
    for (int i = 0; i < 1000; i++) {
      final Path fileName = new Path("/test/" + i);
      Callable<Void> callable = new Callable<Void>() {
        @Override
        public Void call() throws Exception {
          fs.create(fileName);
          return null;
        }
      };

      tasks.add(es.submit(callable));
    }

    for (Future<Void> task : tasks) {
      task.get();
    }

    es.shutdownNow();
    fs.rename(new Path("/test"), new Path("/renamedDir"));

    FileStatus[] files = fs.listStatus(new Path("/renamedDir"));
    Assert.assertEquals(files.length, 1000);
    fs.getFileStatus(new Path("/test"));
  }

  @Test(expected = FileNotFoundException.class)
  public void testDeleteRootDirectory() throws Exception {
    Configuration configuration = ServiceProviderImpl.instance().get(ConfigurationService.class).getConfiguration();
    final AzureDistributedFileSystem fs = (AzureDistributedFileSystem) FileSystem.get(configuration);
    final List<Future> tasks = new ArrayList<>();

    ExecutorService es = Executors.newFixedThreadPool(10);
    for (int i = 0; i < 1000; i++) {
      final Path fileName = new Path("/test/" + i);
      Callable<Void> callable = new Callable<Void>() {
        @Override
        public Void call() throws Exception {
          fs.create(fileName);
          return null;
        }
      };

      tasks.add(es.submit(callable));
    }

    for (Future<Void> task : tasks) {
      task.get();
    }

    es.shutdownNow();
    fs.delete(new Path("/test"), true);
    fs.getFileStatus(new Path("/test"));
  }

  @Test
  public void testRenameRoot() throws Exception {
    Configuration configuration = ServiceProviderImpl.instance().get(ConfigurationService.class).getConfiguration();
    final AzureDistributedFileSystem fs = (AzureDistributedFileSystem) FileSystem.get(configuration);
    boolean renamed = fs.rename(new Path("/"), new Path("/ddd"));
    assertFalse(renamed);

    renamed = fs.rename(new Path(fs.getUri().toString() + "/"), new Path(fs.getUri().toString() + "/s"));
    assertFalse(renamed);
  }

  @Test (expected = IOException.class)
  public void testOOBWrites() throws Exception {
    Configuration configuration = ServiceProviderImpl.instance().get(ConfigurationService.class).getConfiguration();
    final AzureDistributedFileSystem fs = (AzureDistributedFileSystem) FileSystem.get(configuration);
    int readBufferSize = ServiceProviderImpl.instance().get(ConfigurationService.class).getReadBufferSize();

    fs.create(new Path("testfile"));
    FSDataOutputStream writeStream = fs.create(new Path("testfile"));

    byte[] bytesToRead = new byte[readBufferSize];
    final byte[] b = new byte[2 * readBufferSize];
    new Random().nextBytes(b);

    writeStream.write(b);
    writeStream.flush();
    writeStream.close();

    FSDataInputStream readStream = fs.open(new Path("testfile"));
    readStream.read(bytesToRead, 0, readBufferSize);

    writeStream = fs.create(new Path("testfile"));
    writeStream.write(b);
    writeStream.flush();
    writeStream.close();

    readStream.read(bytesToRead, 0, readBufferSize);
    readStream.close();
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