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

import java.io.IOException;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import com.microsoft.azure.storage.blob.BlockEntry;
import com.microsoft.azure.storage.blob.BlockListingFilter;
import com.microsoft.azure.storage.blob.CloudBlockBlob;
import org.hamcrest.core.IsEqual;
import org.hamcrest.core.IsNot;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.azure.AzureBlobStorageTestAccount;
import org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import org.junit.Test;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertThat;

public class ITestAzureBlobFileSystemFlush extends DependencyInjectedTest {
  private static final String TEST_FILE = "/testfile";
  private static final int TEST_FILE_LENGTH = 1024 * 1024 * 4;

  public ITestAzureBlobFileSystemFlush() throws Exception {
    super();
  }

  @Test
  public void testAbfsOutputStreamAsyncFlushWithRetainUncommitedData() throws Exception {
    final AzureBlobFileSystem fs = this.getFileSystem();
    final FSDataOutputStream stream = fs.create(new Path(TEST_FILE));

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
    FSDataInputStream inputStream = fs.open(new Path(TEST_FILE), 4 * 1024 * 1024);

    while (inputStream.available() != 0) {
      int result = inputStream.read(r);

      assertNotEquals(-1, result);
      assertArrayEquals(r, b);
    }

    inputStream.close();
  }

  @Test
  public void testAbfsOutputStreamSyncFlush() throws Exception {
    final AzureBlobFileSystem fs = this.getFileSystem();
    final FSDataOutputStream stream = fs.create(new Path(TEST_FILE));

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
    FSDataInputStream inputStream = fs.open(new Path(TEST_FILE), 4 * 1024 * 1024);
    int result = inputStream.read(r);

    assertNotEquals(-1, result);
    assertArrayEquals(r, b);

    inputStream.close();
  }


  @Test
  public void testWriteHeavyBytesToFileSyncFlush() throws Exception {
    final AzureBlobFileSystem fs = this.getFileSystem();
    final FSDataOutputStream stream = fs.create(new Path(TEST_FILE));
    final FileSystem.Statistics abfsStatistics = fs.getFsStatistics();
    abfsStatistics.reset();

    ExecutorService es = Executors.newFixedThreadPool(10);

    final byte[] b = new byte[2 * 10240000];
    new Random().nextBytes(b);

    List<Future<Void>> tasks = new ArrayList<>();
    for (int i = 0; i < 200; i++) {
      Callable<Void> callable = new Callable<Void>() {
        @Override
        public Void call() throws Exception {
          stream.write(b);
          return null;
        }
      };

      tasks.add(es.submit(callable));
    }

    boolean shouldStop = false;
    while (!shouldStop) {
      shouldStop = true;
      for (Future<Void> task : tasks) {
        if (!task.isDone()) {
          stream.hsync();
          shouldStop = false;
          Thread.sleep(60000);
        }
      }
    }

    tasks.clear();
    stream.close();

    es.shutdownNow();
    FileStatus fileStatus = fs.getFileStatus(new Path(TEST_FILE));
    assertEquals(4096000000l, fileStatus.getLen());
    assertEquals(4096000000l, abfsStatistics.getBytesWritten());
  }

  @Test
  public void testWriteHeavyBytesToFileAsyncFlush() throws Exception {
    final AzureBlobFileSystem fs = this.getFileSystem();
    fs.create(new Path(TEST_FILE));
    final FSDataOutputStream stream = fs.create(new Path(TEST_FILE));
    ExecutorService es = Executors.newFixedThreadPool(10);

    final byte[] b = new byte[2 * 10240000];
    new Random().nextBytes(b);

    List<Future<Void>> tasks = new ArrayList<>();
    for (int i = 0; i < 200; i++) {
      Callable<Void> callable = new Callable<Void>() {
        @Override
        public Void call() throws Exception {
          stream.write(b);
          return null;
        }
      };

      tasks.add(es.submit(callable));
    }

    boolean shouldStop = false;
    while (!shouldStop) {
      shouldStop = true;
      for (Future<Void> task : tasks) {
        if (!task.isDone()) {
          stream.flush();
          shouldStop = false;
        }
      }
    }

    tasks.clear();
    stream.close();

    es.shutdownNow();
    FileStatus fileStatus = fs.getFileStatus(new Path(TEST_FILE));
    assertEquals(4096000000l, fileStatus.getLen());
  }

  @Test
  public void testFlushWithFlushEnabled() throws Exception {
    setEnableFlush(true);

    AzureBlobStorageTestAccount testAccount = createWasbTestAccount();
    String wasbUrl = testAccount.getFileSystem().getName();
    String abfsUrl = wasbUrlToAbfsUrl(wasbUrl);

    final AzureBlobFileSystem fs = this.getFileSystem(abfsUrl);
    Path path = new Path(TEST_FILE);
    byte[] buffer = getRandomBytesArray();

    CloudBlockBlob blob = testAccount.getBlobReference(path.toString().substring(1));

    try (FSDataOutputStream stream = getStreamAfterWrite(fs, path, buffer)) {
      // Wait for write request to be executed
      Thread.sleep(3000);
      stream.flush();
      ArrayList<BlockEntry> blockList = blob.downloadBlockList(
              BlockListingFilter.COMMITTED, null,null, null);
      // verify block has been committed
      assertEquals(1, blockList.size());
    }
  }

  @Test
  public void testFlushWithFlushDisabled() throws Exception {
    setEnableFlush(false);

    AzureBlobStorageTestAccount testAccount = createWasbTestAccount();
    String wasbUrl = testAccount.getFileSystem().getName();
    String abfsUrl = wasbUrlToAbfsUrl(wasbUrl);

    final AzureBlobFileSystem fs = this.getFileSystem(abfsUrl);
    Path path = new Path(TEST_FILE);
    byte[] buffer = getRandomBytesArray();

    CloudBlockBlob blob = testAccount.getBlobReference(path.toString().substring(1));

    try (FSDataOutputStream stream = getStreamAfterWrite(fs, path, buffer)) {
      // Wait for write request to be executed
      Thread.sleep(3000);
      stream.flush();
      ArrayList<BlockEntry> blockList = blob.downloadBlockList(
              BlockListingFilter.COMMITTED, null,null, null);
      // verify block has not been committed
      assertEquals(0, blockList.size());
    }
  }

  @Test
  public void testHflushWithFlushEnabled() throws Exception {
    setEnableFlush(true);

    final AzureBlobFileSystem fs = this.getFileSystem();
    Path path = new Path(TEST_FILE);
    byte[] buffer = getRandomBytesArray();

    try (FSDataOutputStream stream = getStreamAfterWrite(fs, path, buffer)) {
      stream.hflush();
      validate(fs, path, buffer,true);
    }
  }

  @Test
  public void testHflushWithFlushDisabled() throws Exception {
    setEnableFlush(false);

    final AzureBlobFileSystem fs = this.getFileSystem();
    Path path = new Path(TEST_FILE);
    byte[] buffer = getRandomBytesArray();

    try (FSDataOutputStream stream = getStreamAfterWrite(fs, path, buffer)) {
      stream.hflush();
      validate(fs, path, buffer,false);
    }
  }

  @Test
  public void testSyncWithFlushEnabled() throws Exception {
    setEnableFlush(true);

    final AzureBlobFileSystem fs = this.getFileSystem();
    Path path = new Path(TEST_FILE);
    byte[] buffer = getRandomBytesArray();

    try (FSDataOutputStream stream = getStreamAfterWrite(fs, path, buffer)) {
      stream.sync();
      validate(fs, path, buffer,true);
    }
  }

  @Test
  public void testSyncWithFlushDisabled() throws Exception {
    setEnableFlush(false);

    final AzureBlobFileSystem fs = this.getFileSystem();
    Path path = new Path(TEST_FILE);
    byte[] buffer = getRandomBytesArray();

    try (FSDataOutputStream stream = getStreamAfterWrite(fs, path, buffer)) {
      stream.sync();
      validate(fs, path, buffer,false);
    }
  }

  @Test
  public void testHsyncWithFlushEnabled() throws Exception {
    setEnableFlush(true);

    final AzureBlobFileSystem fs = this.getFileSystem();
    Path path = new Path(TEST_FILE);
    byte[] buffer = getRandomBytesArray();

    try (FSDataOutputStream stream = getStreamAfterWrite(fs, path, buffer)) {
      stream.hsync();
      validate(fs, path, buffer,true);
    }
  }

  @Test
  public void testHsyncWithFlushDisabled() throws Exception {
    setEnableFlush(false);

    final AzureBlobFileSystem fs = this.getFileSystem();
    Path path = new Path(TEST_FILE);
    byte[] buffer = getRandomBytesArray();

    try (FSDataOutputStream stream = getStreamAfterWrite(fs, path, buffer)) {
      stream.hsync();
      validate(fs, path, buffer,false);
    }

  }

  private void setEnableFlush(boolean enableFlush) {
    Configuration configuration = this.getConfiguration();
    configuration.setBoolean(ConfigurationKeys.FS_AZURE_ENABLE_FLUSH, enableFlush);
    this.mockServiceInjector.replaceInstance(Configuration.class, configuration);
  }

  private byte[] getRandomBytesArray() {
    final byte[] b = new byte[TEST_FILE_LENGTH];
    new Random().nextBytes(b);
    return b;
  }

  private FSDataOutputStream getStreamAfterWrite(FileSystem fs, Path path, byte[] buffer) throws IOException {
    FSDataOutputStream stream = fs.create(path);
    stream.write(buffer);
    return stream;
  }

  private AzureBlobStorageTestAccount createWasbTestAccount() throws Exception {
    return AzureBlobStorageTestAccount.create("", EnumSet.of(AzureBlobStorageTestAccount.CreateOptions.CreateContainer),
    this.getConfiguration());
  }

  private void validate(FileSystem fs, Path path, byte[] writeBuffer, boolean isEqual) throws IOException {
    String filePath = path.toUri().toString();

    try (FSDataInputStream inputStream = fs.open(path)) {
      byte[] readBuffer = new byte[TEST_FILE_LENGTH];
      int numBytesRead = inputStream.read(readBuffer, 0, readBuffer.length);
      if (isEqual) {
        assertArrayEquals(
                String.format("Bytes read do not match bytes written to %1$s", filePath), writeBuffer, readBuffer);
      } else {
        assertThat(
                String.format("Bytes read unexpectedly match bytes written to %1$s",
                        filePath),
                readBuffer,
                IsNot.not(IsEqual.equalTo(writeBuffer)));
      }
    }
  }

}
