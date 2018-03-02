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

import org.junit.Ignore;
import org.junit.Test;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

public class ITestAzureDistributedFileSystemFlush extends DependencyInjectedTest {
  public ITestAzureDistributedFileSystemFlush() throws Exception {
    super();
  }

  @Test
  @Ignore(value = "Due to bug in the service")
  public void testAdfsOutputStreamAsyncFlushWithRetainUncommitedData() throws Exception {
    final AzureDistributedFileSystem fs = this.getFileSystem();
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

    inputStream.close();
  }

  @Test
  public void testAdfsOutputStreamSyncFlush() throws Exception {
    final AzureDistributedFileSystem fs = this.getFileSystem();
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

    inputStream.close();
  }


  @Test
  public void testWriteHeavyBytesToFileSyncFlush() throws Exception {
    final AzureDistributedFileSystem fs = this.getFileSystem();
    final FSDataOutputStream stream = fs.create(new Path("testfile"));
    final FileSystem.Statistics adfsStatistics = fs.getFsStatistics();
    adfsStatistics.reset();

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
    FileStatus fileStatus = fs.getFileStatus(new Path("testfile"));
    assertEquals(4096000000l, fileStatus.getLen());
    assertEquals(4096000000l, adfsStatistics.getBytesWritten());
  }

  @Test
  @Ignore(value = "Due to bug in the service")
  public void testWriteHeavyBytesToFileAsyncFlush() throws Exception {
    final AzureDistributedFileSystem fs = this.getFileSystem();
    fs.create(new Path("testfile"));
    final FSDataOutputStream stream = fs.create(new Path("testfile"));
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
    FileStatus fileStatus = fs.getFileStatus(new Path("testfile"));
    assertEquals(4096000000l, fileStatus.getLen());
  }
}
