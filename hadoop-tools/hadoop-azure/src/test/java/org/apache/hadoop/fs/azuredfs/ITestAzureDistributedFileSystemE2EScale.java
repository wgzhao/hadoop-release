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

import org.junit.Assert;
import org.junit.Test;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

public class ITestAzureDistributedFileSystemE2EScale extends DependencyInjectedTest {
  public ITestAzureDistributedFileSystemE2EScale() throws Exception {
    super();
  }

  @Test
  public void testWriteHeavyBytesToFile() throws Exception {
    final AzureDistributedFileSystem fs = this.getFileSystem();
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
  public void testReadWriteHeavyBytesToFile() throws Exception {
    final AzureDistributedFileSystem fs = this.getFileSystem();
    final FSDataOutputStream stream = fs.create(new Path("testfile"));

    final byte[] b = new byte[5 * 10240000];
    new Random().nextBytes(b);
    stream.write(b);
    stream.close();

    final byte[] r = new byte[5 * 10240000];
    FSDataInputStream inputStream = fs.open(new Path("testfile"), 4 * 1024 * 1024);
    int result = inputStream.read(r);
    inputStream.close();

    assertNotEquals(-1, result);
    assertArrayEquals(r, b);
  }

  @Test
  public void testReadWriteHeavyBytesToFileWithStatistics() throws Exception {
    final AzureDistributedFileSystem fs = this.getFileSystem();
    final FSDataOutputStream stream = fs.create(new Path("testfile"));
    final FileSystem.Statistics adfsStatistics = fs.getFsStatistics();
    adfsStatistics.reset();

    final byte[] b = new byte[5 * 10240000];
    new Random().nextBytes(b);
    stream.write(b);
    stream.close();

    final byte[] r = new byte[5 * 10240000];
    FSDataInputStream inputStream = fs.open(new Path("testfile"), 4 * 1024 * 1024);
    inputStream.read(r);
    inputStream.close();

    Assert.assertEquals(r.length, adfsStatistics.getBytesRead());
    Assert.assertEquals(b.length, adfsStatistics.getBytesWritten());
  }
}
