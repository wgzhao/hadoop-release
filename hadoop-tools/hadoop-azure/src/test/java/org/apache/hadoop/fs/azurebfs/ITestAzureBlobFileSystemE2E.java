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
import java.util.Arrays;
import java.util.Random;


import org.junit.Test;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.azurebfs.contracts.services.ConfigurationService;
import org.apache.hadoop.fs.azurebfs.services.ServiceProviderImpl;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertArrayEquals;

public class ITestAzureBlobFileSystemE2E extends DependencyInjectedTest {
  public ITestAzureBlobFileSystemE2E() throws Exception {
    super();
    Configuration configuration = this.getConfiguration();
    configuration.set(ConfigurationKeys.FS_AZURE_READ_AHEAD_QUEUE_DEPTH, "0");
    this.mockServiceInjector.replaceInstance(Configuration.class, configuration);

  }

  @Test
  public void testWriteOneByteToFile() throws Exception {
    final AzureBlobFileSystem fs = this.getFileSystem();
    FSDataOutputStream stream = fs.create(new Path("testfile"));

    stream.write(100);
    stream.close();

    FileStatus fileStatus = fs.getFileStatus(new Path("testfile"));
    assertEquals(1, fileStatus.getLen());
  }

  @Test
  public void testReadWriteBytesToFile() throws Exception {
    final AzureBlobFileSystem fs = this.getFileSystem();
    testWriteOneByteToFile();

    FSDataInputStream inputStream = fs.open(new Path("testfile"), 4 * 1024 * 1024);
    int i = inputStream.read();
    inputStream.close();

    assertEquals(100, i);
  }

  @Test (expected = IOException.class)
  public void testOOBWrites() throws Exception {
    final AzureBlobFileSystem fs = this.getFileSystem();
    int readBufferSize = fs.getConfigurationService().getReadBufferSize();

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

  @Test
  public void testWriteWithBufferOffset() throws Exception {
    final AzureBlobFileSystem fs = this.getFileSystem();
    final FSDataOutputStream stream = fs.create(new Path("/testfile"));

    final byte[] b = new byte[1024000];
    new Random().nextBytes(b);
    stream.write(b, 100, b.length - 100);
    stream.close();

    final byte[] r = new byte[1023900];
    FSDataInputStream inputStream = fs.open(new Path("/testfile"), 4 * 1024 * 1024);
    int result = inputStream.read(r);

    assertNotEquals(-1, result);
    assertArrayEquals(r, Arrays.copyOfRange(b, 100, b.length));

    inputStream.close();
  }

  @Test
  public void testReadWriteHeavyBytesToFileWithSmallerChunks() throws Exception {
    final AzureBlobFileSystem fs = this.getFileSystem();
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
    inputStream.close();
  }
}
