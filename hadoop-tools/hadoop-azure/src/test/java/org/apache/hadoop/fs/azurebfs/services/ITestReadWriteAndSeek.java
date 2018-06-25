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

package org.apache.hadoop.fs.azurebfs.services;

import java.util.Random;

import org.junit.Test;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.azurebfs.AzureBlobFileSystem;
import org.apache.hadoop.fs.azurebfs.DependencyInjectedTest;
import org.apache.hadoop.fs.azurebfs.contracts.services.ConfigurationService;

import static org.apache.hadoop.fs.azurebfs.constants.FileSystemConfigurations.*;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertNotEquals;

public class ITestReadWriteAndSeek extends DependencyInjectedTest {
  public ITestReadWriteAndSeek() throws Exception {
    super();
  }

  @Test
  public void testReadAndWriteWithDifferentBufferSizesAndSeek() throws Exception {
    testReadWriteAndSeek(MIN_BUFFER_SIZE);
    testReadWriteAndSeek(DEFAULT_READ_BUFFER_SIZE);
    testReadWriteAndSeek(MAX_BUFFER_SIZE);
  }

  private void testReadWriteAndSeek(int bufferSize) throws Exception {
    final AzureBlobFileSystem fs = this.getFileSystem();
    final ConfigurationServiceImpl configurationservice = (ConfigurationServiceImpl)fs.getConfigurationService();

    fs.create(new Path("testfile"));

    configurationservice.setWriteBufferSize(bufferSize);
    configurationservice.setReadBufferSize(bufferSize);

    final FSDataOutputStream stream = fs.create(new Path("testfile"));

    final byte[] b = new byte[2 * bufferSize];
    new Random().nextBytes(b);
    stream.write(b);
    stream.close();

    final byte[] r = new byte[2 * bufferSize];
    final FSDataInputStream inputStream = fs.open(new Path("testfile"));
    inputStream.seek(bufferSize);
    int result = inputStream.read(r, bufferSize, bufferSize);
    assertNotEquals(-1, result);

    inputStream.seek(0);
    result = inputStream.read(r, 0, bufferSize);
    assertNotEquals(-1, result);
    assertArrayEquals(r, b);
  }
}
