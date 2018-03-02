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

import java.io.FileNotFoundException;
import java.util.Random;

import org.junit.Test;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;

import static org.junit.Assert.assertEquals;

public class ITestAzureDistributedFileSystemAppend extends DependencyInjectedTest {
  public ITestAzureDistributedFileSystemAppend() throws Exception {
    super();
  }

  @Test(expected = FileNotFoundException.class)
  public void testAppendDirShouldFail() throws Exception {
    final AzureDistributedFileSystem fs = this.getFileSystem();
    fs.mkdirs(new Path("testfile"));
    fs.append(new Path("testfile"), 0);
  }

  @Test
  public void testAppendWithLength0() throws Exception {
    final AzureDistributedFileSystem fs = this.getFileSystem();
    FSDataOutputStream stream = fs.create(new Path("testFile"));
    final byte[] b = new byte[10240000];
    new Random().nextBytes(b);
    stream.write(b, 1000, 0);

    assertEquals(0, stream.getPos());
  }


  @Test(expected = FileNotFoundException.class)
  public void testAppendFileAfterDelete() throws Exception {
    final AzureDistributedFileSystem fs = this.getFileSystem();
    fs.create(new Path("/testFile"));
    fs.delete(new Path("/testFile"), false);

    fs.append(new Path("/testFile"));
  }

  @Test(expected = FileNotFoundException.class)
  public void testAppendDirectory() throws Exception {
    final AzureDistributedFileSystem fs = this.getFileSystem();
    fs.mkdirs(new Path("testFolder"));
    fs.append(new Path("testFolder"));
  }
}
