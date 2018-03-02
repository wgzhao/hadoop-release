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

import org.junit.Test;

import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;

import static org.junit.Assert.assertNotNull;

public class ITestAzureDistributedFileSystemCreate extends DependencyInjectedTest {
  public ITestAzureDistributedFileSystemCreate() throws Exception {
    super();
  }

  @Test(expected = FileAlreadyExistsException.class)
  public void testCreateFileWithExistingDir() throws Exception {
    final AzureDistributedFileSystem fs = this.getFileSystem();
    fs.mkdirs(new Path("testFolder"));
    fs.create(new Path("testFolder"));
  }

  @Test
  public void testEnsureFileCreated() throws Exception {
    final AzureDistributedFileSystem fs = this.getFileSystem();
    fs.create(new Path("testfile"));

    FileStatus fileStatus = fs.getFileStatus(new Path("testfile"));
    assertNotNull(fileStatus);
  }
}
