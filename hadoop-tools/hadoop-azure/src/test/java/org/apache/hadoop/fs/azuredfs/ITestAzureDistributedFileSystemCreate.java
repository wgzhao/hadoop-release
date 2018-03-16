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
import java.util.EnumSet;

import org.junit.Test;

import org.apache.hadoop.fs.CreateFlag;
import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

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

  @Test
  @SuppressWarnings("deprecation")
  public void testCreateNonRecursive() throws Exception {
    final AzureDistributedFileSystem fs = this.getFileSystem();
    Path testFolder = new Path("/testFolder");
    Path testFile = new Path(testFolder, "testFile");
    try {
      fs.createNonRecursive(testFile, true, 1024, (short)1, 1024, null);
      assertTrue("Should've thrown", false);
    } catch (FileNotFoundException e) {
    }
    fs.mkdirs(testFolder);
    fs.createNonRecursive(testFile, true, 1024, (short)1, 1024, null)
        .close();
    assertTrue(fs.exists(testFile));
  }

  @Test
  @SuppressWarnings("deprecation")
  public void testCreateNonRecursive1() throws Exception {
    final AzureDistributedFileSystem fs = this.getFileSystem();
    Path testFolder = new Path("/testFolder");
    Path testFile = new Path(testFolder, "testFile");
    try {
      fs.createNonRecursive(testFile, FsPermission.getDefault(), EnumSet.of(CreateFlag.CREATE, CreateFlag.OVERWRITE), 1024, (short)1, 1024, null);
      assertTrue("Should've thrown", false);
    } catch (FileNotFoundException e) {
    }
    fs.mkdirs(testFolder);
    fs.createNonRecursive(testFile, true, 1024, (short)1, 1024, null)
        .close();
    assertTrue(fs.exists(testFile));
  }

  @Test
  @SuppressWarnings("deprecation")
  public void testCreateNonRecursive2() throws Exception {
    final AzureDistributedFileSystem fs = this.getFileSystem();
    Path testFolder = new Path("/testFolder");
    Path testFile = new Path(testFolder, "testFile");
    try {
      fs.createNonRecursive(testFile, FsPermission.getDefault(), false, 1024, (short)1, 1024, null);
      assertTrue("Should've thrown", false);
    } catch (FileNotFoundException e) {
    }
    fs.mkdirs(testFolder);
    fs.createNonRecursive(testFile, true, 1024, (short)1, 1024, null)
        .close();
    assertTrue(fs.exists(testFile));
  }
}
