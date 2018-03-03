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
package org.apache.hadoop.fs.azuredfs.contract;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.azuredfs.constants.TestConfigurationKeys;
import org.junit.Ignore;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;

public class TestAzureDistributedFileSystemContract extends FileSystemContractBaseTest {
  @Override
  protected void setUp() throws Exception {
    Configuration configuration = new Configuration();
    configuration.addResource("azure-adfs-test.xml");
    URI testUri = new URI(configuration.get(TestConfigurationKeys.FS_AZURE_CONTRACT_TEST_URI));
    configuration.set(CommonConfigurationKeysPublic.FS_DEFAULT_NAME_KEY, testUri.toString());
    fs = FileSystem.get(configuration);
    super.setUp();
  }

  @Override
  protected void tearDown() throws Exception {
    super.tearDown();
  }

  @Test
  public void testListOnFolderWithNoChilren() throws IOException {
    assertTrue(fs.mkdirs(path("testListStatus/c/1")));

    FileStatus[] paths;
    paths = fs.listStatus(path("testListStatus"));
    assertEquals(1, paths.length);

    // ListStatus on folder with child
    paths = fs.listStatus(path("testListStatus/c"));
    assertEquals(1, paths.length);

    // Remove the child and listStatus
    fs.delete(path("testListStatus/c/1"), true);
    paths = fs.listStatus(path("testListStatus/c"));
    assertEquals(0, paths.length);
    assertTrue(fs.delete(path("testListStatus"), true));
  }

  @Test
  public void testListOnfileAndFolder() throws IOException {
    Path folderPath = path("testListStatus/folder");
    Path filePath = path("testListStatus/file");

    assertTrue(fs.mkdirs(folderPath));
    fs.create(filePath);

    FileStatus[] listFolderStatus;
    listFolderStatus = fs.listStatus(path("testListStatus"));
    assertEquals(filePath, listFolderStatus[0].getPath());

    //List on file should return absolute path
    FileStatus[] listFileStatus = fs.listStatus(filePath);
    assertEquals(filePath, listFileStatus[0].getPath());
  }

  @Override
  @Ignore("Not implemented in ADFS yet")
  public void testMkdirsWithUmask() throws Exception {
  }

  @Override
  @Ignore("A bug has been created to track this issue")
  public void testRenameFileMoveToNonExistentDirectory() throws Exception {
  }

  @Override
  @Ignore("A bug has been created to track this issue")
  public void testRenameDirectoryMoveToNonExistentDirectory() throws Exception {
  }

}