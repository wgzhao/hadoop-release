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

import org.junit.Test;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;

import static org.junit.Assert.assertEquals;

public class ITestAzureBlobFileSystemFileStatus extends DependencyInjectedTest {
  private static final String DEFAULT_FILE_PERMISSION_VALUE = "640";
  private static final String DEFAULT_DIR_PERMISSION_VALUE = "750";
  private static final String DEFAULT_PERMISSION_VALUE = "777";

  private final String filePermissions;
  private final String dirPermissions;

  public ITestAzureBlobFileSystemFileStatus() throws Exception {
    super();
    this.filePermissions = this.isNamespaceEnabled() ? DEFAULT_FILE_PERMISSION_VALUE : DEFAULT_PERMISSION_VALUE;
    this.dirPermissions = this.isNamespaceEnabled() ? DEFAULT_DIR_PERMISSION_VALUE : DEFAULT_PERMISSION_VALUE;
  }

  @Test
  public void testEnsureStatusWorksForRoot() throws Exception {
    final AzureBlobFileSystem fs = this.getFileSystem();

    fs.getFileStatus(new Path("/"));
    fs.listStatus(new Path("/"));
  }

  @Test
  public void testFileStatusPermissionsAndOwnerAndGroup() throws Exception {
    final AzureBlobFileSystem fs = this.getFileSystem();
    fs.create(new Path("/testFile"));
    fs.mkdirs(new Path("/testDir"));

    FileStatus fileStatus = fs.getFileStatus(new Path("/testFile"));
    assertEquals(new FsPermission(filePermissions), fileStatus.getPermission());
    assertEquals(fs.getOwnerUser(), fileStatus.getOwner());
    assertEquals(fs.getOwnerUserPrimaryGroup(), fileStatus.getGroup());

    fileStatus = fs.getFileStatus(new Path("/testDir"));
    assertEquals(new FsPermission(dirPermissions), fileStatus.getPermission());
    assertEquals(fs.getOwnerUser(), fileStatus.getOwner());
    assertEquals(fs.getOwnerUserPrimaryGroup(), fileStatus.getGroup());
  }
}
