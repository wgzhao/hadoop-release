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
package org.apache.hadoop.hdfs.server.namenode;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.protocol.HdfsConstants.SafeModeAction;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.*;

public class TestLeaseManager {
  @Rule
  public Timeout timeout = new Timeout(300000);

  @Test
  public void testRemoveLeaseWithPrefixPath() throws Exception {
    final Configuration conf = new HdfsConfiguration();
    MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).numDataNodes(2).build();
    cluster.waitActive();

    LeaseManager lm = NameNodeAdapter.getLeaseManager(cluster.getNamesystem());
    lm.addLease("holder1", "/a/b");
    lm.addLease("holder2", "/a/c");
    assertNotNull(lm.getLeaseByPath("/a/b"));
    assertNotNull(lm.getLeaseByPath("/a/c"));

    lm.removeLeaseWithPrefixPath("/a");

    assertNull(lm.getLeaseByPath("/a/b"));
    assertNull(lm.getLeaseByPath("/a/c"));

    lm.addLease("holder1", "/a/b");
    lm.addLease("holder2", "/a/c");

    lm.removeLeaseWithPrefixPath("/a/");

    assertNull(lm.getLeaseByPath("/a/b"));
    assertNull(lm.getLeaseByPath("/a/c"));
  }

  /** Check that even if LeaseManager.checkLease is not able to relinquish
   * leases, the Namenode does't enter an infinite loop while holding the FSN
   * write lock and thus become unresponsive
   */
  @Test
  public void testCheckLeaseNotInfiniteLoop() {
    LeaseManager lm = new LeaseManager(makeMockFsNameSystem());

    //Make sure the leases we are going to add exceed the hard limit
    lm.setLeasePeriod(0,0);

    //Add some leases to the LeaseManager
    lm.addLease("holder1", "src1");
    lm.addLease("holder2", "src2");
    lm.addLease("holder3", "src3");
    assertEquals(lm.getNumSortedLeases(), 3);

    //Initiate a call to checkLease. This should exit within the test timeout
    lm.checkLeases();
  }

  /**
   * Make sure the lease is restored even if only the inode has the record.
   */
  @Test
  public void testLeaseRestorationOnRestart() throws Exception {
    MiniDFSCluster cluster = null;
    try {
      cluster = new MiniDFSCluster.Builder(new HdfsConfiguration())
          .numDataNodes(1).build();
      DistributedFileSystem dfs = cluster.getFileSystem();

      // Create an empty file
      String path = "/testLeaseRestorationOnRestart";
      FSDataOutputStream out = dfs.create(new Path(path));

      // Remove the lease from the lease manager, but leave it in the inode.
      FSDirectory dir = cluster.getNamesystem().getFSDirectory();
      INodeFile file = dir.getINode(path).asFile();
      cluster.getNamesystem().leaseManager.removeLease(
          file.getFileUnderConstructionFeature().getClientName(), path);

      // Save a fsimage.
      dfs.setSafeMode(SafeModeAction.SAFEMODE_ENTER);
      cluster.getNameNodeRpc().saveNamespace();
      dfs.setSafeMode(SafeModeAction.SAFEMODE_LEAVE);

      // Restart the namenode.
      cluster.restartNameNode(true);

      // Check whether the lease manager has the lease
      assertNotNull("Lease should exist",
          cluster.getNamesystem().leaseManager.getLeaseByPath(path));
    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }

  @Test
  public void testCountPath() {
    LeaseManager lm = new LeaseManager(makeMockFsNameSystem());

    lm.addLease("holder1", "/path1");
    assertThat(lm.countPath(), is(1));

    lm.addLease("holder2", "/path2");
    assertThat(lm.countPath(), is(2));
    lm.addLease("holder2", "/path2");                   // Duplicate addition
    assertThat(lm.countPath(), is(2));

    assertThat(lm.countPath(), is(2));

    // Remove a couple of non-existing leases. countPath should not change.
    lm.removeLease("holder1", "/path3");
    lm.removeLease("InvalidLeaseHolder", "/path1");
    assertThat(lm.countPath(), is(2));

    lm.reassignLease(lm.getLease("holder1"), "/path1", "holder2");
    assertThat(lm.countPath(), is(2));          // Count unchanged on reassign

    lm.removeLease("holder2", "/path2"); // Remove existing
    assertThat(lm.countPath(), is(1));
  }

  private static FSNamesystem makeMockFsNameSystem() {
    FSDirectory dir = mock(FSDirectory.class);
    FSNamesystem fsn = mock(FSNamesystem.class);
    when(fsn.isRunning()).thenReturn(true);
    when(fsn.hasWriteLock()).thenReturn(true);
    when(fsn.getFSDirectory()).thenReturn(dir);
    return fsn;
  }
}
