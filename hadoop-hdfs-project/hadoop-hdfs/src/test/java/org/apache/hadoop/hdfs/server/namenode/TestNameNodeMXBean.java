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

import com.google.common.base.Supplier;
import com.google.common.util.concurrent.Uninterruptibles;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.StripedFileTestUtil;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedStripedBlock;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.hdfs.server.datanode.DataNodeTestUtils;
import org.apache.hadoop.hdfs.server.namenode.top.TopConf;
import org.apache.hadoop.hdfs.util.StripedBlockUtil;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.nativeio.NativeIO;
import org.apache.hadoop.io.nativeio.NativeIO.POSIX.NoMlockCacheManipulator;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.util.VersionInfo;
import org.codehaus.jackson.map.ObjectMapper;
import org.junit.Assert;
import org.junit.Test;
import org.mortbay.util.ajax.JSON;

import javax.management.MBeanServer;
import javax.management.ObjectName;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * Class for testing {@link NameNodeMXBean} implementation
 */
public class TestNameNodeMXBean {

  /**
   * Used to assert equality between doubles
   */
  private static final double DELTA = 0.000001;

  static {
    NativeIO.POSIX.setCacheManipulator(new NoMlockCacheManipulator());
  }

  @SuppressWarnings({ "unchecked" })
  @Test
  public void testNameNodeMXBeanInfo() throws Exception {
    Configuration conf = new Configuration();
    conf.setLong(DFSConfigKeys.DFS_DATANODE_MAX_LOCKED_MEMORY_KEY,
      NativeIO.POSIX.getCacheManipulator().getMemlockLimit());
    MiniDFSCluster cluster = null;

    try {
      cluster = new MiniDFSCluster.Builder(conf).numDataNodes(2).build();
      cluster.waitActive();

      FSNamesystem fsn = cluster.getNameNode().namesystem;

      MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
      ObjectName mxbeanName = new ObjectName(
          "Hadoop:service=NameNode,name=NameNodeInfo");
      // get attribute "ClusterId"
      String clusterId = (String) mbs.getAttribute(mxbeanName, "ClusterId");
      assertEquals(fsn.getClusterId(), clusterId);
      // get attribute "BlockPoolId"
      String blockpoolId = (String) mbs.getAttribute(mxbeanName, 
          "BlockPoolId");
      assertEquals(fsn.getBlockPoolId(), blockpoolId);
      // get attribute "Version"
      String version = (String) mbs.getAttribute(mxbeanName, "Version");
      assertEquals(fsn.getVersion(), version);
      assertTrue(version.equals(VersionInfo.getVersion()
          + ", r" + VersionInfo.getRevision()));
      // get attribute "Used"
      Long used = (Long) mbs.getAttribute(mxbeanName, "Used");
      assertEquals(fsn.getUsed(), used.longValue());
      // get attribute "Total"
      Long total = (Long) mbs.getAttribute(mxbeanName, "Total");
      assertEquals(fsn.getTotal(), total.longValue());
      // get attribute "safemode"
      String safemode = (String) mbs.getAttribute(mxbeanName, "Safemode");
      assertEquals(fsn.getSafemode(), safemode);
      // get attribute nondfs
      Long nondfs = (Long) (mbs.getAttribute(mxbeanName, "NonDfsUsedSpace"));
      assertEquals(fsn.getNonDfsUsedSpace(), nondfs.longValue());
      // get attribute percentremaining
      Float percentremaining = (Float) (mbs.getAttribute(mxbeanName,
          "PercentRemaining"));
      assertEquals(fsn.getPercentRemaining(), percentremaining, DELTA);
      // get attribute Totalblocks
      Long totalblocks = (Long) (mbs.getAttribute(mxbeanName, "TotalBlocks"));
      assertEquals(fsn.getTotalBlocks(), totalblocks.longValue());
      // get attribute alivenodeinfo
      String alivenodeinfo = (String) (mbs.getAttribute(mxbeanName,
          "LiveNodes"));
      Map<String, Map<String, Object>> liveNodes =
          (Map<String, Map<String, Object>>) JSON.parse(alivenodeinfo);
      assertTrue(liveNodes.size() == 2);
      for (Map<String, Object> liveNode : liveNodes.values()) {
        assertTrue(liveNode.containsKey("nonDfsUsedSpace"));
        assertTrue(((Long)liveNode.get("nonDfsUsedSpace")) > 0);
        assertTrue(liveNode.containsKey("capacity"));
        assertTrue(((Long)liveNode.get("capacity")) > 0);
        assertTrue(liveNode.containsKey("numBlocks"));
        assertTrue(((Long)liveNode.get("numBlocks")) == 0);
      }
      assertEquals(fsn.getLiveNodes(), alivenodeinfo);
      // get attribute deadnodeinfo
      String deadnodeinfo = (String) (mbs.getAttribute(mxbeanName,
          "DeadNodes"));
      assertEquals(fsn.getDeadNodes(), deadnodeinfo);
      // get attribute NodeUsage
      String nodeUsage = (String) (mbs.getAttribute(mxbeanName,
          "NodeUsage"));
      assertEquals("Bad value for NodeUsage", fsn.getNodeUsage(), nodeUsage);
      // get attribute NameJournalStatus
      String nameJournalStatus = (String) (mbs.getAttribute(mxbeanName,
          "NameJournalStatus"));
      assertEquals("Bad value for NameJournalStatus", fsn.getNameJournalStatus(), nameJournalStatus);
      // get attribute JournalTransactionInfo
      String journalTxnInfo = (String) mbs.getAttribute(mxbeanName,
          "JournalTransactionInfo");
      assertEquals("Bad value for NameTxnIds", fsn.getJournalTransactionInfo(),
          journalTxnInfo);
      // get attribute "NNStarted"
      String nnStarted = (String) mbs.getAttribute(mxbeanName, "NNStarted");
      assertEquals("Bad value for NNStarted", fsn.getNNStarted(), nnStarted);
      // get attribute "CompileInfo"
      String compileInfo = (String) mbs.getAttribute(mxbeanName, "CompileInfo");
      assertEquals("Bad value for CompileInfo", fsn.getCompileInfo(), compileInfo);
      // get attribute CorruptFiles
      String corruptFiles = (String) (mbs.getAttribute(mxbeanName,
          "CorruptFiles"));
      assertEquals("Bad value for CorruptFiles", fsn.getCorruptFiles(), corruptFiles);
      // get attribute NameDirStatuses
      String nameDirStatuses = (String) (mbs.getAttribute(mxbeanName,
          "NameDirStatuses"));
      assertEquals(fsn.getNameDirStatuses(), nameDirStatuses);
      Map<String, Map<String, String>> statusMap =
        (Map<String, Map<String, String>>) JSON.parse(nameDirStatuses);
      Collection<URI> nameDirUris = cluster.getNameDirs(0);
      for (URI nameDirUri : nameDirUris) {
        File nameDir = new File(nameDirUri);
        System.out.println("Checking for the presence of " + nameDir +
            " in active name dirs.");
        assertTrue(statusMap.get("active").containsKey(nameDir.getAbsolutePath()));
      }
      assertEquals(2, statusMap.get("active").size());
      assertEquals(0, statusMap.get("failed").size());
      
      // This will cause the first dir to fail.
      File failedNameDir = new File(nameDirUris.iterator().next());
      assertEquals(0, FileUtil.chmod(
          new File(failedNameDir, "current").getAbsolutePath(), "000"));
      cluster.getNameNodeRpc().rollEditLog();
      
      nameDirStatuses = (String) (mbs.getAttribute(mxbeanName,
          "NameDirStatuses"));
      statusMap = (Map<String, Map<String, String>>) JSON.parse(nameDirStatuses);
      for (URI nameDirUri : nameDirUris) {
        File nameDir = new File(nameDirUri);
        String expectedStatus =
            nameDir.equals(failedNameDir) ? "failed" : "active";
        System.out.println("Checking for the presence of " + nameDir +
            " in " + expectedStatus + " name dirs.");
        assertTrue(statusMap.get(expectedStatus).containsKey(
            nameDir.getAbsolutePath()));
      }
      assertEquals(1, statusMap.get("active").size());
      assertEquals(1, statusMap.get("failed").size());
      assertEquals(0L, mbs.getAttribute(mxbeanName, "CacheUsed"));
      assertEquals(NativeIO.POSIX.getCacheManipulator().getMemlockLimit() *
          cluster.getDataNodes().size(),
              mbs.getAttribute(mxbeanName, "CacheCapacity"));
      assertNull("RollingUpgradeInfo should be null when there is no rolling"
          + " upgrade", mbs.getAttribute(mxbeanName, "RollingUpgradeStatus"));
    } finally {
      if (cluster != null) {
        for (URI dir : cluster.getNameDirs(0)) {
          FileUtil.chmod(
            new File(new File(dir), "current").getAbsolutePath(), "755");
        }
        cluster.shutdown();
      }
    }
  }

  @SuppressWarnings({ "unchecked" })
  @Test
  public void testLastContactTime() throws Exception {
    Configuration conf = new Configuration();
    conf.setInt(DFSConfigKeys.DFS_HEARTBEAT_INTERVAL_KEY, 1);
    conf.setInt(DFSConfigKeys.DFS_NAMENODE_HEARTBEAT_RECHECK_INTERVAL_KEY, 1);
    MiniDFSCluster cluster = null;

    try {
      cluster = new MiniDFSCluster.Builder(conf).numDataNodes(3).build();
      cluster.waitActive();

      FSNamesystem fsn = cluster.getNameNode().namesystem;

      MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
      ObjectName mxbeanName = new ObjectName(
        "Hadoop:service=NameNode,name=NameNodeInfo");

      // Define include file to generate deadNodes metrics
      FileSystem localFileSys = FileSystem.getLocal(conf);
      Path workingDir = localFileSys.getWorkingDirectory();
      Path dir = new Path(workingDir,
        "build/test/data/temp/TestNameNodeMXBean");
      Path includeFile = new Path(dir, "include");
      assertTrue(localFileSys.mkdirs(dir));
      StringBuilder includeHosts = new StringBuilder();
      for(DataNode dn : cluster.getDataNodes()) {
        includeHosts.append(dn.getDisplayName()).append("\n");
      }
      DFSTestUtil.writeFile(localFileSys, includeFile, includeHosts.toString());
      conf.set(DFSConfigKeys.DFS_HOSTS, includeFile.toUri().getPath());
      fsn.getBlockManager().getDatanodeManager().refreshNodes(conf);

      cluster.stopDataNode(0);
      while (fsn.getBlockManager().getDatanodeManager().getNumLiveDataNodes()
        != 2 ) {
        Uninterruptibles.sleepUninterruptibly(1, TimeUnit.SECONDS);
      }

      // get attribute deadnodeinfo
      String deadnodeinfo = (String) (mbs.getAttribute(mxbeanName,
        "DeadNodes"));
      assertEquals(fsn.getDeadNodes(), deadnodeinfo);
      Map<String, Map<String, Object>> deadNodes =
        (Map<String, Map<String, Object>>) JSON.parse(deadnodeinfo);
      assertTrue(deadNodes.size() > 0);
      for (Map<String, Object> deadNode : deadNodes.values()) {
        assertTrue(deadNode.containsKey("lastContact"));
        assertTrue(deadNode.containsKey("decommissioned"));
        assertTrue(deadNode.containsKey("xferaddr"));
      }

    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }

  @Test(timeout=120000)
  @SuppressWarnings("unchecked")
  public void testTopUsers() throws Exception {
    final Configuration conf = new Configuration();
    MiniDFSCluster cluster = null;
    try {
      cluster = new MiniDFSCluster.Builder(conf).numDataNodes(0).build();
      cluster.waitActive();
      MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
      ObjectName mxbeanNameFsns = new ObjectName(
          "Hadoop:service=NameNode,name=FSNamesystemState");
      FileSystem fs = cluster.getFileSystem();
      final Path path = new Path("/");
      final int NUM_OPS = 10;
      for (int i=0; i< NUM_OPS; i++) {
        fs.listStatus(path);
        fs.setTimes(path, 0, 1);
      }
      String topUsers =
          (String) (mbs.getAttribute(mxbeanNameFsns, "TopUserOpCounts"));
      ObjectMapper mapper = new ObjectMapper();
      Map<String, Object> map = mapper.readValue(topUsers, Map.class);
      assertTrue("Could not find map key timestamp", 
          map.containsKey("timestamp"));
      assertTrue("Could not find map key windows", map.containsKey("windows"));
      List<Map<String, List<Map<String, Object>>>> windows =
          (List<Map<String, List<Map<String, Object>>>>) map.get("windows");
      assertEquals("Unexpected num windows", 3, windows.size());
      for (Map<String, List<Map<String, Object>>> window : windows) {
        final List<Map<String, Object>> ops = window.get("ops");
        assertEquals("Unexpected num ops", 3, ops.size());
        for (Map<String, Object> op: ops) {
          final long count = Long.parseLong(op.get("totalCount").toString());
          final String opType = op.get("opType").toString();
          final int expected;
          if (opType.equals(TopConf.ALL_CMDS)) {
            expected = 2*NUM_OPS;
          } else {
            expected = NUM_OPS;
          }
          assertEquals("Unexpected total count", expected, count);
        }
      }
    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }

  @Test(timeout=120000)
  public void testTopUsersDisabled() throws Exception {
    final Configuration conf = new Configuration();
    // Disable nntop
    conf.setBoolean(DFSConfigKeys.NNTOP_ENABLED_KEY, false);
    MiniDFSCluster cluster = null;
    try {
      cluster = new MiniDFSCluster.Builder(conf).numDataNodes(0).build();
      cluster.waitActive();
      MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
      ObjectName mxbeanNameFsns = new ObjectName(
          "Hadoop:service=NameNode,name=FSNamesystemState");
      FileSystem fs = cluster.getFileSystem();
      final Path path = new Path("/");
      final int NUM_OPS = 10;
      for (int i=0; i< NUM_OPS; i++) {
        fs.listStatus(path);
        fs.setTimes(path, 0, 1);
      }
      String topUsers =
          (String) (mbs.getAttribute(mxbeanNameFsns, "TopUserOpCounts"));
      assertNull("Did not expect to find TopUserOpCounts bean!", topUsers);
    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }

  @Test(timeout=120000)
  public void testTopUsersNoPeriods() throws Exception {
    final Configuration conf = new Configuration();
    conf.setBoolean(DFSConfigKeys.NNTOP_ENABLED_KEY, true);
    conf.set(DFSConfigKeys.NNTOP_WINDOWS_MINUTES_KEY, "");
    MiniDFSCluster cluster = null;
    try {
      cluster = new MiniDFSCluster.Builder(conf).numDataNodes(0).build();
      cluster.waitActive();
      MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
      ObjectName mxbeanNameFsns = new ObjectName(
          "Hadoop:service=NameNode,name=FSNamesystemState");
      FileSystem fs = cluster.getFileSystem();
      final Path path = new Path("/");
      final int NUM_OPS = 10;
      for (int i=0; i< NUM_OPS; i++) {
        fs.listStatus(path);
        fs.setTimes(path, 0, 1);
      }
      String topUsers =
          (String) (mbs.getAttribute(mxbeanNameFsns, "TopUserOpCounts"));
      assertNotNull("Expected TopUserOpCounts bean!", topUsers);
    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }

  @Test(timeout = 120000)
  public void testQueueLength() throws Exception {
    final Configuration conf = new Configuration();
    MiniDFSCluster cluster = null;
    try {
      cluster = new MiniDFSCluster.Builder(conf).numDataNodes(0).build();
      cluster.waitActive();
      MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
      ObjectName mxbeanNameFs =
          new ObjectName("Hadoop:service=NameNode,name=FSNamesystem");
      int queueLength = (int) mbs.getAttribute(mxbeanNameFs, "LockQueueLength");
      assertEquals(0, queueLength);
    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }

  @Test
  public void testVerifyMissingBlockGroupsMetrics() throws Exception {
    MiniDFSCluster cluster = null;
    DistributedFileSystem fs = null;
    try {
      Configuration conf = new HdfsConfiguration();
      int dataBlocks = ErasureCodingPolicyManager
          .getSystemDefaultPolicy().getNumDataUnits();
      int parityBlocks = ErasureCodingPolicyManager
          .getSystemDefaultPolicy().getNumParityUnits();
      int cellSize = ErasureCodingPolicyManager
          .getSystemDefaultPolicy().getCellSize();
      int totalSize = dataBlocks + parityBlocks;
      cluster = new MiniDFSCluster.Builder(conf)
          .numDataNodes(totalSize).build();
      fs = cluster.getFileSystem();

      Map<Integer, Integer> dnIndices = new HashMap<>();
      ArrayList<DataNode> dnList = cluster.getDataNodes();
      for (int i = 0; i < totalSize; i++) {
        dnIndices.put(dnList.get(i).getIpcPort(), i);
      }

      // create file
      Path ecDirPath = new Path("/striped");
      fs.mkdir(ecDirPath, FsPermission.getDirDefault());
      fs.getClient().setErasureCodingPolicy(ecDirPath.toString(), null);
      Path file = new Path(ecDirPath, "corrupted");
      final int length = cellSize * dataBlocks;
      final byte[] bytes = StripedFileTestUtil.generateBytes(length);
      DFSTestUtil.writeFile(fs, file, bytes);

      LocatedStripedBlock lsb = (LocatedStripedBlock)fs.getClient()
          .getLocatedBlocks(file.toString(), 0, cellSize * dataBlocks).get(0);
      final LocatedBlock[] blks = StripedBlockUtil.parseStripedBlockGroup(lsb,
          cellSize, dataBlocks, parityBlocks);

      // make an unrecoverable ec file with corrupted blocks
      for(int i = 0; i < parityBlocks + 1; i++) {
        int ipcPort = blks[i].getLocations()[0].getIpcPort();
        int dnIndex = dnIndices.get(ipcPort);
        File storageDir = cluster.getInstanceStorageDir(dnIndex, 0);
        File blkFile = MiniDFSCluster.getBlockFile(storageDir,
            blks[i].getBlock());
        Assert.assertTrue("Block file does not exist", blkFile.exists());

        FileOutputStream out = new FileOutputStream(blkFile);
        out.write("corruption".getBytes());
      }

      // disable the heart beat from DN so that the corrupted block record is
      // kept in NameNode
      for (DataNode dn : cluster.getDataNodes()) {
        DataNodeTestUtils.setHeartbeatsDisabledForTests(dn, true);
      }

      // Read the file to trigger reportBadBlocks
      try {
        IOUtils.copyBytes(fs.open(file), new IOUtils.NullOutputStream(), conf,
            true);
      } catch (IOException ie) {
        assertTrue(ie.getMessage().contains(
            "missingChunksNum=" + (parityBlocks + 1)));
      }

      final MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
      final ObjectName mxbeanName = new ObjectName(
          "Hadoop:service=NameNode,name=NameNodeInfo");

      // Wait for the metrics to discover the unrecoverable block group
      GenericTestUtils.waitFor(new Supplier<Boolean>() {
        @Override
        public Boolean get() {
          try {
            Long numMissingBlocks =
                (Long) mbs.getAttribute(mxbeanName, "NumberOfMissingBlocks");
            if (numMissingBlocks == 1L) {
              return true;
            }
          } catch (Exception e) {
            Assert.fail("Caught unexpected exception.");
          }
          return false;
        }
      }, 1000, 60000);

      String corruptFiles = (String) (mbs.getAttribute(mxbeanName,
          "CorruptFiles"));
      int numCorruptFiles = ((Object[]) JSON.parse(corruptFiles)).length;
      assertEquals(1, numCorruptFiles);
    } finally {
      if (fs != null) {
        try {
          fs.close();
        } catch (Exception e) {
          throw e;
        }
      }
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }
}
