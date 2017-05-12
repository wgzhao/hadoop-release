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
package org.apache.hadoop.hdfs.nfs.nfs3;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystemTestHelper;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FsConstants;
import org.apache.hadoop.fs.viewfs.ConfigUtil;

import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.MiniDFSNNTopology;
import org.apache.hadoop.hdfs.client.HdfsAdmin;
import org.apache.hadoop.hdfs.nfs.conf.NfsConfigKeys;
import org.apache.hadoop.hdfs.nfs.conf.NfsConfiguration;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.hdfs.nfs.mount.RpcProgramMountd;
import org.apache.hadoop.nfs.nfs3.FileHandle;
import org.apache.hadoop.nfs.nfs3.Nfs3Constant;
import org.apache.hadoop.nfs.nfs3.Nfs3Status;
import org.apache.hadoop.nfs.nfs3.request.GETATTR3Request;
import org.apache.hadoop.nfs.nfs3.request.WRITE3Request;
import org.apache.hadoop.nfs.nfs3.response.GETATTR3Response;
import org.apache.hadoop.nfs.nfs3.response.WRITE3Response;
import org.apache.hadoop.oncrpc.XDR;
import org.apache.hadoop.oncrpc.security.SecurityHandler;
import org.apache.hadoop.security.authorize.DefaultImpersonationProvider;
import org.apache.hadoop.security.authorize.ProxyUsers;
import org.junit.Test;
import org.junit.Before;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.mockito.Mockito;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;

import static org.junit.Assert.assertEquals;


/**
 * Tests for {@link RpcProgramNfs3} with {@link org.apache.hadoop.fs.viewfs.ViewFileSystem}.
 */
public class TestViewfsWithNfs3 {
  private static DistributedFileSystem hdfs1;
  private static DistributedFileSystem hdfs2;
  private static MiniDFSCluster cluster = null;
  private static NfsConfiguration config = new NfsConfiguration();
  private static HdfsAdmin dfsAdmin1;
  private static HdfsAdmin dfsAdmin2;
  private static FileSystem viewFs;

  private static NameNode nn1;
  private static NameNode nn2;
  private static Nfs3 nfs;
  private static RpcProgramNfs3 nfsd;
  private static RpcProgramMountd mountd;
  private static SecurityHandler securityHandler;
  private static FileSystemTestHelper fsHelper;
  private static File testRootDir;

  @BeforeClass
  public static void setup() throws Exception {
    String currentUser = System.getProperty("user.name");

    config.set("fs.permissions.umask-mode", "u=rwx,g=,o=");
    config.set(DefaultImpersonationProvider.getTestProvider()
        .getProxySuperuserGroupConfKey(currentUser), "*");
    config.set(DefaultImpersonationProvider.getTestProvider()
        .getProxySuperuserIpConfKey(currentUser), "*");
    fsHelper = new FileSystemTestHelper();
    // Set up java key store
    String testRoot = fsHelper.getTestRootDir();
    testRootDir = new File(testRoot).getAbsoluteFile();
    final Path jksPath = new Path(testRootDir.toString(), "test.jks");
    ProxyUsers.refreshSuperUserGroupsConfiguration(config);

    cluster =
        new MiniDFSCluster.Builder(config).nnTopology(
            MiniDFSNNTopology.simpleFederatedTopology(2))
            .numDataNodes(2)
            .build();
    cluster.waitClusterUp();
    hdfs1 = cluster.getFileSystem(0);
    hdfs2 = cluster.getFileSystem(1);

    nn1 = cluster.getNameNode(0);
    nn2 = cluster.getNameNode(1);
    nn2.getServiceRpcAddress();
    dfsAdmin1 = new HdfsAdmin(cluster.getURI(0), config);
    dfsAdmin2 = new HdfsAdmin(cluster.getURI(1), config);

    // Use ephemeral ports in case tests are running in parallel
    config.setInt("nfs3.mountd.port", 0);
    config.setInt("nfs3.server.port", 0);
    config.set(CommonConfigurationKeysPublic.FS_DEFAULT_NAME_KEY,
        FsConstants.VIEWFS_URI.toString());
    // Start NFS with allowed.hosts set to "* rw"
    config.set("dfs.nfs.exports.allowed.hosts", "* rw");

    Path base1 = new Path("/user1");
    Path base2 = new Path("/user2");
    hdfs1.delete(base1, true);
    hdfs2.delete(base2, true);
    hdfs1.mkdirs(base1);
    hdfs2.mkdirs(base2);
    ConfigUtil.addLink(config, "/hdfs1", hdfs1.makeQualified(base1).toUri());
    ConfigUtil.addLink(config, "/hdfs2", hdfs2.makeQualified(base2).toUri());


    viewFs = FileSystem.get(config);
    config.setStrings(NfsConfigKeys.DFS_NFS_EXPORT_POINT_KEY,
        "/hdfs1", "/hdfs2");

    nfs = new Nfs3(config);
    nfs.startServiceInternal(false);
    nfsd = (RpcProgramNfs3) nfs.getRpcProgram();
    mountd = (RpcProgramMountd) nfs.getMountd().getRpcProgram();

    // Mock SecurityHandler which returns system user.name
    securityHandler = Mockito.mock(SecurityHandler.class);
    Mockito.when(securityHandler.getUser()).thenReturn(currentUser);
  }

  @AfterClass
  public static void shutdown() throws Exception {
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  @Before
  public void createFiles() throws IllegalArgumentException, IOException {
    viewFs.delete(new Path("/hdfs1/file1"), true);
    viewFs.create(new Path("/hdfs1/file1"));

    viewFs.delete(new Path("/hdfs1/file2"), true);
    viewFs.create(new Path("/hdfs1/file2"));

    viewFs.delete(new Path("/hdfs2/dir2"), true);
    viewFs.mkdirs(new Path("/hdfs2/dir2"));

    viewFs.delete(new Path("/hdfs1/dir1"), true);
    viewFs.mkdirs(new Path("/hdfs1/dir1"));

    viewFs.delete(new Path("/hdfs1/write1"), true);
    viewFs.create(new Path("/hdfs1/write1"));

    viewFs.delete(new Path("/hdfs2/write2"), true);
    viewFs.create(new Path("/hdfs2/write2"));
  }

  @Test
  public void testNumExports() throws Exception {
    assertEquals(mountd.getExports().size(),
        viewFs.getChildFileSystems().length);
  }

  @Test
  public void testPaths() throws Exception {
    assertEquals(hdfs1.resolvePath(new Path("/user1/file1")),
        viewFs.resolvePath(new Path("/hdfs1/file1")));
    assertEquals(hdfs1.resolvePath(new Path("/user1/file2")),
        viewFs.resolvePath(new Path("/hdfs1/file2")));
    assertEquals(hdfs2.resolvePath(new Path("/user2/dir2")),
        viewFs.resolvePath(new Path("/hdfs2/dir2")));
  }

  @Test
  public void testFileStatus() throws Exception {
    HdfsFileStatus status = nn1.getRpcServer().getFileInfo("/user1/file1");
    FileStatus st = viewFs.getFileStatus(new Path("/hdfs1/file1"));
    assertEquals(st.isDirectory(), status.isDir());

    HdfsFileStatus status2 = nn2.getRpcServer().getFileInfo("/user2/dir2");
    FileStatus st2 = viewFs.getFileStatus(new Path("/hdfs2/dir2"));
    assertEquals(st2.isDirectory(), status2.isDir());
  }

  private void testNfsGetAttrResponse(long fileId, int namenodeId,
                                      int expectedStatus) {
    FileHandle handle = new FileHandle(fileId, namenodeId);
    XDR xdrReq = new XDR();
    GETATTR3Request req = new GETATTR3Request(handle);
    req.serialize(xdrReq);
    GETATTR3Response response = nfsd.getattr(xdrReq.asReadOnlyWrap(),
        securityHandler, new InetSocketAddress("localhost", 1234));
    assertEquals("Incorrect return code", expectedStatus, response.getStatus());
  }

  @Test (timeout = 60000)
  public void testNfsAccessNN1() throws Exception {
    HdfsFileStatus status = nn1.getRpcServer().getFileInfo("/user1/file1");
    int namenodeId = NameNode.getNamenodeId(config, hdfs1.getUri());
    testNfsGetAttrResponse(status.getFileId(), namenodeId, Nfs3Status.NFS3_OK);
  }

  @Test (timeout = 60000)
  public void testNfsAccessNN2() throws Exception {
    HdfsFileStatus status = nn2.getRpcServer().getFileInfo("/user2/dir2");
    int namenodeId = NameNode.getNamenodeId(config, hdfs2.getUri());
    testNfsGetAttrResponse(status.getFileId(), namenodeId, Nfs3Status.NFS3_OK);
  }

  @Test (timeout = 60000)
  public void testWrongNfsAccess() throws Exception {
    HdfsFileStatus status = nn1.getRpcServer().getFileInfo("/user1/file2");
    int namenodeId = NameNode.getNamenodeId(config, hdfs2.getUri());
    testNfsGetAttrResponse(status.getFileId(), namenodeId,
        Nfs3Status.NFS3ERR_IO);
  }

  public void testNfsWriteResponse(long dirId, int namenodeId) throws Exception {
    FileHandle handle = new FileHandle(dirId, namenodeId);

    byte[] buffer = new byte[10];
    for (int i = 0; i < 10; i++) {
      buffer[i] = (byte) i;
    }

    WRITE3Request writeReq = new WRITE3Request(handle, 0, 10,
        Nfs3Constant.WriteStableHow.DATA_SYNC, ByteBuffer.wrap(buffer));
    XDR xdr_req = new XDR();
    writeReq.serialize(xdr_req);

    // Attempt by a priviledged user should pass.
    WRITE3Response response = nfsd.write(xdr_req.asReadOnlyWrap(),
        null, 1, securityHandler,
        new InetSocketAddress("localhost", 1234));
    assertEquals("Incorrect response:", null, response);
  }

  @Test (timeout = 60000)
  public void testNfsWriteNN1() throws Exception {
    HdfsFileStatus status = nn1.getRpcServer().getFileInfo("/user1/write1");
    int namenodeId = NameNode.getNamenodeId(config, hdfs1.getUri());
    testNfsWriteResponse(status.getFileId(), namenodeId);
  }

  @Test (timeout = 60000)
  public void testNfsWriteNN2() throws Exception {
    HdfsFileStatus status = nn2.getRpcServer().getFileInfo("/user2/write2");
    int namenodeId = NameNode.getNamenodeId(config, hdfs2.getUri());
    testNfsWriteResponse(status.getFileId(), namenodeId);
  }
}
