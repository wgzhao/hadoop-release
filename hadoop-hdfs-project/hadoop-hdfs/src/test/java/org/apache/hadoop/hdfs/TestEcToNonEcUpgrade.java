/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hdfs;

import com.google.common.collect.Sets;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.StartupOption;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.test.PathUtils;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.Set;

import static org.apache.hadoop.hdfs.protocol.HdfsConstants.SafeModeAction.SAFEMODE_ENTER;
import static org.apache.hadoop.hdfs.protocol.HdfsConstants.SafeModeAction.SAFEMODE_LEAVE;


/**
 * This verifies the special case upgrade from 2.3-ec to 2.6-nonEc.
 */
public class TestEcToNonEcUpgrade {
  public static final Logger LOG = LoggerFactory.getLogger(
      TestEcToNonEcUpgrade.class);

  @Rule
  public Timeout testTimeout = new Timeout(600_000);  // 10 minutes.

  @Test
  public void testBaseImage() throws Exception {
    // Loading a 2.3-ec image with no EC files/dirs must succeed.
    testImageLoading("01-base-2.3-ec-image-no-ec-policy-set.tar.gz", true);
  }

  @Test
  public void testImageWithEcPolicySetOnDir() throws Exception {
    // Loading a 2.3-ec image with EC policy on a directory must fail.
    testImageLoading("02-2.3-ec-image-with-ec-policy-set-no-ec-files.tar.gz",
        false);
  }

  @Test
  public void testImageWithEcFile() throws Exception {
    // Loading a 2.3-ec image with EC file must fail.
    testImageLoading("03-2.3-ec-image-with-1-ec-file-exists.tar.gz", false);
  }

  @Test
  public void testImageWithEcFileRemoved() throws Exception {
    // Loading a 2.3-ec image with EC files removed must fail if a directory
    // with EC policy still exists.
    testImageLoading(
        "04-2.3-ec-image-ec-files-removed-dir-with-ec-policy-exists.tar.gz",
        false);
  }

  @Test
  public void testImageWithEcFilesAndDirsRemoved() throws Exception {
    // Loading a 2.3-ec image with EC files and dirs removed must succeed.
    testImageLoading("05-2.3-ec-image-with-all-ec-files-dirs-deleted.tar.gz",
        true);
  }

  @Test
  public void testImageWithEcFileButNoEcDir() throws Exception {
    // Loading a 2.3-ec image with an EC file but no EC dir policy
    // (transient situation) must fail.
    testImageLoading("06-2.3-ec-image-no-ec-dir-corrupt-ec-file-moved-outside-ec-dir.tar.gz",
        false);
  }

  /**
   * This method does the following:
   *  - Attempts to load the given FsImage file.
   *  - Ensures that the load either succeeds or fails depending on expectSuccess.
   *  - If the image was successfully loaded, does the following:
   *    * Enters safe mode and writes a new FsImage.
   *    * Restarts the NameNode.
   *    * Checks that the new FsImage was successfully loaded.
   */
  public void testImageLoading(final String imageName, boolean expectSuccess)
      throws Exception {

    final File tarFile =
        new File(System.getProperty("test.cache.data", "build/test/cache") +
            "/" + imageName);
    final File testDir = new File(PathUtils.getTestDirName(getClass()));
    if (testDir.exists() && !FileUtil.fullyDelete(testDir)) {
      throw new IOException("Could not delete dfs directory '" + testDir + "'");
    }
    FileUtil.unTar(tarFile, testDir);
    final File nameDir = new File(testDir, getImageName(tarFile)+"/nn");
    final File dataDir = new File(testDir, getImageName(tarFile)+"/dn");

    // Sanity check that the image was correctly unpacked where we expect it.
    GenericTestUtils.assertExists(Paths.get(
        nameDir.toString(), "current", "VERSION").toFile());

    final Configuration conf = new HdfsConfiguration();
    conf.set(DFSConfigKeys.DFS_NAMENODE_NAME_DIR_KEY,
        nameDir.getAbsolutePath());
    conf.set(DFSConfigKeys.DFS_DATANODE_DATA_DIR_KEY,
        dataDir.getAbsolutePath());

    conf.setInt(DFSConfigKeys.DFS_NAMENODE_NUM_CHECKPOINTS_RETAINED_KEY,
        Integer.MAX_VALUE);  // Ensure saveNameSpace keeps all old images.
    conf.setBoolean(DFSConfigKeys.DFS_UPGRADE_ALLOW_OLDER_VERSION_KEY, true);

    MiniDFSCluster cluster = null;

    try {
      StartupOption startupOption = StartupOption.ROLLINGUPGRADE;
      startupOption.setRollingUpgradeStartupOption("started");
      cluster = new MiniDFSCluster.Builder(conf).numDataNodes(1)
          .format(false)
          .manageDataDfsDirs(false)
          .manageNameDfsDirs(false)
          .waitSafeMode(true)
          .startupOption(startupOption)
          .build();
      Assert.assertTrue("Expected failure while loading image " + imageName,
          expectSuccess);

      final Set<File> imagesBeforeSave = listFsImageFiles(nameDir);
      LOG.info("Images before saveNamespace: {}", imagesBeforeSave);

      final DistributedFileSystem dfs = cluster.getFileSystem();
      Assert.assertTrue("Failed to load pre-upgrade image " + tarFile,
          cluster.isNameNodeUp(0));
      dfs.setSafeMode(SAFEMODE_ENTER);
      dfs.saveNamespace();
      dfs.setSafeMode(SAFEMODE_LEAVE);

      // Ensure that a new image was generated.
      final Set<File> imagesAfterSave = listFsImageFiles(nameDir);
      LOG.info("Images after saveNamespace: {}", imagesAfterSave);
      Assert.assertTrue("New FsImage was not written out as expected ",
          imagesAfterSave.size() - imagesBeforeSave.size() == 1);

      // Now attempt to load the new namespace.
      cluster.shutdownNameNode(0);
      cluster.restartNameNode(0, true);

      Assert.assertTrue("Failed to load post-upgrade image " + tarFile,
          cluster.isNameNodeUp(0));
    } catch(IOException ioe) {
      if (expectSuccess) {
        LOG.error("Unexpected failure while loading image {}", imageName);
        throw ioe;
      } else {
        GenericTestUtils.assertExceptionContains(
            "Failed to load FSImage file", ioe);
        LOG.info("Got expected exception while loading image {}", imageName);
      }
    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
      FileUtil.fullyDelete(testDir);
    }
  }

  /**
   * Utility method to get the name of an fsimage from the tarball path,
   * stripping out the leading path and the extensions.
   */
  private static String getImageName(File tarFile) {
    // First call to getName strips out the parent directories.
    // Subsequent calls to replaceFirst strip out the archive extension.
    return tarFile.getName().replaceFirst(".tar.gz$", "")
        .replaceFirst(".tgz$", "")
        .replaceFirst(".tar$", "");
  }

  /**
   * Get the list of FsImage files from the given nameDir.
   *
   * @param nameDir
   * @return
   */
  private static Set<File> listFsImageFiles(File nameDir) {
    final File currentDir = new File(nameDir, "current");
    final File[] images = currentDir.listFiles(new FilenameFilter() {
      @Override
      public boolean accept(File dir, String name) {
        return name.startsWith("fsimage") && !name.endsWith(".md5");
      }
    });
    return Sets.newHashSet(images != null ? images : new File[0]);
  }
}
