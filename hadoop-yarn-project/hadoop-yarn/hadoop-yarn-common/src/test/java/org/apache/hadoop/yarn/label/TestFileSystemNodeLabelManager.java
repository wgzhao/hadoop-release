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

package org.apache.hadoop.yarn.label;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.ImmutableMap;

public class TestFileSystemNodeLabelManager extends NodeLabelTestBase {
  MockFileSystemNodeLabelManager mgr = null;
  Configuration conf = null;

  private static class MockFileSystemNodeLabelManager extends
      FileSystemNodeLabelManager {
    @Override
    protected void initDispatcher(Configuration conf) {
      super.dispatcher = new SyncDispatcher();
    }

    @Override
    protected void startDispatcher() {
      // do nothing
    }
  }

  @Before
  public void before() throws IOException {
    mgr = new MockFileSystemNodeLabelManager();
    conf = new Configuration();
    File tempDir = File.createTempFile("nlb", ".tmp");
    tempDir.delete();
    tempDir.mkdirs();
    tempDir.deleteOnExit();
    conf.set(YarnConfiguration.FS_NODE_LABEL_STORE_URI,
        tempDir.getAbsolutePath());
    mgr.init(conf);
    mgr.start();
  }

  @After
  public void after() throws IOException {
    mgr.fs.delete(mgr.rootDirPath, true);
    mgr.stop();
  }

  @SuppressWarnings({ "unchecked", "rawtypes" })
  @Test(timeout = 10000)
  public void testRecoverWithMirror() throws Exception {
    mgr.addLabels(toSet("p1", "p2", "p3"));
    mgr.addLabel("p4");
    mgr.addLabels(toSet("p5", "p6"));
    mgr.setLabelsOnMultipleNodes((Map) ImmutableMap.of("n1", toSet("p1"), "n2",
        toSet("p2")));
    mgr.setLabelsOnMultipleNodes((Map) ImmutableMap.of("n3", toSet("p3"), "n4",
        toSet("p4"), "n5", toSet("p5"), "n6", toSet("p6"), "n7", toSet("p6")));

    /*
     * node -> partition p1: n1 p2: n2 p3: n3 p4: n4 p5: n5 p6: n6, n7
     */

    mgr.removeLabel("p1");
    mgr.removeLabels(Arrays.asList("p3", "p5"));

    /*
     * After removed p2: n2 p4: n4 p6: n6, n7
     */
    // shutdown mgr and start a new mgr
    mgr.stop();

    mgr = new MockFileSystemNodeLabelManager();
    mgr.init(conf);

    // check variables
    Assert.assertEquals(3, mgr.getLabels().size());
    Assert.assertTrue(mgr.getLabels().containsAll(
        Arrays.asList("p2", "p4", "p6")));

    assertMapContains(mgr.getNodesToLabels(), ImmutableMap.of("n2",
        toSet("p2"), "n4", toSet("p4"), "n6", toSet("p6"), "n7", toSet("p6")));

    // stutdown mgr and start a new mgr
    mgr.stop();
    mgr = new MockFileSystemNodeLabelManager();
    mgr.init(conf);

    // check variables
    Assert.assertEquals(3, mgr.getLabels().size());
    Assert.assertTrue(mgr.getLabels().containsAll(
        Arrays.asList("p2", "p4", "p6")));

    assertMapContains(mgr.getNodesToLabels(), ImmutableMap.of("n2",
        toSet("p2"), "n4", toSet("p4"), "n6", toSet("p6"), "n7", toSet("p6")));
  }

  @SuppressWarnings({ "unchecked", "rawtypes" })
  @Test(timeout = 10000)
  public void testEditlogRecover() throws Exception {
    mgr.addLabels(toSet("p1", "p2", "p3"));
    mgr.addLabel("p4");
    mgr.addLabels(toSet("p5", "p6"));
    mgr.setLabelsOnMultipleNodes((Map) ImmutableMap.of("n1", toSet("p1"), "n2",
        toSet("p2")));
    mgr.setLabelsOnMultipleNodes((Map) ImmutableMap.of("n3", toSet("p3"), "n4",
        toSet("p4"), "n5", toSet("p5"), "n6", toSet("p6"), "n7", toSet("p6")));

    /*
     * node -> partition p1: n1 p2: n2 p3: n3 p4: n4 p5: n5 p6: n6, n7
     */

    mgr.removeLabel("p1");
    mgr.removeLabels(Arrays.asList("p3", "p5"));

    /*
     * After removed p2: n2 p4: n4 p6: n6, n7
     */
    // shutdown mgr and start a new mgr
    mgr.stop();

    mgr = new MockFileSystemNodeLabelManager();
    mgr.init(conf);

    // check variables
    Assert.assertEquals(3, mgr.getLabels().size());
    Assert.assertTrue(mgr.getLabels().containsAll(
        Arrays.asList("p2", "p4", "p6")));

    assertMapContains(mgr.getNodesToLabels(), ImmutableMap.of(
        "n2", toSet("p2"), "n4", toSet("p4"), "n6", toSet("p6"), "n7",
        toSet("p6")));
  }
}
