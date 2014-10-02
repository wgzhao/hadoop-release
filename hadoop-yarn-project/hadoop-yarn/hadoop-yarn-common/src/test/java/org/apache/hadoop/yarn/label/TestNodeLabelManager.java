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

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.util.resource.Resources;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;

public class TestNodeLabelManager extends NodeLabelTestBase {
  private final Resource EMPTY_RESOURCE = Resource.newInstance(0, 0);
  private final Resource SMALL_NODE = Resource.newInstance(100, 0);
  private final Resource LARGE_NODE = Resource.newInstance(1000, 0);

  DumbNodeLabelManager mgr = null;

  private static class DumbNodeLabelManager extends NodeLabelManager {
    Map<String, Set<String>> lastNodeToLabels = null;
    Collection<String> lastAddedlabels = null;
    Collection<String> lastRemovedlabels = null;

    @Override
    public void persistRemovingLabels(Collection<String> labels)
        throws IOException {
      lastRemovedlabels = labels;
    }

    @Override
    public void recover() throws IOException {
      // do nothing here
    }

    @Override
    protected void initDispatcher(Configuration conf) {
      super.dispatcher = new SyncDispatcher();
    }

    @Override
    protected void startDispatcher() {
      // do nothing
    }

    @Override
    public void persistNodeToLabelsChanges(
        Map<String, Set<String>> nodeToLabels) throws IOException {
      this.lastNodeToLabels = nodeToLabels;
    }

    @Override
    public void persistAddingLabels(Set<String> labels) throws IOException {
      this.lastAddedlabels = labels;
    }
  }

  @Before
  public void before() {
    mgr = new DumbNodeLabelManager();
    mgr.init(new Configuration());
    mgr.start();
  }

  @After
  public void after() {
    mgr.stop();
  }

  @Test(timeout = 5000)
  public void testAddRemovelabel() throws Exception {
    // Add some label
    mgr.addLabel("hello");
    assertCollectionEquals(mgr.lastAddedlabels, Arrays.asList("hello"));

    mgr.addLabel("world");
    mgr.addLabels(toSet("hello1", "world1"));
    assertCollectionEquals(mgr.lastAddedlabels,
        Sets.newHashSet("hello1", "world1"));

    Assert.assertTrue(mgr.getLabels().containsAll(
        Sets.newHashSet("hello", "world", "hello1", "world1")));

    // try to remove null, empty and non-existed label, should fail
    for (String p : Arrays.asList(null, NodeLabelManager.NO_LABEL, "xx")) {
      boolean caught = false;
      try {
        mgr.removeLabel(p);
      } catch (IOException e) {
        caught = true;
      }
      Assert.assertTrue("remove label should fail "
          + "when label is null/empty/non-existed", caught);
    }

    // Remove some label
    mgr.removeLabel("hello");
    assertCollectionEquals(mgr.lastRemovedlabels, Arrays.asList("hello"));
    Assert.assertTrue(mgr.getLabels().containsAll(
        Arrays.asList("world", "hello1", "world1")));

    mgr.removeLabels(Arrays.asList("hello1", "world1", "world"));
    Assert.assertTrue(mgr.lastRemovedlabels.containsAll(Sets.newHashSet(
        "hello1", "world1", "world")));
    Assert.assertTrue(mgr.getLabels().isEmpty());
  }

  @Test(timeout = 5000)
  public void testAddlabelWithCase() throws Exception {
    // Add some label
    mgr.addLabel("HeLlO");
    assertCollectionEquals(mgr.lastAddedlabels, Arrays.asList("HeLlO"));
    Assert.assertFalse(mgr.getLabels().containsAll(Arrays.asList("hello")));
  }

  @Test(timeout = 5000)
  public void testAddInvalidlabel() throws IOException {
    boolean caught = false;
    try {
      mgr.addLabel(null);
    } catch (IOException e) {
      caught = true;
    }
    Assert.assertTrue("null label should not add to repo", caught);

    caught = false;
    try {
      mgr.addLabel(NodeLabelManager.NO_LABEL);
    } catch (IOException e) {
      caught = true;
    }

    Assert.assertTrue("empty label should not add to repo", caught);

    caught = false;
    try {
      mgr.addLabel("-?");
    } catch (IOException e) {
      caught = true;
    }
    Assert.assertTrue("invalid label charactor should not add to repo", caught);

    caught = false;
    try {
      mgr.addLabel(StringUtils.repeat("c", 257));
    } catch (IOException e) {
      caught = true;
    }
    Assert.assertTrue("too long label should not add to repo", caught);

    caught = false;
    try {
      mgr.addLabel("-aaabbb");
    } catch (IOException e) {
      caught = true;
    }
    Assert.assertTrue("label cannot start with \"-\"", caught);

    caught = false;
    try {
      mgr.addLabel("_aaabbb");
    } catch (IOException e) {
      caught = true;
    }
    Assert.assertTrue("label cannot start with \"_\"", caught);
  }

  @SuppressWarnings({ "unchecked", "rawtypes" })
  @Test(timeout = 5000)
  public void testSetRemoveLabelsOnNodes() throws Exception {
    // set a label on a node, but label doesn't exist
    boolean caught = false;
    try {
      mgr.setLabelsOnSingleNode("node", toSet("label"));
    } catch (IOException e) {
      caught = true;
    }
    Assert.assertTrue("trying to set a label to a node but "
        + "label doesn't exist in repository should fail", caught);

    // set a label on a node, but node is null or empty
    try {
      mgr.setLabelsOnSingleNode(NodeLabelManager.NO_LABEL, toSet("label"));
    } catch (IOException e) {
      caught = true;
    }
    Assert.assertTrue("trying to add a empty node but succeeded", caught);

    // set node->label one by one
    mgr.addLabels(toSet("p1", "p2", "p3"));
    mgr.setLabelsOnSingleNode("n1", toSet("p1"));
    mgr.setLabelsOnSingleNode("n1", toSet("p2"));
    mgr.setLabelsOnSingleNode("n2", toSet("p3"));
    assertMapEquals(mgr.getNodesToLabels(),
        ImmutableMap.of("n1", toSet("p2"), "n2", toSet("p3")));
    assertMapEquals(mgr.lastNodeToLabels, ImmutableMap.of("n2", toSet("p3")));

    // set bunch of node->label
    mgr.setLabelsOnMultipleNodes((Map) ImmutableMap.of("n3", toSet("p3"), "n1",
        toSet("p1")));
    assertMapEquals(mgr.getNodesToLabels(), ImmutableMap.of("n1", toSet("p1"),
        "n2", toSet("p3"), "n3", toSet("p3")));
    assertMapEquals(mgr.lastNodeToLabels,
        ImmutableMap.of("n3", toSet("p3"), "n1", toSet("p1")));

    // remove label on node
    mgr.removeLabelOnNode("n1");
    assertMapEquals(mgr.getNodesToLabels(),
        ImmutableMap.of("n1", NodeLabelManager.EMPTY_STRING_SET, "n2",
            toSet("p3"), "n3", toSet("p3")));
    assertMapEquals(mgr.lastNodeToLabels,
        ImmutableMap.of("n1", NodeLabelManager.EMPTY_STRING_SET));

    // remove labels on node
    mgr.removeLabelsOnNodes(Arrays.asList("n2", "n3"));
    assertMapEquals(mgr.nodeToLabels, ImmutableMap.of("n1",
        NodeLabelManager.EMPTY_STRING_SET, "n2",
        NodeLabelManager.EMPTY_STRING_SET, "n3",
        NodeLabelManager.EMPTY_STRING_SET));
    assertMapEquals(mgr.lastNodeToLabels, ImmutableMap.of("n2",
        NodeLabelManager.EMPTY_STRING_SET, "n3",
        NodeLabelManager.EMPTY_STRING_SET));
  }

  @Test(timeout = 5000)
  public void testRemovelabelWithNodes() throws Exception {
    mgr.addLabels(toSet("p1", "p2", "p3"));
    mgr.setLabelsOnSingleNode("n1", toSet("p1"));
    mgr.setLabelsOnSingleNode("n2", toSet("p2"));
    mgr.setLabelsOnSingleNode("n3", toSet("p3"));

    mgr.removeLabel("p1");
    assertMapEquals(mgr.getNodesToLabels(),
        ImmutableMap.of("n2", toSet("p2"), "n3", toSet("p3")));
    assertCollectionEquals(mgr.lastRemovedlabels, Arrays.asList("p1"));

    mgr.removeLabels(Arrays.asList("p2", "p3"));
    Assert.assertTrue(mgr.getNodesToLabels().isEmpty());
    Assert.assertTrue(mgr.getLabels().isEmpty());
    assertCollectionEquals(mgr.lastRemovedlabels, Arrays.asList("p2", "p3"));
  }

  @Test(timeout = 5000)
  public void testNodeActiveDeactiveUpdate() throws Exception {
    mgr.addLabels(toSet("p1", "p2", "p3"));
    mgr.setLabelsOnSingleNode("n1", toSet("p1"));
    mgr.setLabelsOnSingleNode("n2", toSet("p2"));
    mgr.setLabelsOnSingleNode("n3", toSet("p3"));

    Assert.assertEquals(mgr.getResourceWithLabel("p1"), EMPTY_RESOURCE);
    Assert.assertEquals(mgr.getResourceWithLabel("p2"), EMPTY_RESOURCE);
    Assert.assertEquals(mgr.getResourceWithLabel("p3"), EMPTY_RESOURCE);
    Assert.assertEquals(mgr.getResourceWithLabel(NodeLabelManager.NO_LABEL),
        EMPTY_RESOURCE);

    // active two NM to n1, one large and one small
    mgr.activatedNode(NodeId.newInstance("n1", 0), SMALL_NODE);
    mgr.activatedNode(NodeId.newInstance("n1", 1), LARGE_NODE);
    Assert.assertEquals(mgr.getResourceWithLabel("p1"),
        Resources.add(SMALL_NODE, LARGE_NODE));
    Assert.assertEquals(mgr.getNumOfNodesByLabel("p1"), 1);

    // change the large NM to small, check if resource updated
    mgr.updateNodeResource(NodeId.newInstance("n1", 1), SMALL_NODE);
    Assert.assertEquals(mgr.getResourceWithLabel("p1"),
        Resources.multiply(SMALL_NODE, 2));
    Assert.assertEquals(mgr.getNumOfNodesByLabel("p1"), 1);

    // deactive one NM, and check if resource updated
    mgr.deactivateNode(NodeId.newInstance("n1", 1));
    Assert.assertEquals(mgr.getResourceWithLabel("p1"), SMALL_NODE);
    Assert.assertEquals(mgr.getNumOfNodesByLabel("p1"), 1);

    // continus deactive, check if resource updated
    mgr.deactivateNode(NodeId.newInstance("n1", 0));
    Assert.assertEquals(mgr.getResourceWithLabel("p1"), EMPTY_RESOURCE);
    Assert.assertEquals(mgr.getNumOfNodesByLabel("p1"), 0);

    // Add two NM to n1 back
    mgr.activatedNode(NodeId.newInstance("n1", 0), SMALL_NODE);
    mgr.activatedNode(NodeId.newInstance("n1", 1), LARGE_NODE);

    // And remove p1, now the two NM should come to default label,
    mgr.removeLabel("p1");
    Assert.assertEquals(mgr.getResourceWithLabel(NodeLabelManager.NO_LABEL),
        Resources.add(SMALL_NODE, LARGE_NODE));
  }

  @SuppressWarnings({ "unchecked", "rawtypes" })
  @Test(timeout = 5000)
  public void testUpdateNodeLabelWithActiveNode() throws Exception {
    mgr.addLabels(toSet("p1", "p2", "p3"));
    mgr.setLabelsOnSingleNode("n1", toSet("p1"));
    mgr.setLabelsOnSingleNode("n2", toSet("p2"));
    mgr.setLabelsOnSingleNode("n3", toSet("p3"));

    // active two NM to n1, one large and one small
    mgr.activatedNode(NodeId.newInstance("n1", 0), SMALL_NODE);
    mgr.activatedNode(NodeId.newInstance("n2", 0), SMALL_NODE);
    mgr.activatedNode(NodeId.newInstance("n3", 0), SMALL_NODE);

    // change label of n1 to p2
    mgr.setLabelsOnSingleNode("n1", toSet("p2"));
    Assert.assertEquals(mgr.getResourceWithLabel("p1"), EMPTY_RESOURCE);
    Assert.assertEquals(mgr.getNumOfNodesByLabel("p1"), 0);
    Assert.assertEquals(mgr.getResourceWithLabel("p2"),
        Resources.multiply(SMALL_NODE, 2));
    Assert.assertEquals(mgr.getNumOfNodesByLabel("p2"), 2);
    Assert.assertEquals(mgr.getResourceWithLabel("p3"), SMALL_NODE);
    Assert.assertEquals(mgr.getNumOfNodesByLabel("p3"), 1);

    // add more labels
    mgr.addLabels(toSet("p4", "p5", "p6"));
    mgr.setLabelsOnMultipleNodes((Map) ImmutableMap.of("n4", toSet("p1"), "n5",
        toSet("p2"), "n6", toSet("p3"), "n7", toSet("p4"), "n8", toSet("p5")));

    // now node -> label is,
    // p1 : n4
    // p2 : n1, n2, n5
    // p3 : n3, n6
    // p4 : n7
    // p5 : n8
    // no-label : n9

    // active these nodes
    mgr.activatedNode(NodeId.newInstance("n4", 0), SMALL_NODE);
    mgr.activatedNode(NodeId.newInstance("n5", 0), SMALL_NODE);
    mgr.activatedNode(NodeId.newInstance("n6", 0), SMALL_NODE);
    mgr.activatedNode(NodeId.newInstance("n7", 0), SMALL_NODE);
    mgr.activatedNode(NodeId.newInstance("n8", 0), SMALL_NODE);
    mgr.activatedNode(NodeId.newInstance("n9", 0), SMALL_NODE);

    // check varibles
    Assert.assertEquals(mgr.getResourceWithLabel("p1"), SMALL_NODE);
    Assert.assertEquals(mgr.getNumOfNodesByLabel("p1"), 1);
    Assert.assertEquals(mgr.getResourceWithLabel("p2"),
        Resources.multiply(SMALL_NODE, 3));
    Assert.assertEquals(mgr.getNumOfNodesByLabel("p2"), 3);
    Assert.assertEquals(mgr.getResourceWithLabel("p3"),
        Resources.multiply(SMALL_NODE, 2));
    Assert.assertEquals(mgr.getNumOfNodesByLabel("p3"), 2);
    Assert.assertEquals(mgr.getResourceWithLabel("p4"),
        Resources.multiply(SMALL_NODE, 1));
    Assert.assertEquals(mgr.getNumOfNodesByLabel("p4"), 1);
    Assert.assertEquals(mgr.getResourceWithLabel("p5"),
        Resources.multiply(SMALL_NODE, 1));
    Assert.assertEquals(mgr.getNumOfNodesByLabel("p5"), 1);
    Assert.assertEquals(mgr.getResourceWithLabel(""),
        Resources.multiply(SMALL_NODE, 1));
    Assert.assertEquals(mgr.getNumOfNodesByLabel(""), 1);

    // change a bunch of nodes -> labels
    // n4 -> p2
    // n7 -> empty
    // n5 -> p1
    // n8 -> empty
    // n9 -> p1
    //
    // now become:
    // p1 : n5, n9
    // p2 : n1, n2, n4
    // p3 : n3, n6
    // p4 : [ ]
    // p5 : [ ]
    // no label: n8, n7
    mgr.setLabelsOnMultipleNodes((Map) ImmutableMap.of("n4", toSet("p2"), "n7",
        NodeLabelManager.EMPTY_STRING_SET, "n5", toSet("p1"), "n8",
        NodeLabelManager.EMPTY_STRING_SET, "n9", toSet("p1")));

    // check varibles
    Assert.assertEquals(mgr.getResourceWithLabel("p1"),
        Resources.multiply(SMALL_NODE, 2));
    Assert.assertEquals(mgr.getNumOfNodesByLabel("p1"), 2);
    Assert.assertEquals(mgr.getResourceWithLabel("p2"),
        Resources.multiply(SMALL_NODE, 3));
    Assert.assertEquals(mgr.getNumOfNodesByLabel("p2"), 3);
    Assert.assertEquals(mgr.getResourceWithLabel("p3"),
        Resources.multiply(SMALL_NODE, 2));
    Assert.assertEquals(mgr.getNumOfNodesByLabel("p3"), 2);
    Assert.assertEquals(mgr.getResourceWithLabel("p4"),
        Resources.multiply(SMALL_NODE, 0));
    Assert.assertEquals(mgr.getNumOfNodesByLabel("p4"), 0);
    Assert.assertEquals(mgr.getResourceWithLabel("p5"),
        Resources.multiply(SMALL_NODE, 0));
    Assert.assertEquals(mgr.getNumOfNodesByLabel("p5"), 0);
    Assert.assertEquals(mgr.getResourceWithLabel(""),
        Resources.multiply(SMALL_NODE, 2));
    Assert.assertEquals(mgr.getNumOfNodesByLabel(""), 2);
  }
  
  @Test
  public void testGetQueueResource() throws Exception {
    Resource clusterResource = Resource.newInstance(9999, 1);
    
    /*
     * Node->Labels:
     *   host1 : red, blue
     *   host2 : blue, yellow
     *   host3 : yellow
     *   host4 :
     */
    mgr.addLabels(toSet("red", "blue", "yellow"));
    mgr.setLabelsOnSingleNode("host1", toSet("red", "blue"));
    mgr.setLabelsOnSingleNode("host2", toSet("blue", "yellow"));
    mgr.setLabelsOnSingleNode("host3", toSet("yellow"));
    
    // active two NM to n1, one large and one small
    mgr.activatedNode(NodeId.newInstance("host1", 0), SMALL_NODE);
    mgr.activatedNode(NodeId.newInstance("host2", 0), SMALL_NODE);
    mgr.activatedNode(NodeId.newInstance("host3", 0), SMALL_NODE);
    mgr.activatedNode(NodeId.newInstance("host4", 0), SMALL_NODE);
    
    // reinitialize queue
    Set<String> q1Label = toSet("red", "blue");
    Set<String> q2Label = toSet("blue", "yellow");
    Set<String> q3Label = toSet("yellow");
    Set<String> q4Label = NodeLabelManager.EMPTY_STRING_SET;
    Set<String> q5Label = toSet(NodeLabelManager.ANY);
    
    Map<String, Set<String>> queueToLabels = new HashMap<String, Set<String>>();
    queueToLabels.put("Q1", q1Label);
    queueToLabels.put("Q2", q2Label);
    queueToLabels.put("Q3", q3Label);
    queueToLabels.put("Q4", q4Label);
    queueToLabels.put("Q5", q5Label);

    mgr.reinitializeQueueLabels(queueToLabels);
    
    // check resource
    Assert.assertEquals(Resources.multiply(SMALL_NODE, 3),
        mgr.getQueueResource("Q1", q1Label, clusterResource));
    Assert.assertEquals(Resources.multiply(SMALL_NODE, 4),
        mgr.getQueueResource("Q2", q2Label, clusterResource));
    Assert.assertEquals(Resources.multiply(SMALL_NODE, 3),
        mgr.getQueueResource("Q3", q3Label, clusterResource));
    Assert.assertEquals(Resources.multiply(SMALL_NODE, 1),
        mgr.getQueueResource("Q4", q4Label, clusterResource));
    Assert.assertEquals(clusterResource,
        mgr.getQueueResource("Q5", q5Label, clusterResource));
    
    /*
     * Check resource after changes some labels
     * Node->Labels:
     *   host1 : blue
     *   host2 :
     *   host3 : red, yellow
     *   host4 :
     */
    mgr.setLabelsOnMultipleNodes(ImmutableMap.of(
        "host3", toSet("red", "yellow"), 
        "host1", toSet("blue"), 
        "host2", NodeLabelManager.EMPTY_STRING_SET));
    
    // check resource
    Assert.assertEquals(Resources.multiply(SMALL_NODE, 4),
        mgr.getQueueResource("Q1", q1Label, clusterResource));
    Assert.assertEquals(Resources.multiply(SMALL_NODE, 4),
        mgr.getQueueResource("Q2", q2Label, clusterResource));
    Assert.assertEquals(Resources.multiply(SMALL_NODE, 3),
        mgr.getQueueResource("Q3", q3Label, clusterResource));
    Assert.assertEquals(Resources.multiply(SMALL_NODE, 2),
        mgr.getQueueResource("Q4", q4Label, clusterResource));
    Assert.assertEquals(clusterResource,
        mgr.getQueueResource("Q5", q5Label, clusterResource));
    
    /*
     * Check resource after deactive/active some nodes 
     * Node->Labels:
     *   (deactived) host1 : blue
     *   host2 :
     *   (deactived and then actived) host3 : red, yellow
     *   host4 :
     */
    mgr.deactivateNode(NodeId.newInstance("host1", 0));
    mgr.deactivateNode(NodeId.newInstance("host3", 0));
    mgr.activatedNode(NodeId.newInstance("host3", 0), SMALL_NODE);
    
    // check resource
    Assert.assertEquals(Resources.multiply(SMALL_NODE, 3),
        mgr.getQueueResource("Q1", q1Label, clusterResource));
    Assert.assertEquals(Resources.multiply(SMALL_NODE, 3),
        mgr.getQueueResource("Q2", q2Label, clusterResource));
    Assert.assertEquals(Resources.multiply(SMALL_NODE, 3),
        mgr.getQueueResource("Q3", q3Label, clusterResource));
    Assert.assertEquals(Resources.multiply(SMALL_NODE, 2),
        mgr.getQueueResource("Q4", q4Label, clusterResource));
    Assert.assertEquals(clusterResource,
        mgr.getQueueResource("Q5", q5Label, clusterResource));
    
    /*
     * Check resource after refresh queue:
     *    Q1: blue
     *    Q2: red, blue
     *    Q3: red
     *    Q4:
     *    Q5: ANY
     */
    q1Label = toSet("blue");
    q2Label = toSet("blue", "red");
    q3Label = toSet("red");
    q4Label = NodeLabelManager.EMPTY_STRING_SET;
    q5Label = toSet(NodeLabelManager.ANY);
    
    queueToLabels.clear();
    queueToLabels.put("Q1", q1Label);
    queueToLabels.put("Q2", q2Label);
    queueToLabels.put("Q3", q3Label);
    queueToLabels.put("Q4", q4Label);
    queueToLabels.put("Q5", q5Label);

    mgr.reinitializeQueueLabels(queueToLabels);
    
    // check resource
    Assert.assertEquals(Resources.multiply(SMALL_NODE, 2),
        mgr.getQueueResource("Q1", q1Label, clusterResource));
    Assert.assertEquals(Resources.multiply(SMALL_NODE, 3),
        mgr.getQueueResource("Q2", q2Label, clusterResource));
    Assert.assertEquals(Resources.multiply(SMALL_NODE, 3),
        mgr.getQueueResource("Q3", q3Label, clusterResource));
    Assert.assertEquals(Resources.multiply(SMALL_NODE, 2),
        mgr.getQueueResource("Q4", q4Label, clusterResource));
    Assert.assertEquals(clusterResource,
        mgr.getQueueResource("Q5", q5Label, clusterResource));
    
    /*
     * Active NMs in nodes already have NM
     * Node->Labels:
     *   host2 :
     *   host3 : red, yellow (3 NMs)
     *   host4 : (2 NMs)
     */
    mgr.activatedNode(NodeId.newInstance("host3", 1), SMALL_NODE);
    mgr.activatedNode(NodeId.newInstance("host3", 2), SMALL_NODE);
    mgr.activatedNode(NodeId.newInstance("host4", 1), SMALL_NODE);
    
    // check resource
    Assert.assertEquals(Resources.multiply(SMALL_NODE, 3),
        mgr.getQueueResource("Q1", q1Label, clusterResource));
    Assert.assertEquals(Resources.multiply(SMALL_NODE, 6),
        mgr.getQueueResource("Q2", q2Label, clusterResource));
    Assert.assertEquals(Resources.multiply(SMALL_NODE, 6),
        mgr.getQueueResource("Q3", q3Label, clusterResource));
    Assert.assertEquals(Resources.multiply(SMALL_NODE, 3),
        mgr.getQueueResource("Q4", q4Label, clusterResource));
    Assert.assertEquals(clusterResource,
        mgr.getQueueResource("Q5", q5Label, clusterResource));
    
    /*
     * Deactive NMs in nodes already have NMs
     * Node->Labels:
     *   host2 :
     *   host3 : red, yellow (2 NMs)
     *   host4 : (0 NMs)
     */
    mgr.deactivateNode(NodeId.newInstance("host3", 2));
    mgr.deactivateNode(NodeId.newInstance("host4", 1));
    mgr.deactivateNode(NodeId.newInstance("host4", 0));
    
    // check resource
    Assert.assertEquals(Resources.multiply(SMALL_NODE, 1),
        mgr.getQueueResource("Q1", q1Label, clusterResource));
    Assert.assertEquals(Resources.multiply(SMALL_NODE, 3),
        mgr.getQueueResource("Q2", q2Label, clusterResource));
    Assert.assertEquals(Resources.multiply(SMALL_NODE, 3),
        mgr.getQueueResource("Q3", q3Label, clusterResource));
    Assert.assertEquals(Resources.multiply(SMALL_NODE, 1),
        mgr.getQueueResource("Q4", q4Label, clusterResource));
    Assert.assertEquals(clusterResource,
        mgr.getQueueResource("Q5", q5Label, clusterResource));
  }
}
