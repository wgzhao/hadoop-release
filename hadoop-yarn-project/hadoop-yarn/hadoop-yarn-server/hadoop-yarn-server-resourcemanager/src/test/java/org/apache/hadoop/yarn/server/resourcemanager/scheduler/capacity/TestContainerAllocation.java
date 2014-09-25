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

package org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.SecurityUtilTestHelper;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateResponse;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.LogAggregationContext;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.api.records.Token;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.label.MemoryNodeLabelManager;
import org.apache.hadoop.yarn.label.NodeLabelManager;
import org.apache.hadoop.yarn.security.ContainerTokenIdentifier;
import org.apache.hadoop.yarn.server.resourcemanager.MockAM;
import org.apache.hadoop.yarn.server.resourcemanager.MockNM;
import org.apache.hadoop.yarn.server.resourcemanager.MockRM;
import org.apache.hadoop.yarn.server.resourcemanager.RMContextImpl;
import org.apache.hadoop.yarn.server.resourcemanager.RMSecretManagerService;
import org.apache.hadoop.yarn.server.resourcemanager.ResourceManager;
import org.apache.hadoop.yarn.server.resourcemanager.TestFifoScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttempt;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptState;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainer;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainerState;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerAppReport;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.YarnScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.security.RMContainerTokenSecretManager;
import org.apache.hadoop.yarn.server.utils.BuilderUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;


public class TestContainerAllocation {

  private static final Log LOG = LogFactory.getLog(TestFifoScheduler.class);

  private final int GB = 1024;

  private YarnConfiguration conf;

  @Before
  public void setUp() throws Exception {
    conf = new YarnConfiguration();
    conf.setClass(YarnConfiguration.RM_SCHEDULER, CapacityScheduler.class,
      ResourceScheduler.class);
  }

  @Test(timeout = 3000000)
  public void testExcessReservationThanNodeManagerCapacity() throws Exception {
    MockRM rm = new MockRM(conf);
    rm.start();

    // Register node1
    MockNM nm1 = rm.registerNode("127.0.0.1:1234", 2 * GB, 4);
    MockNM nm2 = rm.registerNode("127.0.0.1:2234", 3 * GB, 4);

    nm1.nodeHeartbeat(true);
    nm2.nodeHeartbeat(true);

    // wait..
    int waitCount = 20;
    int size = rm.getRMContext().getRMNodes().size();
    while ((size = rm.getRMContext().getRMNodes().size()) != 2
        && waitCount-- > 0) {
      LOG.info("Waiting for node managers to register : " + size);
      Thread.sleep(100);
    }
    Assert.assertEquals(2, rm.getRMContext().getRMNodes().size());
    // Submit an application
    RMApp app1 = rm.submitApp(128);

    // kick the scheduling
    nm1.nodeHeartbeat(true);
    RMAppAttempt attempt1 = app1.getCurrentAppAttempt();
    MockAM am1 = rm.sendAMLaunched(attempt1.getAppAttemptId());
    am1.registerAppAttempt();

    LOG.info("sending container requests ");
    am1.addRequests(new String[] {"*"}, 3 * GB, 1, 1);
    AllocateResponse alloc1Response = am1.schedule(); // send the request

    // kick the scheduler
    nm1.nodeHeartbeat(true);
    int waitCounter = 20;
    LOG.info("heartbeating nm1");
    while (alloc1Response.getAllocatedContainers().size() < 1
        && waitCounter-- > 0) {
      LOG.info("Waiting for containers to be created for app 1...");
      Thread.sleep(500);
      alloc1Response = am1.schedule();
    }
    LOG.info("received container : "
        + alloc1Response.getAllocatedContainers().size());

    // No container should be allocated.
    // Internally it should not been reserved.
    Assert.assertTrue(alloc1Response.getAllocatedContainers().size() == 0);

    LOG.info("heartbeating nm2");
    waitCounter = 20;
    nm2.nodeHeartbeat(true);
    while (alloc1Response.getAllocatedContainers().size() < 1
        && waitCounter-- > 0) {
      LOG.info("Waiting for containers to be created for app 1...");
      Thread.sleep(500);
      alloc1Response = am1.schedule();
    }
    LOG.info("received container : "
        + alloc1Response.getAllocatedContainers().size());
    Assert.assertTrue(alloc1Response.getAllocatedContainers().size() == 1);

    rm.stop();
  }

  // This is to test container tokens are generated when the containers are
  // acquired by the AM, not when the containers are allocated
  @Test
  public void testContainerTokenGeneratedOnPullRequest() throws Exception {
    MockRM rm1 = new MockRM(conf);
    rm1.start();
    MockNM nm1 = rm1.registerNode("127.0.0.1:1234", 8000);
    RMApp app1 = rm1.submitApp(200);
    MockAM am1 = MockRM.launchAndRegisterAM(app1, rm1, nm1);
    // request a container.
    am1.allocate("127.0.0.1", 1024, 1, new ArrayList<ContainerId>());
    ContainerId containerId2 =
        ContainerId.newInstance(am1.getApplicationAttemptId(), 2);
    rm1.waitForState(nm1, containerId2, RMContainerState.ALLOCATED);

    RMContainer container =
        rm1.getResourceScheduler().getRMContainer(containerId2);
    // no container token is generated.
    Assert.assertEquals(containerId2, container.getContainerId());
    Assert.assertNull(container.getContainer().getContainerToken());

    // acquire the container.
    List<Container> containers =
        am1.allocate(new ArrayList<ResourceRequest>(),
          new ArrayList<ContainerId>()).getAllocatedContainers();
    Assert.assertEquals(containerId2, containers.get(0).getId());
    // container token is generated.
    Assert.assertNotNull(containers.get(0).getContainerToken());
    rm1.stop();
  }

  @Test
  public void testNormalContainerAllocationWhenDNSUnavailable() throws Exception{
    MockRM rm1 = new MockRM(conf);
    rm1.start();
    MockNM nm1 = rm1.registerNode("unknownhost:1234", 8000);
    RMApp app1 = rm1.submitApp(200);
    MockAM am1 = MockRM.launchAndRegisterAM(app1, rm1, nm1);

    // request a container.
    am1.allocate("127.0.0.1", 1024, 1, new ArrayList<ContainerId>());
    ContainerId containerId2 =
        ContainerId.newInstance(am1.getApplicationAttemptId(), 2);
    rm1.waitForState(nm1, containerId2, RMContainerState.ALLOCATED);

    // acquire the container.
    SecurityUtilTestHelper.setTokenServiceUseIp(true);
    List<Container> containers =
        am1.allocate(new ArrayList<ResourceRequest>(),
          new ArrayList<ContainerId>()).getAllocatedContainers();
    // not able to fetch the container;
    Assert.assertEquals(0, containers.size());

    SecurityUtilTestHelper.setTokenServiceUseIp(false);
    containers =
        am1.allocate(new ArrayList<ResourceRequest>(),
          new ArrayList<ContainerId>()).getAllocatedContainers();
    // should be able to fetch the container;
    Assert.assertEquals(1, containers.size());
  }

  // This is to test whether LogAggregationContext is passed into
  // container tokens correctly
  @Test
  public void testLogAggregationContextPassedIntoContainerToken()
      throws Exception {
    MockRM rm1 = new MockRM(conf);
    rm1.start();
    MockNM nm1 = rm1.registerNode("127.0.0.1:1234", 8000);
    MockNM nm2 = rm1.registerNode("127.0.0.1:2345", 8000);
    // LogAggregationContext is set as null
    Assert
      .assertNull(getLogAggregationContextFromContainerToken(rm1, nm1, null));

    // create a not-null LogAggregationContext
    final int interval = 2000;
    LogAggregationContext logAggregationContext =
        LogAggregationContext.newInstance(
          "includePattern", "excludePattern", interval);
    LogAggregationContext returned =
        getLogAggregationContextFromContainerToken(rm1, nm2,
          logAggregationContext);
    Assert.assertEquals("includePattern", returned.getIncludePattern());
    Assert.assertEquals("excludePattern", returned.getExcludePattern());
    Assert.assertEquals(interval, returned.getRollingIntervalSeconds());
    rm1.stop();
  }

  private LogAggregationContext getLogAggregationContextFromContainerToken(
      MockRM rm1, MockNM nm1, LogAggregationContext logAggregationContext)
      throws Exception {
    RMApp app2 = rm1.submitApp(200, logAggregationContext);
    MockAM am2 = MockRM.launchAndRegisterAM(app2, rm1, nm1);
    nm1.nodeHeartbeat(true);
    // request a container.
    am2.allocate("127.0.0.1", 512, 1, new ArrayList<ContainerId>());
    ContainerId containerId =
        ContainerId.newInstance(am2.getApplicationAttemptId(), 2);
    rm1.waitForState(nm1, containerId, RMContainerState.ALLOCATED);

    // acquire the container.
    List<Container> containers =
        am2.allocate(new ArrayList<ResourceRequest>(),
          new ArrayList<ContainerId>()).getAllocatedContainers();
    Assert.assertEquals(containerId, containers.get(0).getId());
    // container token is generated.
    Assert.assertNotNull(containers.get(0).getContainerToken());
    ContainerTokenIdentifier token =
        BuilderUtils.newContainerTokenIdentifier(containers.get(0)
          .getContainerToken());
    return token.getLogAggregationContext();
  }

  private volatile int numRetries = 0;
  private class TestRMSecretManagerService extends RMSecretManagerService {

    public TestRMSecretManagerService(Configuration conf,
        RMContextImpl rmContext) {
      super(conf, rmContext);
    }
    @Override
    protected RMContainerTokenSecretManager createContainerTokenSecretManager(
        Configuration conf) {
      return new RMContainerTokenSecretManager(conf) {

        @Override
        public Token createContainerToken(ContainerId containerId,
            NodeId nodeId, String appSubmitter, Resource capability,
            Priority priority, long createTime,
            LogAggregationContext logAggregationContext) {
          numRetries++;
          return super.createContainerToken(containerId, nodeId, appSubmitter,
            capability, priority, createTime, logAggregationContext);
        }
      };
    }
  }

  // This is to test fetching AM container will be retried, if AM container is
  // not fetchable since DNS is unavailable causing container token/NMtoken
  // creation failure.
  @Test(timeout = 20000)
  public void testAMContainerAllocationWhenDNSUnavailable() throws Exception {
    MockRM rm1 = new MockRM(conf) {
      @Override
      protected RMSecretManagerService createRMSecretManagerService() {
        return new TestRMSecretManagerService(conf, rmContext);
      }
    };
    rm1.start();

    MockNM nm1 = rm1.registerNode("unknownhost:1234", 8000);
    SecurityUtilTestHelper.setTokenServiceUseIp(true);
    RMApp app1 = rm1.submitApp(200);
    RMAppAttempt attempt = app1.getCurrentAppAttempt();
    nm1.nodeHeartbeat(true);

    // fetching am container will fail, keep retrying 5 times.
    while (numRetries <= 5) {
      nm1.nodeHeartbeat(true);
      Thread.sleep(1000);
      Assert.assertEquals(RMAppAttemptState.SCHEDULED,
        attempt.getAppAttemptState());
      System.out.println("Waiting for am container to be allocated.");
    }

    SecurityUtilTestHelper.setTokenServiceUseIp(false);
    rm1.waitForState(attempt.getAppAttemptId(), RMAppAttemptState.ALLOCATED);
    MockRM.launchAndRegisterAM(app1, rm1, nm1);
  }
  
  private Configuration getConfigurationWithDefaultQueueLabels(
      Configuration config) {
    CapacitySchedulerConfiguration conf =
        new CapacitySchedulerConfiguration(config);
    
    // Define top-level queues
    conf.setQueues(CapacitySchedulerConfiguration.ROOT, new String[] {"a", "b", "c"});
    
    // root can access anything
    conf.setLabels(CapacitySchedulerConfiguration.ROOT, toSet("*"));

    final String A = CapacitySchedulerConfiguration.ROOT + ".a";
    conf.setCapacity(A, 10);
    conf.setMaximumCapacity(A, 15);
    conf.setLabels(A, toSet("x"));
    conf.setDefaultLabelExpression(A, "x");
    
    final String B = CapacitySchedulerConfiguration.ROOT + ".b";
    conf.setCapacity(B, 20);
    conf.setLabels(B, toSet("y"));
    conf.setDefaultLabelExpression(B, "y");
    
    final String C = CapacitySchedulerConfiguration.ROOT + ".c";
    conf.setCapacity(C, 70);
    conf.setMaximumCapacity(C, 70);
    conf.setLabels(C, NodeLabelManager.EMPTY_STRING_SET);
    
    // Define 2nd-level queues
    final String A1 = A + ".a1";
    conf.setQueues(A, new String[] {"a1"});
    conf.setCapacity(A1, 100);
    conf.setMaximumCapacity(A1, 100);
    
    final String B1 = B + ".b1";
    conf.setQueues(B, new String[] {"b1"});
    conf.setCapacity(B1, 100);
    conf.setMaximumCapacity(B1, 100);

    final String C1 = C + ".c1";
    conf.setQueues(C, new String[] {"c1"});
    conf.setCapacity(C1, 100);
    conf.setMaximumCapacity(C1, 100);
    
    return conf;
  }
  
  private Configuration getConfigurationWithQueueLabels(Configuration config) {
    CapacitySchedulerConfiguration conf =
        new CapacitySchedulerConfiguration(config);
    
    // Define top-level queues
    conf.setQueues(CapacitySchedulerConfiguration.ROOT, new String[] {"a", "b", "c"});
    
    // root can access anything
    conf.setLabels(CapacitySchedulerConfiguration.ROOT, toSet("*"));

    final String A = CapacitySchedulerConfiguration.ROOT + ".a";
    conf.setCapacity(A, 10);
    conf.setMaximumCapacity(A, 15);
    conf.setLabels(A, toSet("x"));
    
    final String B = CapacitySchedulerConfiguration.ROOT + ".b";
    conf.setCapacity(B, 20);
    conf.setLabels(B, toSet("y"));
    
    final String C = CapacitySchedulerConfiguration.ROOT + ".c";
    conf.setCapacity(C, 70);
    conf.setMaximumCapacity(C, 70);
    conf.setLabels(C, NodeLabelManager.EMPTY_STRING_SET);
    
    // Define 2nd-level queues
    final String A1 = A + ".a1";
    conf.setQueues(A, new String[] {"a1"});
    conf.setCapacity(A1, 100);
    conf.setMaximumCapacity(A1, 100);
    
    final String B1 = B + ".b1";
    conf.setQueues(B, new String[] {"b1"});
    conf.setCapacity(B1, 100);
    conf.setMaximumCapacity(B1, 100);

    final String C1 = C + ".c1";
    conf.setQueues(C, new String[] {"c1"});
    conf.setCapacity(C1, 100);
    conf.setMaximumCapacity(C1, 100);
    
    return conf;
  }
  
  private void checkTaskContainersHost(ApplicationAttemptId attemptId,
      ContainerId containerId, ResourceManager rm, String host) {
    YarnScheduler scheduler = rm.getRMContext().getScheduler();
    SchedulerAppReport appReport = scheduler.getSchedulerAppInfo(attemptId);

    Assert.assertTrue(appReport.getLiveContainers().size() > 0);
    for (RMContainer c : appReport.getLiveContainers()) {
      if (c.getContainerId().equals(containerId)) {
        Assert.assertEquals(host, c.getAllocatedNode().getHost());
      }
    }
  }
  
  private <E> Set<E> toSet(E... elements) {
    Set<E> set = Sets.newHashSet(elements);
    return set;
  }
  
  private Configuration getComplexConfigurationWithQueueLabels(
      Configuration config) {
    CapacitySchedulerConfiguration conf =
        new CapacitySchedulerConfiguration(config);
    
    // Define top-level queues
    conf.setQueues(CapacitySchedulerConfiguration.ROOT, new String[] {"a", "b"});
    
    // root can access anything
    conf.setLabels(CapacitySchedulerConfiguration.ROOT, toSet("*"));

    final String A = CapacitySchedulerConfiguration.ROOT + ".a";
    conf.setCapacity(A, 50);
    conf.setMaximumCapacity(A, 50);
    conf.setLabels(A, toSet("x"));
    
    final String B = CapacitySchedulerConfiguration.ROOT + ".b";
    conf.setCapacity(B, 50);
    conf.setMaximumCapacity(B, 50);
    conf.setLabels(B, toSet("y"));
    
    // Define 2nd-level queues
    final String A1 = A + ".a1";
    conf.setQueues(A, new String[] {"a1"});
    conf.setCapacity(A1, 100);
    conf.setMaximumCapacity(A1, 100);
    conf.setLabels(A1, toSet("x", "y"));
    conf.setDefaultLabelExpression(A1, "x");
    
    conf.setQueues(B, new String[] {"b1", "b2"});
    final String B1 = B + ".b1";
    conf.setCapacity(B1, 10);
    conf.setMaximumCapacity(B1, 20);
    conf.setLabels(B1, NodeLabelManager.EMPTY_STRING_SET);

    final String B2 = B + ".b2";
    conf.setCapacity(B2, 90);
    conf.setMaximumCapacity(B2, 90);
    conf.setLabels(B2, toSet("y", "z"));

    return conf;
  }
  
  @Test (timeout = 300000)
  public void testContainerAllocateWithComplexLabels() throws Exception {
    // make it harder ..
    final NodeLabelManager mgr = new MemoryNodeLabelManager();
    mgr.init(conf);
    
    /*
     * Queue structure:
     *                  root (*)
     *                 /          \
     *               a(x) 50%      b(y) 50%
     *              /               /      \
     *             a1 (x,y)     b1(NO)  b2(y,z)
     *               100%        10%    90%
     *                           
     * Node structure:
     * h1 : x
     * h2 : x, y
     * h3 : y
     * h4 : y, z
     * h5 : NO
     * 
     * Each node can only allocate two containers
     */

    // set node -> label
    mgr.addLabels(ImmutableSet.of("x", "y", "z"));
    mgr.setLabelsOnMultipleNodes(ImmutableMap.of("h1", toSet("x"), "h2",
        toSet("x", "y"), "h3", toSet("y"), "h4", toSet("y", "z"), "h5",
        NodeLabelManager.EMPTY_STRING_SET));

    // inject node label manager
    MockRM rm1 = new MockRM(getComplexConfigurationWithQueueLabels(conf)) {
      @Override
      public NodeLabelManager createNodeLabelManager() {
        return mgr;
      }
    };

    rm1.getRMContext().setNodeLabelManager(mgr);
    rm1.start();
    MockNM nm1 = rm1.registerNode("h1:1234", 2048);
    MockNM nm2 = rm1.registerNode("h2:1234", 2048);
    MockNM nm3 = rm1.registerNode("h3:1234", 2048);
    MockNM nm4 = rm1.registerNode("h4:1234", 2048);
    MockNM nm5 = rm1.registerNode("h5:1234", 2048);
    
    ContainerId containerId;

    // launch an app to queue a1 (label = x), and check all container will
    // be allocated in h1
    RMApp app1 = rm1.submitApp(1024, "app", "user", null, "a1");
    MockAM am1 = MockRM.launchAndRegisterAM(app1, rm1, nm1);

    // request a container (label = x && y). can only allocate on nm2 
    am1.allocate("*", 1024, 1, new ArrayList<ContainerId>(), "x && y");
    containerId =
        ContainerId.newInstance(am1.getApplicationAttemptId(), 2);
    Assert.assertFalse(rm1.waitForState(nm1, containerId,
        RMContainerState.ALLOCATED, 10 * 1000));
    Assert.assertTrue(rm1.waitForState(nm2, containerId,
        RMContainerState.ALLOCATED, 10 * 1000));
    checkTaskContainersHost(am1.getApplicationAttemptId(), containerId, rm1,
        "h2");

    // launch an app to queue b1 (label = y), and check all container will
    // be allocated in h2
    RMApp app2 = rm1.submitApp(1024, "app", "user", null, "b1");
    MockAM am2 = MockRM.launchAndRegisterAM(app2, rm1, nm5);

    // request a container for AM, will succeed
    // and now b1's queue capacity will be used, cannot allocate more containers
    am2.allocate("*", 1024, 1, new ArrayList<ContainerId>());
    containerId = ContainerId.newInstance(am2.getApplicationAttemptId(), 2);
    Assert.assertFalse(rm1.waitForState(nm4, containerId,
        RMContainerState.ALLOCATED, 10 * 1000));
    Assert.assertFalse(rm1.waitForState(nm5, containerId,
        RMContainerState.ALLOCATED, 10 * 1000));
    
    // launch an app to queue b2
    RMApp app3 = rm1.submitApp(1024, "app", "user", null, "b2");
    MockAM am3 = MockRM.launchAndRegisterAM(app3, rm1, nm5);

    // request a container. try to allocate on nm1 (label = x) and nm3 (label =
    // y,z). Will successfully allocate on nm3
    am3.allocate("*", 1024, 1, new ArrayList<ContainerId>(), "y");
    containerId = ContainerId.newInstance(am3.getApplicationAttemptId(), 2);
    Assert.assertFalse(rm1.waitForState(nm1, containerId,
        RMContainerState.ALLOCATED, 10 * 1000));
    Assert.assertTrue(rm1.waitForState(nm3, containerId,
        RMContainerState.ALLOCATED, 10 * 1000));
    checkTaskContainersHost(am3.getApplicationAttemptId(), containerId, rm1,
        "h3");
    
    // try to allocate container (request label = y && z) on nm3 (label = y) and
    // nm4 (label = y,z). Will sucessfully allocate on nm4 only.
    am3.allocate("*", 1024, 1, new ArrayList<ContainerId>(), "y && z");
    containerId = ContainerId.newInstance(am3.getApplicationAttemptId(), 3);
    Assert.assertFalse(rm1.waitForState(nm3, containerId,
        RMContainerState.ALLOCATED, 10 * 1000));
    Assert.assertTrue(rm1.waitForState(nm4, containerId,
        RMContainerState.ALLOCATED, 10 * 1000));
    checkTaskContainersHost(am3.getApplicationAttemptId(), containerId, rm1,
        "h4");

    rm1.close();
  }

  @Test (timeout = 120000)
  public void testContainerAllocateWithLabels() throws Exception {
    final NodeLabelManager mgr = new MemoryNodeLabelManager();
    mgr.init(conf);

    // set node -> label
    mgr.addLabels(ImmutableSet.of("x", "y"));
    mgr.setLabelsOnMultipleNodes(ImmutableMap.of("h1", toSet("x"),
        "h2", toSet("y")));

    // inject node label manager
    MockRM rm1 = new MockRM(getConfigurationWithQueueLabels(conf)) {
      @Override
      public NodeLabelManager createNodeLabelManager() {
        return mgr;
      }
    };

    rm1.getRMContext().setNodeLabelManager(mgr);
    rm1.start();
    MockNM nm1 = rm1.registerNode("h1:1234", 8000); // label = x
    MockNM nm2 = rm1.registerNode("h2:1234", 8000); // label = y
    MockNM nm3 = rm1.registerNode("h3:1234", 8000); // label = <empty>
    
    ContainerId containerId;

    // launch an app to queue a1 (label = x), and check all container will
    // be allocated in h1
    RMApp app1 = rm1.submitApp(200, "app", "user", null, "a1");
    MockAM am1 = MockRM.launchAndRegisterAM(app1, rm1, nm3);

    // request a container.
    am1.allocate("*", 1024, 1, new ArrayList<ContainerId>(), "x");
    containerId =
        ContainerId.newInstance(am1.getApplicationAttemptId(), 2);
    Assert.assertFalse(rm1.waitForState(nm2, containerId,
        RMContainerState.ALLOCATED, 10 * 1000));
    Assert.assertTrue(rm1.waitForState(nm1, containerId,
        RMContainerState.ALLOCATED, 10 * 1000));
    checkTaskContainersHost(am1.getApplicationAttemptId(), containerId, rm1,
        "h1");

    // launch an app to queue b1 (label = y), and check all container will
    // be allocated in h2
    RMApp app2 = rm1.submitApp(200, "app", "user", null, "b1");
    MockAM am2 = MockRM.launchAndRegisterAM(app2, rm1, nm3);

    // request a container.
    am2.allocate("*", 1024, 1, new ArrayList<ContainerId>(), "y");
    containerId = ContainerId.newInstance(am2.getApplicationAttemptId(), 2);
    Assert.assertFalse(rm1.waitForState(nm1, containerId,
        RMContainerState.ALLOCATED, 10 * 1000));
    Assert.assertTrue(rm1.waitForState(nm2, containerId,
        RMContainerState.ALLOCATED, 10 * 1000));
    checkTaskContainersHost(am2.getApplicationAttemptId(), containerId, rm1,
        "h2");
    
    // launch an app to queue c1 (label = ""), and check all container will
    // be allocated in h3
    RMApp app3 = rm1.submitApp(200, "app", "user", null, "c1");
    MockAM am3 = MockRM.launchAndRegisterAM(app3, rm1, nm3);

    // request a container.
    am3.allocate("*", 1024, 1, new ArrayList<ContainerId>());
    containerId = ContainerId.newInstance(am3.getApplicationAttemptId(), 2);
    Assert.assertFalse(rm1.waitForState(nm2, containerId,
        RMContainerState.ALLOCATED, 10 * 1000));
    Assert.assertTrue(rm1.waitForState(nm3, containerId,
        RMContainerState.ALLOCATED, 10 * 1000));
    checkTaskContainersHost(am3.getApplicationAttemptId(), containerId, rm1,
        "h3");

    rm1.close();
  }
  
  @Test (timeout = 120000)
  public void testContainerAllocateWithDefaultQueueLabels() throws Exception {
    // This test is pretty much similar to testContainerAllocateWithLabel.
    // Difference is, this test doesn't specify label expression in ResourceRequest,
    // instead, it uses default queue label expression
    
    final NodeLabelManager mgr = new MemoryNodeLabelManager();
    mgr.init(conf);

    // set node -> label
    mgr.addLabels(ImmutableSet.of("x", "y"));
    mgr.setLabelsOnMultipleNodes(ImmutableMap.of("h1", toSet("x"),
        "h2", toSet("y")));

    // inject node label manager
    MockRM rm1 = new MockRM(getConfigurationWithDefaultQueueLabels(conf)) {
      @Override
      public NodeLabelManager createNodeLabelManager() {
        return mgr;
      }
    };

    rm1.getRMContext().setNodeLabelManager(mgr);
    rm1.start();
    MockNM nm1 = rm1.registerNode("h1:1234", 8000); // label = x
    MockNM nm2 = rm1.registerNode("h2:1234", 8000); // label = y
    MockNM nm3 = rm1.registerNode("h3:1234", 8000); // label = <empty>
    
    ContainerId containerId;

    // launch an app to queue a1 (label = x), and check all container will
    // be allocated in h1
    RMApp app1 = rm1.submitApp(200, "app", "user", null, "a1");
    MockAM am1 = MockRM.launchAndRegisterAM(app1, rm1, nm1);

    // request a container.
    am1.allocate("*", 1024, 1, new ArrayList<ContainerId>());
    containerId =
        ContainerId.newInstance(am1.getApplicationAttemptId(), 2);
    Assert.assertFalse(rm1.waitForState(nm3, containerId,
        RMContainerState.ALLOCATED, 10 * 1000));
    Assert.assertTrue(rm1.waitForState(nm1, containerId,
        RMContainerState.ALLOCATED, 10 * 1000));
    checkTaskContainersHost(am1.getApplicationAttemptId(), containerId, rm1,
        "h1");

    // launch an app to queue b1 (label = y), and check all container will
    // be allocated in h2
    RMApp app2 = rm1.submitApp(200, "app", "user", null, "b1");
    MockAM am2 = MockRM.launchAndRegisterAM(app2, rm1, nm2);

    // request a container.
    am2.allocate("*", 1024, 1, new ArrayList<ContainerId>());
    containerId = ContainerId.newInstance(am2.getApplicationAttemptId(), 2);
    Assert.assertFalse(rm1.waitForState(nm3, containerId,
        RMContainerState.ALLOCATED, 10 * 1000));
    Assert.assertTrue(rm1.waitForState(nm2, containerId,
        RMContainerState.ALLOCATED, 10 * 1000));
    checkTaskContainersHost(am2.getApplicationAttemptId(), containerId, rm1,
        "h2");
    
    // launch an app to queue c1 (label = ""), and check all container will
    // be allocated in h3
    RMApp app3 = rm1.submitApp(200, "app", "user", null, "c1");
    MockAM am3 = MockRM.launchAndRegisterAM(app3, rm1, nm3);

    // request a container.
    am3.allocate("*", 1024, 1, new ArrayList<ContainerId>());
    containerId = ContainerId.newInstance(am3.getApplicationAttemptId(), 2);
    Assert.assertFalse(rm1.waitForState(nm2, containerId,
        RMContainerState.ALLOCATED, 10 * 1000));
    Assert.assertTrue(rm1.waitForState(nm3, containerId,
        RMContainerState.ALLOCATED, 10 * 1000));
    checkTaskContainersHost(am3.getApplicationAttemptId(), containerId, rm1,
        "h3");

    rm1.close();
  }
}
