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

package org.apache.hadoop.yarn.server.resourcemanager.resourcetracker;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.yarn.api.records.NodeState;
import org.apache.hadoop.yarn.event.DrainDispatcher;
import org.apache.hadoop.yarn.server.resourcemanager.MockNM;
import org.apache.hadoop.yarn.server.resourcemanager.MockRM;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNode;
import org.junit.Assert;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.event.Dispatcher;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.event.InlineDispatcher;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.server.api.protocolrecords.RegisterNodeManagerRequest;
import org.apache.hadoop.yarn.server.resourcemanager.NMLivelinessMonitor;
import org.apache.hadoop.yarn.server.resourcemanager.NodesListManager;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.RMContextImpl;
import org.apache.hadoop.yarn.server.resourcemanager.ResourceManager.NodeEventDispatcher;
import org.apache.hadoop.yarn.server.resourcemanager.ResourceTrackerService;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNodeEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNodeEventType;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.SchedulerEventType;
import org.apache.hadoop.yarn.server.resourcemanager.security.NMTokenSecretManagerInRM;
import org.apache.hadoop.yarn.server.resourcemanager.security.RMContainerTokenSecretManager;
import org.apache.hadoop.yarn.server.utils.BuilderUtils;
import org.junit.Before;
import org.junit.Test;

public class TestNMReconnect {
  private static final RecordFactory recordFactory = 
      RecordFactoryProvider.getRecordFactory(null);

  private List<RMNodeEvent> rmNodeEvents = new ArrayList<RMNodeEvent>();

  private class TestRMNodeEventDispatcher implements
      EventHandler<RMNodeEvent> {

    @Override
    public void handle(RMNodeEvent event) {
      rmNodeEvents.add(event);
    }

  }

  ResourceTrackerService resourceTrackerService;

  @Before
  public void setUp() {
    Configuration conf = new Configuration();
    // Dispatcher that processes events inline
    Dispatcher dispatcher = new InlineDispatcher();

    dispatcher.register(RMNodeEventType.class,
        new TestRMNodeEventDispatcher());

    RMContext context = new RMContextImpl(dispatcher, null,
        null, null, null, null, null, null, null, null, null, null);
    dispatcher.register(SchedulerEventType.class,
        new InlineDispatcher.EmptyEventHandler());
    dispatcher.register(RMNodeEventType.class,
        new NodeEventDispatcher(context));
    NMLivelinessMonitor nmLivelinessMonitor = new NMLivelinessMonitor(
        dispatcher);
    nmLivelinessMonitor.init(conf);
    nmLivelinessMonitor.start();
    NodesListManager nodesListManager = new NodesListManager(context);
    nodesListManager.init(conf);
    RMContainerTokenSecretManager containerTokenSecretManager =
        new RMContainerTokenSecretManager(conf);
    containerTokenSecretManager.start();
    NMTokenSecretManagerInRM nmTokenSecretManager =
        new NMTokenSecretManagerInRM(conf);
    nmTokenSecretManager.start();
    resourceTrackerService = new ResourceTrackerService(context,
        nodesListManager, nmLivelinessMonitor, containerTokenSecretManager,
        nmTokenSecretManager);
    
    resourceTrackerService.init(conf);
    resourceTrackerService.start();
  }

  @Test
  public void testReconnect() throws Exception {
    String hostname1 = "localhost1";
    Resource capability = BuilderUtils.newResource(1024, 1);

    RegisterNodeManagerRequest request1 = recordFactory
        .newRecordInstance(RegisterNodeManagerRequest.class);
    NodeId nodeId1 = NodeId.newInstance(hostname1, 0);
    request1.setNodeId(nodeId1);
    request1.setHttpPort(0);
    request1.setResource(capability);
    resourceTrackerService.registerNodeManager(request1);

    Assert.assertEquals(RMNodeEventType.STARTED, rmNodeEvents.get(0).getType());

    rmNodeEvents.clear();
    resourceTrackerService.registerNodeManager(request1);
    Assert.assertEquals(RMNodeEventType.RECONNECTED,
        rmNodeEvents.get(0).getType());

    rmNodeEvents.clear();
    resourceTrackerService.registerNodeManager(request1);
    capability = BuilderUtils.newResource(1024, 2);
    request1.setResource(capability);
    Assert.assertEquals(RMNodeEventType.RECONNECTED,
        rmNodeEvents.get(0).getType());
  }

  @Test(timeout = 10000)
  public void testRMNodeStatusAfterReconnect() throws Exception {
    // The node(127.0.0.1:1234) reconnected with RM. When it registered with
    // RM, RM set its lastNodeHeartbeatResponse's id to 0 asynchronously. But
    // the node's heartbeat come before RM succeeded setting the id to 0.
    final DrainDispatcher dispatcher = new DrainDispatcher();
    MockRM rm = new MockRM(){
      @Override
      protected Dispatcher createDispatcher() {
        return dispatcher;
      }
    };
    rm.start();
    MockNM nm1 =
        new MockNM("127.0.0.1:1234", 15120, rm.getResourceTrackerService());
    nm1.registerNode();
    int i = 0;
    while(i < 3) {
      nm1.nodeHeartbeat(true);
      dispatcher.await();
      i++;
    }

    MockNM nm2 =
        new MockNM("127.0.0.1:1234", 15120, rm.getResourceTrackerService());
    nm2.registerNode();
    RMNode rmNode = rm.getRMContext().getRMNodes().get(nm2.getNodeId());
    nm2.nodeHeartbeat(true);
    dispatcher.await();
    Assert.assertEquals("Node is Not in Running state.", NodeState.RUNNING,
        rmNode.getState());
    rm.stop();
  }
}
