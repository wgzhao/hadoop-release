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
package org.apache.hadoop.yarn.server.resourcemanager.monitor.capacity;

import static org.apache.hadoop.yarn.server.resourcemanager.monitor.capacity.ProportionalCapacityPreemptionPolicy.MAX_IGNORED_OVER_CAPACITY;
import static org.apache.hadoop.yarn.server.resourcemanager.monitor.capacity.ProportionalCapacityPreemptionPolicy.MONITORING_INTERVAL;
import static org.apache.hadoop.yarn.server.resourcemanager.monitor.capacity.ProportionalCapacityPreemptionPolicy.NATURAL_TERMINATION_FACTOR;
import static org.apache.hadoop.yarn.server.resourcemanager.monitor.capacity.ProportionalCapacityPreemptionPolicy.OBSERVE_ONLY;
import static org.apache.hadoop.yarn.server.resourcemanager.monitor.capacity.ProportionalCapacityPreemptionPolicy.TOTAL_PREEMPTION_PER_ROUND;
import static org.apache.hadoop.yarn.server.resourcemanager.monitor.capacity.ProportionalCapacityPreemptionPolicy.WAIT_TIME_BEFORE_KILL;
import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.ContainerPreemptEventType.KILL_CONTAINER;
import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.ContainerPreemptEventType.PREEMPT_CONTAINER;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.argThat;
import static org.mockito.Matchers.isA;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Deque;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Queue;
import java.util.Random;
import java.util.Set;
import java.util.TreeSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.service.Service;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.label.NodeLabelManager;
import org.apache.hadoop.yarn.server.resourcemanager.MockRM;
import org.apache.hadoop.yarn.server.resourcemanager.monitor.SchedulingMonitor;
import org.apache.hadoop.yarn.server.resourcemanager.resource.Priority;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainer;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ContainerPreemptEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ContainerPreemptEventType;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerNode;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CSQueue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacityScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.LeafQueue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.ParentQueue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.fica.FiCaSchedulerApp;
import org.apache.hadoop.yarn.util.Clock;
import org.apache.hadoop.yarn.util.resource.DefaultResourceCalculator;
import org.apache.hadoop.yarn.util.resource.ResourceCalculator;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.mockito.ArgumentCaptor;
import org.mockito.ArgumentMatcher;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import com.google.common.collect.ImmutableSet;

public class TestProportionalCapacityPreemptionPolicy {

  static final long TS = 3141592653L;

  int appAlloc = 0;
  boolean setAMContainer = false;
  float setAMResourcePercent = 0.0f;
  Random rand = null;
  Clock mClock = null;
  Configuration conf = null;
  CapacityScheduler mCS = null;
  EventHandler<ContainerPreemptEvent> mDisp = null;
  ResourceCalculator rc = new DefaultResourceCalculator();
  final ApplicationAttemptId appA = ApplicationAttemptId.newInstance(
      ApplicationId.newInstance(TS, 0), 0);
  final ApplicationAttemptId appB = ApplicationAttemptId.newInstance(
      ApplicationId.newInstance(TS, 1), 0);
  final ApplicationAttemptId appC = ApplicationAttemptId.newInstance(
      ApplicationId.newInstance(TS, 2), 0);
  final ApplicationAttemptId appD = ApplicationAttemptId.newInstance(
      ApplicationId.newInstance(TS, 3), 0);
  final ApplicationAttemptId appE = ApplicationAttemptId.newInstance(
      ApplicationId.newInstance(TS, 4), 0);
  final ArgumentCaptor<ContainerPreemptEvent> evtCaptor =
    ArgumentCaptor.forClass(ContainerPreemptEvent.class);
  NodeLabelManager labelManager = mock(NodeLabelManager.class);
  private Map<String, Set<String>> nodeToLabels;

  @Rule public TestName name = new TestName();

  @Before
  @SuppressWarnings("unchecked")
  public void setup() {
    conf = new Configuration(false);
    conf.setLong(WAIT_TIME_BEFORE_KILL, 10000);
    conf.setLong(MONITORING_INTERVAL, 3000);
    // report "ideal" preempt
    conf.setFloat(TOTAL_PREEMPTION_PER_ROUND, (float) 1.0);
    conf.setFloat(NATURAL_TERMINATION_FACTOR, (float) 1.0);

    mClock = mock(Clock.class);
    mCS = mock(CapacityScheduler.class);
    when(mCS.getResourceCalculator()).thenReturn(rc);
    mDisp = mock(EventHandler.class);
    rand = new Random();
    long seed = rand.nextLong();
    System.out.println(name.getMethodName() + " SEED: " + seed);
    rand.setSeed(seed);
    appAlloc = 0;
  }

  @Test
  public void testIgnore() {
    int[][] qData = new int[][]{
      //  /   A   B   C
      { 100, 40, 40, 20 },  // abs
      { 100, 100, 100, 100 },  // maxCap
      { 100,  0, 60, 40 },  // used
      {   0,  0,  0,  0 },  // pending
      {   0,  0,  0,  0 },  // reserved
      {   3,  1,  1,  1 },  // apps
      {  -1,  1,  1,  1 },  // req granularity
      {   3,  0,  0,  0 },  // subqueues
    };
    ProportionalCapacityPreemptionPolicy policy = buildPolicy(qData);
    policy.editSchedule();
    // don't correct imbalances without demand
    verify(mDisp, never()).handle(isA(ContainerPreemptEvent.class));
  }

  @Test
  public void testProportionalPreemption() {
    int[][] qData = new int[][]{
      //  /   A   B   C  D
      { 100, 10, 40, 20, 30 },  // abs
      { 100, 100, 100, 100, 100 },  // maxCap
      { 100, 30, 60, 10,  0 },  // used
      {  45, 20,  5, 20,  0 },  // pending
      {   0,  0,  0,  0,  0 },  // reserved
      {   3,  1,  1,  1,  0 },  // apps
      {  -1,  1,  1,  1,  1 },  // req granularity
      {   4,  0,  0,  0,  0 },  // subqueues
    };
    ProportionalCapacityPreemptionPolicy policy = buildPolicy(qData);
    policy.editSchedule();
    verify(mDisp, times(16)).handle(argThat(new IsPreemptionRequestFor(appA)));
  }
  
  @Test
  public void testMaxCap() {
    int[][] qData = new int[][]{
        //  /   A   B   C
        { 100, 40, 40, 20 },  // abs
        { 100, 100, 45, 100 },  // maxCap
        { 100, 55, 45,  0 },  // used
        {  20, 10, 10,  0 },  // pending
        {   0,  0,  0,  0 },  // reserved
        {   2,  1,  1,  0 },  // apps
        {  -1,  1,  1,  0 },  // req granularity
        {   3,  0,  0,  0 },  // subqueues
      };
      ProportionalCapacityPreemptionPolicy policy = buildPolicy(qData);
      policy.editSchedule();
      // despite the imbalance, since B is at maxCap, do not correct
      verify(mDisp, never()).handle(argThat(new IsPreemptionRequestFor(appA)));
  }

  
  @Test
  public void testPreemptCycle() {
    int[][] qData = new int[][]{
      //  /   A   B   C
      { 100, 40, 40, 20 },  // abs
      { 100, 100, 100, 100 },  // maxCap
      { 100,  0, 60, 40 },  // used
      {  10, 10,  0,  0 },  // pending
      {   0,  0,  0,  0 },  // reserved
      {   3,  1,  1,  1 },  // apps
      {  -1,  1,  1,  1 },  // req granularity
      {   3,  0,  0,  0 },  // subqueues
    };
    ProportionalCapacityPreemptionPolicy policy = buildPolicy(qData);
    policy.editSchedule();
    // ensure all pending rsrc from A get preempted from other queues
    verify(mDisp, times(10)).handle(argThat(new IsPreemptionRequestFor(appC)));
  }

  @Test
  public void testExpireKill() {
    final long killTime = 10000L;
    int[][] qData = new int[][]{
      //  /   A   B   C
      { 100, 40, 40, 20 },  // abs
      { 100, 100, 100, 100 },  // maxCap
      { 100,  0, 60, 40 },  // used
      {  10, 10,  0,  0 },  // pending
      {   0,  0,  0,  0 },  // reserved
      {   3,  1,  1,  1 },  // apps
      {  -1,  1,  1,  1 },  // req granularity
      {   3,  0,  0,  0 },  // subqueues
    };
    conf.setLong(WAIT_TIME_BEFORE_KILL, killTime);
    ProportionalCapacityPreemptionPolicy policy = buildPolicy(qData);

    // ensure all pending rsrc from A get preempted from other queues
    when(mClock.getTime()).thenReturn(0L);
    policy.editSchedule();
    verify(mDisp, times(10)).handle(argThat(new IsPreemptionRequestFor(appC)));

    // requests reiterated
    when(mClock.getTime()).thenReturn(killTime / 2);
    policy.editSchedule();
    verify(mDisp, times(20)).handle(argThat(new IsPreemptionRequestFor(appC)));

    // kill req sent
    when(mClock.getTime()).thenReturn(killTime + 1);
    policy.editSchedule();
    verify(mDisp, times(30)).handle(evtCaptor.capture());
    List<ContainerPreemptEvent> events = evtCaptor.getAllValues();
    for (ContainerPreemptEvent e : events.subList(20, 30)) {
      assertEquals(appC, e.getAppId());
      assertEquals(KILL_CONTAINER, e.getType());
    }
  }

  @Test
  public void testDeadzone() {
    int[][] qData = new int[][]{
      //  /   A   B   C
      { 100, 40, 40, 20 },  // abs
      { 100, 100, 100, 100 },  // maxCap
      { 100, 39, 43, 21 },  // used
      {  10, 10,  0,  0 },  // pending
      {   0,  0,  0,  0 },  // reserved
      {   3,  1,  1,  1 },  // apps
      {  -1,  1,  1,  1 },  // req granularity
      {   3,  0,  0,  0 },  // subqueues
    };
    conf.setFloat(MAX_IGNORED_OVER_CAPACITY, (float) 0.1);
    ProportionalCapacityPreemptionPolicy policy = buildPolicy(qData);
    policy.editSchedule();
    // ignore 10% overcapacity to avoid jitter
    verify(mDisp, never()).handle(isA(ContainerPreemptEvent.class));
  }

  @Test
  public void testOverCapacityImbalance() {
    int[][] qData = new int[][]{
      //  /   A   B   C
      { 100, 40, 40, 20 },  // abs
      { 100, 100, 100, 100 },  // maxCap
      { 100, 55, 45,  0 },  // used
      {  20, 10, 10,  0 },  // pending
      {   0,  0,  0,  0 },  // reserved
      {   2,  1,  1,  0 },  // apps
      {  -1,  1,  1,  0 },  // req granularity
      {   3,  0,  0,  0 },  // subqueues
    };
    ProportionalCapacityPreemptionPolicy policy = buildPolicy(qData);
    policy.editSchedule();
    // correct imbalance between over-capacity queues
    verify(mDisp, times(5)).handle(argThat(new IsPreemptionRequestFor(appA)));
  }

  @Test
  public void testNaturalTermination() {
    int[][] qData = new int[][]{
      //  /   A   B   C
      { 100, 40, 40, 20 },  // abs
      { 100, 100, 100, 100 },  // maxCap
      { 100, 55, 45,  0 },  // used
      {  20, 10, 10,  0 },  // pending
      {   0,  0,  0,  0 },  // reserved
      {   2,  1,  1,  0 },  // apps
      {  -1,  1,  1,  0 },  // req granularity
      {   3,  0,  0,  0 },  // subqueues
    };
    conf.setFloat(NATURAL_TERMINATION_FACTOR, (float) 0.1);
    ProportionalCapacityPreemptionPolicy policy = buildPolicy(qData);
    policy.editSchedule();
    // ignore 10% imbalance between over-capacity queues
    verify(mDisp, never()).handle(isA(ContainerPreemptEvent.class));
  }

  @Test
  public void testObserveOnly() {
    int[][] qData = new int[][]{
      //  /   A   B   C
      { 100, 40, 40, 20 },  // abs
      { 100, 100, 100, 100 },  // maxCap
      { 100, 90, 10,  0 },  // used
      {  80, 10, 20, 50 },  // pending
      {   0,  0,  0,  0 },  // reserved
      {   2,  1,  1,  0 },  // apps
      {  -1,  1,  1,  0 },  // req granularity
      {   3,  0,  0,  0 },  // subqueues
    };
    conf.setBoolean(OBSERVE_ONLY, true);
    ProportionalCapacityPreemptionPolicy policy = buildPolicy(qData);
    policy.editSchedule();
    // verify even severe imbalance not affected
    verify(mDisp, never()).handle(isA(ContainerPreemptEvent.class));
  }

  @Test
  public void testHierarchical() {
    int[][] qData = new int[][] {
      //  /    A   B   C    D   E   F
      { 200, 100, 50, 50, 100, 10, 90 },  // abs
      { 200, 200, 200, 200, 200, 200, 200 },  // maxCap
      { 200, 110, 60, 50,  90, 90,  0 },  // used
      {  10,   0,  0,  0,  10,  0, 10 },  // pending
      {   0,   0,  0,  0,   0,  0,  0 },  // reserved
      {   4,   2,  1,  1,   2,  1,  1 },  // apps
      {  -1,  -1,  1,  1,  -1,  1,  1 },  // req granularity
      {   2,   2,  0,  0,   2,  0,  0 },  // subqueues
    };
    ProportionalCapacityPreemptionPolicy policy = buildPolicy(qData);
    policy.editSchedule();
    // verify capacity taken from A1, not B1 despite B1 being far over
    // its absolute guaranteed capacity
    verify(mDisp, times(10)).handle(argThat(new IsPreemptionRequestFor(appA)));
  }

  @Test
  public void testZeroGuar() {
    int[][] qData = new int[][] {
      //  /    A   B   C    D   E   F
        { 200, 100, 0, 99, 100, 10, 90 },  // abs
        { 200, 200, 200, 200, 200, 200, 200 },  // maxCap
        { 170,  80, 60, 20,  90, 90,  0 },  // used
        {  10,   0,  0,  0,  10,  0, 10 },  // pending
        {   0,   0,  0,  0,   0,  0,  0 },  // reserved
        {   4,   2,  1,  1,   2,  1,  1 },  // apps
        {  -1,  -1,  1,  1,  -1,  1,  1 },  // req granularity
        {   2,   2,  0,  0,   2,  0,  0 },  // subqueues
    };
    ProportionalCapacityPreemptionPolicy policy = buildPolicy(qData);
    policy.editSchedule();
    // verify capacity taken from A1, not B1 despite B1 being far over
    // its absolute guaranteed capacity
    verify(mDisp, never()).handle(argThat(new IsPreemptionRequestFor(appA)));
  }
  
  @Test
  public void testZeroGuarOverCap() {
    int[][] qData = new int[][] {
      //  /    A   B   C    D   E   F
         { 200, 100, 0, 99, 0, 100, 100 },  // abs
        { 200, 200, 200, 200, 200, 200, 200 },  // maxCap
        { 170,  170, 60, 20, 90, 0,  0 },  // used
        {  85,   50,  30,  10,  10,  20, 20 },  // pending
        {   0,   0,  0,  0,   0,  0,  0 },  // reserved
        {   4,   3,  1,  1,   1,  1,  1 },  // apps
        {  -1,  -1,  1,  1,  1,  -1,  1 },  // req granularity
        {   2,   3,  0,  0,   0,  1,  0 },  // subqueues
    };
    ProportionalCapacityPreemptionPolicy policy = buildPolicy(qData);
    policy.editSchedule();
    // we verify both that C has priority on B and D (has it has >0 guarantees)
    // and that B and D are force to share their over capacity fairly (as they
    // are both zero-guarantees) hence D sees some of its containers preempted
    verify(mDisp, times(14)).handle(argThat(new IsPreemptionRequestFor(appC)));
  }
  
  
  
  @Test
  public void testHierarchicalLarge() {
    int[][] qData = new int[][] {
      //  /    A   B   C    D   E   F    G   H   I
      { 400, 200, 60, 140, 100, 70, 30, 100, 10, 90  },  // abs
      { 400, 400, 400, 400, 400, 400, 400, 400, 400, 400, },  // maxCap
      { 400, 210, 70,140, 100, 50, 50,  90, 90,  0  },  // used
      {  10,   0,  0,  0,   0,  0,  0,   0,  0, 15  },  // pending
      {   0,   0,  0,  0,   0,  0,  0,   0,  0,  0  },  // reserved
      {   6,   2,  1,  1,   2,  1,  1,   2,  1,  1  },  // apps
      {  -1,  -1,  1,  1,  -1,  1,  1,  -1,  1,  1  },  // req granularity
      {   3,   2,  0,  0,   2,  0,  0,   2,  0,  0  },  // subqueues
    };
    ProportionalCapacityPreemptionPolicy policy = buildPolicy(qData);
    policy.editSchedule();
    // verify capacity taken from A1, not H1 despite H1 being far over
    // its absolute guaranteed capacity

    // XXX note: compensating for rounding error in Resources.multiplyTo
    // which is likely triggered since we use small numbers for readability
    verify(mDisp, times(9)).handle(argThat(new IsPreemptionRequestFor(appA)));
    verify(mDisp, times(4)).handle(argThat(new IsPreemptionRequestFor(appE)));
  }

  @Test
  public void testContainerOrdering(){

    List<RMContainer> containers = new ArrayList<RMContainer>();

    ApplicationAttemptId appAttId = ApplicationAttemptId.newInstance(
        ApplicationId.newInstance(TS, 10), 0);

    // create a set of containers
    RMContainer rm1 = mockContainer(appAttId, 5, mock(Resource.class), 3);
    RMContainer rm2 = mockContainer(appAttId, 3, mock(Resource.class), 3);
    RMContainer rm3 = mockContainer(appAttId, 2, mock(Resource.class), 2);
    RMContainer rm4 = mockContainer(appAttId, 1, mock(Resource.class), 2);
    RMContainer rm5 = mockContainer(appAttId, 4, mock(Resource.class), 1);

    // insert them in non-sorted order
    containers.add(rm3);
    containers.add(rm2);
    containers.add(rm1);
    containers.add(rm5);
    containers.add(rm4);

    // sort them
    ProportionalCapacityPreemptionPolicy.sortContainers(containers);

    // verify the "priority"-first, "reverse container-id"-second
    // ordering is enforced correctly
    assert containers.get(0).equals(rm1);
    assert containers.get(1).equals(rm2);
    assert containers.get(2).equals(rm3);
    assert containers.get(3).equals(rm4);
    assert containers.get(4).equals(rm5);

  }
  
  @Test
  public void testPolicyInitializeAfterSchedulerInitialized() {
    Configuration conf = new Configuration();
    conf.set(YarnConfiguration.RM_SCHEDULER_MONITOR_POLICIES,
        ProportionalCapacityPreemptionPolicy.class.getCanonicalName());
    conf.setBoolean(YarnConfiguration.RM_SCHEDULER_ENABLE_MONITORS, true);
    
    @SuppressWarnings("resource")
    MockRM rm = new MockRM(conf);
    rm.init(conf);
    
    // ProportionalCapacityPreemptionPolicy should be initialized after
    // CapacityScheduler initialized. We will 
    // 1) find SchedulingMonitor from RMActiveService's service list, 
    // 2) check if ResourceCalculator in policy is null or not. 
    // If it's not null, we can come to a conclusion that policy initialized
    // after scheduler got initialized
    for (Service service : rm.getRMActiveService().getServices()) {
      if (service instanceof SchedulingMonitor) {
        ProportionalCapacityPreemptionPolicy policy =
            (ProportionalCapacityPreemptionPolicy) ((SchedulingMonitor) service)
                .getSchedulingEditPolicy();
        assertNotNull(policy.getResourceCalculator());
        return;
      }
    }
    
    fail("Failed to find SchedulingMonitor service, please check what happened");
  }
  
  @Test
  public void testSkipAMContainer() {
    int[][] qData = new int[][] {
        //  /   A   B
        { 100, 50, 50 }, // abs
        { 100, 100, 100 }, // maxcap
        { 100, 100, 0 }, // used
        { 70, 20, 50 }, // pending
        { 0, 0, 0 }, // reserved
        { 5, 4, 1 }, // apps
        { -1, 1, 1 }, // req granularity
        { 2, 0, 0 }, // subqueues
    };
    setAMContainer = true;
    ProportionalCapacityPreemptionPolicy policy = buildPolicy(qData);
    policy.editSchedule();
    
    // By skipping AM Container, all other 24 containers of appD will be
    // preempted
    verify(mDisp, times(24)).handle(argThat(new IsPreemptionRequestFor(appD)));

    // By skipping AM Container, all other 24 containers of appC will be
    // preempted
    verify(mDisp, times(24)).handle(argThat(new IsPreemptionRequestFor(appC)));

    // Since AM containers of appC and appD are saved, 2 containers from appB
    // has to be preempted.
    verify(mDisp, times(2)).handle(argThat(new IsPreemptionRequestFor(appB)));
    setAMContainer = false;
  }
  
  @Test
  public void testPreemptSkippedAMContainers() {
    int[][] qData = new int[][] {
        //  /   A   B
        { 100, 10, 90 }, // abs
        { 100, 100, 100 }, // maxcap
        { 100, 100, 0 }, // used
        { 70, 20, 90 }, // pending
        { 0, 0, 0 }, // reserved
        { 5, 4, 1 }, // apps
        { -1, 5, 5 }, // req granularity
        { 2, 0, 0 }, // subqueues
    };
    setAMContainer = true;
    ProportionalCapacityPreemptionPolicy policy = buildPolicy(qData);
    policy.editSchedule();
    
    // All 5 containers of appD will be preempted including AM container.
    verify(mDisp, times(5)).handle(argThat(new IsPreemptionRequestFor(appD)));

    // All 5 containers of appC will be preempted including AM container.
    verify(mDisp, times(5)).handle(argThat(new IsPreemptionRequestFor(appC)));
    
    // By skipping AM Container, all other 4 containers of appB will be
    // preempted
    verify(mDisp, times(4)).handle(argThat(new IsPreemptionRequestFor(appB)));

    // By skipping AM Container, all other 4 containers of appA will be
    // preempted
    verify(mDisp, times(4)).handle(argThat(new IsPreemptionRequestFor(appA)));
    setAMContainer = false;
  }
  
  @Test
  public void testAMResourcePercentForSkippedAMContainers() {
    int[][] qData = new int[][] {
        //  /   A   B
        { 100, 10, 90 }, // abs
        { 100, 100, 100 }, // maxcap
        { 100, 100, 0 }, // used
        { 70, 20, 90 }, // pending
        { 0, 0, 0 }, // reserved
        { 5, 4, 1 }, // apps
        { -1, 5, 5 }, // req granularity
        { 2, 0, 0 }, // subqueues
    };
    setAMContainer = true;
    setAMResourcePercent = 0.5f;
    ProportionalCapacityPreemptionPolicy policy = buildPolicy(qData);
    policy.editSchedule();
    
    // AMResoucePercent is 50% of cluster and maxAMCapacity will be 5Gb.
    // Total used AM container size is 20GB, hence 2 AM container has
    // to be preempted as Queue Capacity is 10Gb.
    verify(mDisp, times(5)).handle(argThat(new IsPreemptionRequestFor(appD)));

    // Including AM Container, all other 4 containers of appC will be
    // preempted
    verify(mDisp, times(5)).handle(argThat(new IsPreemptionRequestFor(appC)));
    
    // By skipping AM Container, all other 4 containers of appB will be
    // preempted
    verify(mDisp, times(4)).handle(argThat(new IsPreemptionRequestFor(appB)));

    // By skipping AM Container, all other 4 containers of appA will be
    // preempted
    verify(mDisp, times(4)).handle(argThat(new IsPreemptionRequestFor(appA)));
    setAMContainer = false;
  }
  
  @SuppressWarnings("unchecked")
  @Test
  public void testIgnoreBecauseQueueCannotAccessSomeLabels() {
    int[][] qData = new int[][]{
      //  /   A   B   C
      { 100, 40, 40, 20 },  // abs
      { 100, 100, 100, 100 },  // maxCap
      { 100,  10, 60, 30 },  // used
      {   0,  30,  0,  0 },  // pending
      {   0,  0,  0,  0 },  // reserved
      {   3,  1,  1,  1 },  // apps
      {  -1,  1,  1,  1 },  // req granularity
      {   3,  0,  0,  0 },  // subqueues
    };
    
    NodeLabelManager labelManager = mock(NodeLabelManager.class);
    when(
        labelManager.getQueueResource(any(String.class), any(Set.class),
            any(Resource.class))).thenReturn(Resource.newInstance(10, 0),
                Resource.newInstance(100, 0), Resource.newInstance(10, 0));
    ProportionalCapacityPreemptionPolicy policy = buildPolicy(qData);
    policy.setNodeLabelManager(labelManager);
    policy.editSchedule();
    // don't correct imbalances without demand
    verify(mDisp, never()).handle(isA(ContainerPreemptEvent.class));
  }
  
  @SuppressWarnings({ "rawtypes" })
  @Test
  public void testPreemptContainerRespectLabels() {
    /*
     * A: yellow
     * B: blue
     * C: green, yellow
     * D: red
     * E: green
     * 
     * All node has labels, so C should only preempt container from A/E
     */
    int[][] qData = new int[][]{
        //  /   A   B   C   D   E
        { 100,  20, 20, 20, 20, 20 },  // abs
        { 100, 100, 100, 100, 100, 100 },  // maxCap
        { 100,  25, 25,  0,  25, 25 },  // used
        {   0,  0,  0,   20,  0,  0 },  // pending
        {   0,  0,  0,   0,   0,  0 },  // reserved
        {   5,  1,  1,   1,   1,  1 },  // apps
        {  -1,  1,  1,   1,   1,  1 },  // req granularity
        {   5,  0,  0,   0,   0,  0 },  // subqueues
      };
    
    Set[] queueLabels = new Set[6];
    queueLabels[1] = ImmutableSet.of("yellow");
    queueLabels[2] = ImmutableSet.of("blue");
    queueLabels[3] = ImmutableSet.of("yellow", "green");
    queueLabels[4] = ImmutableSet.of("red");
    queueLabels[5] = ImmutableSet.of("green");

    String[] hostnames = new String[] { "host1", "host2", "host3", "host4" };
    Set[] nodeLabels = new Set[4];
    nodeLabels[0] = ImmutableSet.of("yellow", "green");
    nodeLabels[1] = ImmutableSet.of("blue");
    nodeLabels[2] = ImmutableSet.of("red");
    nodeLabels[3] = ImmutableSet.of("yellow", "green");
    Resource[] nodeResources =
        new Resource[] { 
            Resource.newInstance(25, 0),
            Resource.newInstance(25, 0), 
            Resource.newInstance(25, 0),
            Resource.newInstance(25, 0) };
    
    Queue<String> containerHosts = new LinkedList<String>();
    addContainerHosts(containerHosts, "host1", 25);
    addContainerHosts(containerHosts, "host2", 25);
    addContainerHosts(containerHosts, "host3", 25);
    addContainerHosts(containerHosts, "host4", 25);

    // build policy and run
    ProportionalCapacityPreemptionPolicy policy =
        buildPolicy(qData, queueLabels, hostnames, nodeLabels, nodeResources,
            containerHosts);
    policy.editSchedule();
    
    // B,D don't have expected labels, will not preempt resource from them
    verify(mDisp, never()).handle(argThat(new IsPreemptionRequestFor(appB)));
    verify(mDisp, never()).handle(argThat(new IsPreemptionRequestFor(appD)));
    
    // A,E have expected resource, preempt resource from them
    verify(mDisp, times(5)).handle(argThat(new IsPreemptionRequestFor(appA)));
    verify(mDisp, times(5)).handle(argThat(new IsPreemptionRequestFor(appE)));
  }
  
  @SuppressWarnings({ "rawtypes" })
  @Test
  public void
      testPreemptContainerRespectLabelsInHierarchyQueuesWithAvailableRes() {
    /*
     * A-E: (x)
     * F: (y)
     * 
     * All node has labels, so C should only preempt container from B/F
     * 
     * Queue structure:
     *           root
     *          /    \
     *         A      F 
     *        / \
     *       B   E
     *      / \
     *     C   D    
     */
    int[][] qData = new int[][] {
        // /   A    B     C    D    E   F
        { 100, 60,  30,  15,  15,  30,  40 }, // abs
        { 100, 100, 100, 100, 100, 100, 100 }, // maxCap
        { 65,  65,  65,  10,  55,   0,   0 }, // used
        { 0,   0,   0,   0,   0,   30,   0 }, // pending
        { 0,   0,   0,   0,   0,    0,   0 }, // reserved
        { 4,   3,   2,   1,   1,    1,   0 }, // apps
        { -1,  1,   1,   1,   1,    1,   1 }, // req granularity
        { 2,   2,   2,   0,   0,    0,   0 }, // subqueues
    };
    
    Set[] queueLabels = new Set[7];
    queueLabels[1] = ImmutableSet.of("x");
    queueLabels[2] = ImmutableSet.of("x");
    queueLabels[3] = ImmutableSet.of("x");
    queueLabels[4] = ImmutableSet.of("x");
    queueLabels[5] = ImmutableSet.of("x");
    queueLabels[6] = ImmutableSet.of("y");
    
    String[] hostnames = new String[] { "host1", "host2", "host3" };
    Set[] nodeLabels = new Set[3];
    nodeLabels[0] = ImmutableSet.of("x");
    nodeLabels[1] = ImmutableSet.of("x");
    nodeLabels[2] = ImmutableSet.of("y");
    Resource[] nodeResources =
        new Resource[] { Resource.newInstance(30, 0),
            Resource.newInstance(40, 0), Resource.newInstance(30, 0) };
    
    Queue<String> containerHosts = new LinkedList<String>();
    addContainerHosts(containerHosts, "host1", 30);
    addContainerHosts(containerHosts, "host2", 35);
    
    // build policy and run
    ProportionalCapacityPreemptionPolicy policy =
        buildPolicy(qData, queueLabels, hostnames, nodeLabels, nodeResources,
            containerHosts);
    policy.editSchedule();
    
    // B,D don't have expected labels, will not preempt resource from them
    verify(mDisp, times(0)).handle(argThat(new IsPreemptionRequestFor(appA)));
    
    // A,E have expected resource, preempt resource from them
    // because of real->integer, it is possible preempted 23 or 25 containers
    // from B
    verify(mDisp, atLeast(23)).handle(argThat(new IsPreemptionRequestFor(appB)));
  }
  
  
  @SuppressWarnings({ "rawtypes" })
  @Test
  public void testPreemptContainerRespectLabelsInHierarchyQueues() {
    /*
     * A: <empty>
     * B: yellow
     * C: blue
     * D: green, yellow
     * E: <empty>
     * F: green
     * 
     * All node has labels, so C should only preempt container from B/F
     * 
     * Queue structure:
     *           root
     *          /  | \
     *         A   D  E
     *        / \      \
     *       B   C      F
     */
    int[][] qData = new int[][] {
        // /   A    B     C    D    E   F
        { 100, 50,  25,  25,  25,  25,  25 }, // abs
        { 100, 100, 100, 100, 100, 100, 100 }, // maxCap
        { 100, 60,  30,  30,   0,  40,  40 }, // used
        { 0,   0,   0,   0,   25,   0,   0 }, // pending
        { 0,   0,   0,   0,   0,    0,   0 }, // reserved
        { 4,   2,   1,   1,   1,    1,   1 }, // apps
        { -1,  1,   1,   1,   1,    1,   1 }, // req granularity
        { 3,   2,   0,   0,   0,    1,   0 }, // subqueues
    };
    
    Set[] queueLabels = new Set[7];
    queueLabels[2] = ImmutableSet.of("yellow"); // B
    queueLabels[3] = ImmutableSet.of("blue"); // C
    queueLabels[4] = ImmutableSet.of("yellow", "green"); // D
    queueLabels[6] = ImmutableSet.of("green"); // F
    
    String[] hostnames = new String[] { "host1", "host2", "host3" };
    Set[] nodeLabels = new Set[3];
    nodeLabels[0] = ImmutableSet.of("blue");
    nodeLabels[1] = ImmutableSet.of("yellow", "green");
    nodeLabels[2] = ImmutableSet.of("yellow", "green");
    Resource[] nodeResources =
        new Resource[] { Resource.newInstance(30, 0),
            Resource.newInstance(40, 0), Resource.newInstance(30, 0) };
    
    Queue<String> containerHosts = new LinkedList<String>();
    addContainerHosts(containerHosts, "host2", 30);
    addContainerHosts(containerHosts, "host1", 30);
    addContainerHosts(containerHosts, "host2", 10);
    addContainerHosts(containerHosts, "host3", 30);

    // build policy and run
    ProportionalCapacityPreemptionPolicy policy =
        buildPolicy(qData, queueLabels, hostnames, nodeLabels, nodeResources,
            containerHosts);
    policy.editSchedule();
    
    // B,D don't have expected labels, will not preempt resource from them
    verify(mDisp, times(0)).handle(argThat(new IsPreemptionRequestFor(appB)));
    
    // A,E have expected resource, preempt resource from them
    verify(mDisp, times(5)).handle(argThat(new IsPreemptionRequestFor(appA)));
    verify(mDisp, times(15)).handle(argThat(new IsPreemptionRequestFor(appD)));
  }
  
  private void addContainerHosts(Queue<String> containerHosts, String host,
      int times) {
    for (int i = 0; i < times; i++) {
      containerHosts.offer(host);
    }
  }
  
  static class IsPreemptionRequestFor
      extends ArgumentMatcher<ContainerPreemptEvent> {
    private final ApplicationAttemptId appAttId;
    private final ContainerPreemptEventType type;
    IsPreemptionRequestFor(ApplicationAttemptId appAttId) {
      this(appAttId, PREEMPT_CONTAINER);
    }
    IsPreemptionRequestFor(ApplicationAttemptId appAttId,
        ContainerPreemptEventType type) {
      this.appAttId = appAttId;
      this.type = type;
    }
    @Override
    public boolean matches(Object o) {
      return appAttId.equals(((ContainerPreemptEvent)o).getAppId())
          && type.equals(((ContainerPreemptEvent)o).getType());
    }
    @Override
    public String toString() {
      return appAttId.toString();
    }
  }
  
  ProportionalCapacityPreemptionPolicy buildPolicy(int[][] qData) {
    return buildPolicy(qData, null, null, null, null, null);
  }

  @SuppressWarnings({ "unchecked", "rawtypes" })
  ProportionalCapacityPreemptionPolicy buildPolicy(int[][] qData, Set[] labels,
      String[] hostnames, Set[] nodeLabels, Resource[] nodeResources,
      Queue<String> containerHosts) {
    nodeToLabels = new HashMap<String, Set<String>>();
    
    ProportionalCapacityPreemptionPolicy policy =
        new ProportionalCapacityPreemptionPolicy(conf, mDisp, mCS, mClock,
            labelManager);
    ParentQueue mRoot = buildMockRootQueue(rand, labels, containerHosts, qData);
    when(mCS.getRootQueue()).thenReturn(mRoot);

    Resource clusterResources =
      Resource.newInstance(leafAbsCapacities(qData[0], qData[7]), 0);
    when(mCS.getClusterResource()).thenReturn(clusterResources);
    // by default, queue's resource equals clusterResource when no label exists
    when(
        labelManager.getQueueResource(any(String.class), any(Set.class),
            any(Resource.class))).thenReturn(clusterResources);
    Mockito.doAnswer(new Answer() {
      @Override
      public Object answer(InvocationOnMock invocation) throws Throwable {
        String hostname = (String) invocation.getArguments()[0];
        return nodeToLabels.get(hostname);
      }
    }).when(labelManager).getLabelsOnNode(any(String.class));
    when(labelManager.getNodesToLabels()).thenReturn(nodeToLabels);
    
    // mock scheduler node
    if (hostnames == null) {
      SchedulerNode node = mock(SchedulerNode.class);
      when(node.getNodeName()).thenReturn("mock_host");
      when(node.getTotalResource()).thenReturn(clusterResources);
      when(mCS.getSchedulerNodes()).thenReturn(Arrays.asList(node));
    } else {
      List<SchedulerNode> schedulerNodes = new ArrayList<SchedulerNode>();
      
      for (int i = 0; i < hostnames.length; i++) {
        String hostname = hostnames[i];
        Set<String> nLabels = nodeLabels[i];
        Resource res = nodeResources[i];
        
        SchedulerNode node = mock(SchedulerNode.class);
        when(node.getNodeName()).thenReturn(hostname);
        when(node.getTotalResource()).thenReturn(res);
        nodeToLabels.put(hostname, nLabels);
        schedulerNodes.add(node);
      }
      when(mCS.getSchedulerNodes()).thenReturn(schedulerNodes);
    }
    
    return policy;
  }

  @SuppressWarnings({ "rawtypes", "unchecked" })
  ParentQueue buildMockRootQueue(Random r, Set[] queueLabels,
      Queue<String> containerHosts, int[]... queueData) {
    int[] abs      = queueData[0];
    int[] maxCap   = queueData[1];
    int[] used     = queueData[2];
    int[] pending  = queueData[3];
    int[] reserved = queueData[4];
    int[] apps     = queueData[5];
    int[] gran     = queueData[6];
    int[] queues   = queueData[7];

    return mockNested(abs, maxCap, used, pending, reserved, apps, gran, queues,
        queueLabels, containerHosts);
  }
  
  ParentQueue mockNested(int[] abs, int[] maxCap, int[] used,
      int[] pending, int[] reserved, int[] apps, int[] gran, int[] queues) {
    return mockNested(abs, maxCap, used, pending, reserved, apps, gran, queues,
        null, null);
  }

  @SuppressWarnings("unchecked")
  ParentQueue mockNested(int[] abs, int[] maxCap, int[] used,
      int[] pending, int[] reserved, int[] apps, int[] gran, int[] queues,
      Set<String>[] queueLabels, Queue<String> containerLabels) {
    if (queueLabels == null) {
      queueLabels = new Set[abs.length];
      for (int i = 0; i < queueLabels.length; i++) {
        queueLabels[i] = null;
      }
    }
    
    float tot = leafAbsCapacities(abs, queues);
    Deque<ParentQueue> pqs = new LinkedList<ParentQueue>();
    ParentQueue root = mockParentQueue(null, queues[0], pqs, queueLabels[0]);
    when(root.getQueueName()).thenReturn("/");
    when(root.getAbsoluteUsedCapacity()).thenReturn(used[0] / tot);
    when(root.getAbsoluteCapacity()).thenReturn(abs[0] / tot);
    when(root.getAbsoluteMaximumCapacity()).thenReturn(maxCap[0] / tot);

    for (int i = 1; i < queues.length; ++i) {
      final CSQueue q;
      final ParentQueue p = pqs.removeLast();
      final String queueName = "queue" + ((char)('A' + i - 1));
      if (queues[i] > 0) {
        q = mockParentQueue(p, queues[i], pqs, queueLabels[i]);
      } else {
        q =
            mockLeafQueue(p, tot, i, abs, used, pending, reserved, apps, gran,
                queueLabels[i], containerLabels);
      }
      when(q.getParent()).thenReturn(p);
      when(q.getQueueName()).thenReturn(queueName);
      when(q.getAbsoluteUsedCapacity()).thenReturn(used[i] / tot);
      when(q.getAbsoluteCapacity()).thenReturn(abs[i] / tot);
      when(q.getAbsoluteMaximumCapacity()).thenReturn(maxCap[i] / tot);
    }
    assert 0 == pqs.size();
    return root;
  }

  ParentQueue mockParentQueue(ParentQueue p, int subqueues,
      Deque<ParentQueue> pqs, Set<String> labels) {
    ParentQueue pq = mock(ParentQueue.class);
    List<CSQueue> cqs = new ArrayList<CSQueue>();
    when(pq.getChildQueues()).thenReturn(cqs);
    for (int i = 0; i < subqueues; ++i) {
      pqs.add(pq);
    }
    if (p != null) {
      p.getChildQueues().add(pq);
    }
    return pq;
  }

  LeafQueue mockLeafQueue(ParentQueue p, float tot, int i, int[] abs,
      int[] used, int[] pending, int[] reserved, int[] apps, int[] gran,
      Set<String> queueLabels,
      Queue<String> containerLabels) {
    LeafQueue lq = mock(LeafQueue.class);
    when(lq.getTotalResourcePending()).thenReturn(
        Resource.newInstance(pending[i], 0));
    if (queueLabels != null) {
      when(lq.getLabels()).thenReturn(queueLabels);
    }
    // consider moving where CapacityScheduler::comparator accessible
    NavigableSet<FiCaSchedulerApp> qApps = new TreeSet<FiCaSchedulerApp>(
      new Comparator<FiCaSchedulerApp>() {
        @Override
        public int compare(FiCaSchedulerApp a1, FiCaSchedulerApp a2) {
          return a1.getApplicationAttemptId()
              .compareTo(a2.getApplicationAttemptId());
        }
      });
    // applications are added in global L->R order in queues
    if (apps[i] != 0) {
      int aUsed    = used[i] / apps[i];
      int aPending = pending[i] / apps[i];
      int aReserve = reserved[i] / apps[i];
      for (int a = 0; a < apps[i]; ++a) {
        qApps.add(mockApp(i, appAlloc, aUsed, aPending, aReserve, gran[i],
            containerLabels));
        ++appAlloc;
      }
    }
    when(lq.getApplications()).thenReturn(qApps);
    if(setAMResourcePercent != 0.0f){
      when(lq.getMaxAMResourcePerQueuePercent()).thenReturn(setAMResourcePercent);
    }
    p.getChildQueues().add(lq);
    return lq;
  }

  FiCaSchedulerApp mockApp(int qid, int id, int used, int pending, int reserved,
      int gran, Queue<String> containerHosts) {
    FiCaSchedulerApp app = mock(FiCaSchedulerApp.class);

    ApplicationId appId = ApplicationId.newInstance(TS, id);
    ApplicationAttemptId appAttId = ApplicationAttemptId.newInstance(appId, 0);
    when(app.getApplicationId()).thenReturn(appId);
    when(app.getApplicationAttemptId()).thenReturn(appAttId);

    int cAlloc = 0;
    Resource unit = Resource.newInstance(gran, 0);
    List<RMContainer> cReserved = new ArrayList<RMContainer>();
    for (int i = 0; i < reserved; i += gran) {
      cReserved.add(mockContainer(appAttId, cAlloc, unit, 1));
      ++cAlloc;
    }
    when(app.getReservedContainers()).thenReturn(cReserved);

    List<RMContainer> cLive = new ArrayList<RMContainer>();
    for (int i = 0; i < used; i += gran) {
      if (setAMContainer && i == 0) {
        cLive.add(mockContainer(appAttId, cAlloc, unit, 0,
            containerHosts == null ? null : containerHosts.remove()));
      } else {
        cLive.add(mockContainer(appAttId, cAlloc, unit, 1,
            containerHosts == null ? null : containerHosts.remove()));
      }
      ++cAlloc;
    }
    when(app.getLiveContainers()).thenReturn(cLive);
    return app;
  }
  
  RMContainer mockContainer(ApplicationAttemptId appAttId, int id,
      Resource r, int priority) {
    return mockContainer(appAttId, id, r, priority, null);
  }

  RMContainer mockContainer(ApplicationAttemptId appAttId, int id,
      Resource r, int priority, String host) {
    ContainerId cId = ContainerId.newInstance(appAttId, id);
    Container c = mock(Container.class);
    when(c.getResource()).thenReturn(r);
    when(c.getPriority()).thenReturn(Priority.create(priority));
    if (host != null) {
      when(c.getNodeId()).thenReturn(NodeId.newInstance(host, 0));
    } else {
      when(c.getNodeId()).thenReturn(NodeId.newInstance("mock_host", 0));
    }
    RMContainer mC = mock(RMContainer.class);
    when(mC.getContainerId()).thenReturn(cId);
    when(mC.getContainer()).thenReturn(c);
    when(mC.getApplicationAttemptId()).thenReturn(appAttId);
    if(0 == priority){
      when(mC.isAMContainer()).thenReturn(true);
    }
    return mC;
  }

  static int leafAbsCapacities(int[] abs, int[] subqueues) {
    int ret = 0;
    for (int i = 0; i < abs.length; ++i) {
      if (0 == subqueues[i]) {
        ret += abs[i];
      }
    }
    return ret;
  }

  void printString(CSQueue nq, String indent) {
    if (nq instanceof ParentQueue) {
      System.out.println(indent + nq.getQueueName()
          + " cur:" + nq.getAbsoluteUsedCapacity()
          + " guar:" + nq.getAbsoluteCapacity()
          );
      for (CSQueue q : ((ParentQueue)nq).getChildQueues()) {
        printString(q, indent + "  ");
      }
    } else {
      System.out.println(indent + nq.getQueueName()
          + " pen:" + ((LeafQueue) nq).getTotalResourcePending()
          + " cur:" + nq.getAbsoluteUsedCapacity()
          + " guar:" + nq.getAbsoluteCapacity()
          );
      for (FiCaSchedulerApp a : ((LeafQueue)nq).getApplications()) {
        System.out.println(indent + "  " + a.getApplicationId());
      }
    }
  }

}
