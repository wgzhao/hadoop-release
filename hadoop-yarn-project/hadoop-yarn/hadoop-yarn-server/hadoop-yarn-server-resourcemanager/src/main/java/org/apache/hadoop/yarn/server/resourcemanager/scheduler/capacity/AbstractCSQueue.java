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

import java.io.IOException;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authorize.AccessControlList;
import org.apache.hadoop.yarn.api.records.QueueACL;
import org.apache.hadoop.yarn.api.records.QueueInfo;
import org.apache.hadoop.yarn.api.records.QueueState;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.label.NodeLabelManager;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.QueueMetrics;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerUtils;
import org.apache.hadoop.yarn.util.resource.ResourceCalculator;
import org.apache.hadoop.yarn.util.resource.Resources;

public abstract class AbstractCSQueue implements CSQueue {
  
  CSQueue parent;
  final String queueName;
  
  float capacity;
  float maximumCapacity;
  float absoluteCapacity;
  float absoluteMaxCapacity;
  float absoluteUsedCapacity = 0.0f;

  float usedCapacity = 0.0f;
  volatile int numApplications;
  volatile int numContainers;
  
  final Resource minimumAllocation;
  final Resource maximumAllocation;
  QueueState state;
  final QueueMetrics metrics;
  
  final ResourceCalculator resourceCalculator;
  Set<String> labels;
  NodeLabelManager labelManager;
  String defaultLabelExpression;
  Resource usedResources = Resources.createResource(0, 0);
  QueueInfo queueInfo;
  final Comparator<CSQueue> queueComparator;
  
  boolean reservationsContinueLooking;
  
  Map<QueueACL, AccessControlList> acls = 
      new HashMap<QueueACL, AccessControlList>();
  
  private final RecordFactory recordFactory = 
      RecordFactoryProvider.getRecordFactory(null);
  
  public AbstractCSQueue(CapacitySchedulerContext cs, 
      String queueName, CSQueue parent, CSQueue old) throws IOException {
    this.minimumAllocation = cs.getMinimumResourceCapability();
    this.maximumAllocation = cs.getMaximumResourceCapability();
    this.labelManager = cs.getRMContext().getNodeLabelManager();
    this.parent = parent;
    this.queueName = queueName;
    this.resourceCalculator = cs.getResourceCalculator();
    this.queueComparator = cs.getQueueComparator();
    
    // must be called after parent and queueName is set
    this.metrics = old != null ? old.getMetrics() :
        QueueMetrics.forQueue(getQueuePath(), parent,
            cs.getConfiguration().getEnableUserMetrics(),
            cs.getConf());
    
    this.labels = cs.getConfiguration().getLabels(getQueuePath());
    this.defaultLabelExpression = cs.getConfiguration()
        .getDefaultLabelExpression(getQueuePath());
    
    this.queueInfo = recordFactory.newRecordInstance(QueueInfo.class);
    this.queueInfo.setQueueName(queueName);
  }
  
  @Override
  public synchronized float getCapacity() {
    return capacity;
  }

  @Override
  public synchronized float getAbsoluteCapacity() {
    return absoluteCapacity;
  }

  @Override
  public float getAbsoluteMaximumCapacity() {
    return absoluteMaxCapacity;
  }

  @Override
  public synchronized float getAbsoluteUsedCapacity() {
    return absoluteUsedCapacity;
  }

  @Override
  public float getMaximumCapacity() {
    return maximumCapacity;
  }

  @Override
  public synchronized float getUsedCapacity() {
    return usedCapacity;
  }

  @Override
  public synchronized Resource getUsedResources() {
    return usedResources;
  }

  public synchronized int getNumContainers() {
    return numContainers;
  }
  
  public synchronized int getNumApplications() {
    return numApplications;
  }

  @Override
  public synchronized QueueState getState() {
    return state;
  }
  
  @Override
  public QueueMetrics getMetrics() {
    return metrics;
  }
  
  @Override
  public String getQueueName() {
    return queueName;
  }
  
  @Override
  public synchronized CSQueue getParent() {
    return parent;
  }

  @Override
  public synchronized void setParent(CSQueue newParentQueue) {
    this.parent = (ParentQueue)newParentQueue;
  }
  
  public Set<String> getLabels() {
    return labels;
  }
  
  @Override
  public boolean hasAccess(QueueACL acl, UserGroupInformation user) {
    synchronized (this) {
      if (acls.get(acl).isUserAllowed(user)) {
        return true;
      }
    }
    
    if (parent != null) {
      return parent.hasAccess(acl, user);
    }
    
    return false;
  }
  
  @Override
  public synchronized void setUsedCapacity(float usedCapacity) {
    this.usedCapacity = usedCapacity;
  }
  
  @Override
  public synchronized void setAbsoluteUsedCapacity(float absUsedCapacity) {
    this.absoluteUsedCapacity = absUsedCapacity;
  }

  /**
   * Set maximum capacity - used only for testing.
   * @param maximumCapacity new max capacity
   */
  synchronized void setMaxCapacity(float maximumCapacity) {
    // Sanity check
    CSQueueUtils.checkMaxCapacity(getQueueName(), capacity, maximumCapacity);
    float absMaxCapacity = CSQueueUtils.computeAbsoluteMaximumCapacity(maximumCapacity, parent);
    CSQueueUtils.checkAbsoluteCapacities(getQueueName(), absoluteCapacity, absMaxCapacity);
    
    this.maximumCapacity = maximumCapacity;
    this.absoluteMaxCapacity = absMaxCapacity;
  }

  @Override
  public float getAbsActualCapacity() {
    // for now, simply return actual capacity = guaranteed capacity for parent
    // queue
    return absoluteCapacity;
  }

  @Override
  public String getDefaultLabelExpression() {
    return defaultLabelExpression;
  }
  
  synchronized void setupQueueConfigs(Resource clusterResource,
      float capacity, float absoluteCapacity, float maximumCapacity,
      float absoluteMaxCapacity, QueueState state,
      Map<QueueACL, AccessControlList> acls, boolean continueLooking, 
      Set<String> labels, String defaultLabelExpression) throws IOException {
    // Sanity check
    CSQueueUtils.checkMaxCapacity(getQueueName(), capacity, maximumCapacity);
    CSQueueUtils.checkAbsoluteCapacities(getQueueName(), absoluteCapacity,
        absoluteMaxCapacity);

    this.reservationsContinueLooking = continueLooking;

    this.capacity = capacity;
    this.absoluteCapacity = absoluteCapacity;

    this.maximumCapacity = maximumCapacity;
    this.absoluteMaxCapacity = absoluteMaxCapacity;

    this.state = state;

    this.acls = acls;
    
    // set labels
    this.labels = labels;
    if (this.labels == null && parent != null) {
      this.labels = parent.getLabels();
      SchedulerUtils.checkAndThrowIfLabelNotIncluded(labelManager, this.labels);
    }
    
    // set label expression
    this.defaultLabelExpression = defaultLabelExpression;
    if (this.defaultLabelExpression == null && parent != null) {
      this.defaultLabelExpression = parent.getDefaultLabelExpression();
    }
    
    this.queueInfo.setLabels(this.labels);
    this.queueInfo.setCapacity(this.capacity);
    this.queueInfo.setMaximumCapacity(this.maximumCapacity);
    this.queueInfo.setQueueState(this.state);
    this.queueInfo.setDefaultLabelExpression(this.defaultLabelExpression);

    // Update metrics
    CSQueueUtils.updateQueueStatistics(
        resourceCalculator, this, parent, clusterResource, minimumAllocation);
  }
  
  @Private
  public Resource getMaximumAllocation() {
    return maximumAllocation;
  }
  
  @Private
  public Resource getMinimumAllocation() {
    return minimumAllocation;
  }
}
