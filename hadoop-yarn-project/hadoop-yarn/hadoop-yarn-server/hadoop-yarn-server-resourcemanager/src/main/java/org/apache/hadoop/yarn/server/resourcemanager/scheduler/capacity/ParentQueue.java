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
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Evolving;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authorize.AccessControlList;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.QueueACL;
import org.apache.hadoop.yarn.api.records.QueueInfo;
import org.apache.hadoop.yarn.api.records.QueueState;
import org.apache.hadoop.yarn.api.records.QueueUserACLInfo;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.label.NodeLabelManager;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainer;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainerEventType;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainerState;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ActiveUsersManager;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.NodeType;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerApplicationAttempt;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerUtils;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.fica.FiCaSchedulerApp;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.fica.FiCaSchedulerNode;
import org.apache.hadoop.yarn.util.resource.Resources;

@Private
@Evolving
public class ParentQueue extends AbstractCSQueue {

  private static final Log LOG = LogFactory.getLog(ParentQueue.class);

  private final Set<CSQueue> childQueues;
  
  private final boolean rootQueue;

  private final RecordFactory recordFactory = 
    RecordFactoryProvider.getRecordFactory(null);

  public ParentQueue(CapacitySchedulerContext cs, 
      String queueName, CSQueue parent, CSQueue old) throws IOException {
    super(cs, queueName, parent, old);

    this.rootQueue = (parent == null);

    float rawCapacity = cs.getConfiguration().getCapacity(getQueuePath());

    if (rootQueue &&
        (rawCapacity != CapacitySchedulerConfiguration.MAXIMUM_CAPACITY_VALUE)) {
      throw new IllegalArgumentException("Illegal " +
          "capacity of " + rawCapacity + " for queue " + queueName +
          ". Must be " + CapacitySchedulerConfiguration.MAXIMUM_CAPACITY_VALUE);
    }

    float capacity = (float) rawCapacity / 100;
    float parentAbsoluteCapacity = 
      (rootQueue) ? 1.0f : parent.getAbsoluteCapacity();
    float absoluteCapacity = parentAbsoluteCapacity * capacity; 

    float  maximumCapacity =
      (float) cs.getConfiguration().getMaximumCapacity(getQueuePath()) / 100;
    float absoluteMaxCapacity = 
          CSQueueUtils.computeAbsoluteMaximumCapacity(maximumCapacity, parent);
    
    QueueState state = cs.getConfiguration().getState(getQueuePath());

    Map<QueueACL, AccessControlList> acls = 
      cs.getConfiguration().getAcls(getQueuePath());

    this.queueInfo.setChildQueues(new ArrayList<QueueInfo>());

    setupQueueConfigs(cs.getClusterResource(), capacity, absoluteCapacity,
        maximumCapacity, absoluteMaxCapacity, state, acls, labels,
        defaultLabelExpression);
    
    this.childQueues = new TreeSet<CSQueue>(queueComparator);

    LOG.info("Initialized parent-queue " + queueName + 
        " name=" + queueName + 
        ", fullname=" + getQueuePath()); 
  }

  synchronized void setupQueueConfigs(Resource clusterResource,
      float capacity, float absoluteCapacity, float maximumCapacity,
      float absoluteMaxCapacity, QueueState state,
      Map<QueueACL, AccessControlList> acls, Set<String> labels,
      String defaultLabelExpression) throws IOException {
    super.setupQueueConfigs(clusterResource, capacity, absoluteCapacity,
        maximumCapacity, absoluteMaxCapacity, state, acls, labels,
        defaultLabelExpression);
    this.absoluteCapacity = absoluteCapacity;
    StringBuilder aclsString = new StringBuilder();
    for (Map.Entry<QueueACL, AccessControlList> e : acls.entrySet()) {
      aclsString.append(e.getKey() + ":" + e.getValue().getAclString());
    }

    StringBuilder labelStrBuilder = new StringBuilder(); 
    if (labels != null) {
      for (String s : labels) {
        labelStrBuilder.append(s);
        labelStrBuilder.append(",");
      }
    }

    LOG.info(queueName +
        ", capacity=" + capacity +
        ", asboluteCapacity=" + absoluteCapacity +
        ", maxCapacity=" + maximumCapacity +
        ", asboluteMaxCapacity=" + absoluteMaxCapacity + 
        ", state=" + state +
        ", acls=" + aclsString + 
        ", labels=" + labelStrBuilder.toString() + "\n");
  }

  private static float PRECISION = 0.0005f; // 0.05% precision
  void setChildQueues(Collection<CSQueue> childQueues) {
    
    // Validate
    float childCapacities = 0;
    for (CSQueue queue : childQueues) {
      childCapacities += queue.getCapacity();
    }
    float delta = Math.abs(1.0f - childCapacities);  // crude way to check
    // allow capacities being set to 0, and enforce child 0 if parent is 0
    if (((capacity > 0) && (delta > PRECISION)) || 
        ((capacity == 0) && (childCapacities > 0))) {
      throw new IllegalArgumentException("Illegal" +
      		" capacity of " + childCapacities + 
      		" for children of queue " + queueName);
    }
    
    this.childQueues.clear();
    this.childQueues.addAll(childQueues);
    if (LOG.isDebugEnabled()) {
      LOG.debug("setChildQueues: " + getChildQueuesToPrint());
    }
  }

  @Override
  public String getQueuePath() {
    String parentPath = ((parent == null) ? "" : (parent.getQueuePath() + "."));
    return parentPath + getQueueName();
  }

  @Override
  public synchronized QueueInfo getQueueInfo( 
      boolean includeChildQueues, boolean recursive) {
    queueInfo.setCurrentCapacity(usedCapacity);

    List<QueueInfo> childQueuesInfo = new ArrayList<QueueInfo>();
    if (includeChildQueues) {
      for (CSQueue child : childQueues) {
        // Get queue information recursively?
        childQueuesInfo.add(
            child.getQueueInfo(recursive, recursive));
      }
    }
    queueInfo.setChildQueues(childQueuesInfo);
    
    return queueInfo;
  }

  private synchronized QueueUserACLInfo getUserAclInfo(
      UserGroupInformation user) {
    QueueUserACLInfo userAclInfo = 
      recordFactory.newRecordInstance(QueueUserACLInfo.class);
    List<QueueACL> operations = new ArrayList<QueueACL>();
    for (QueueACL operation : QueueACL.values()) {
      if (hasAccess(operation, user)) {
        operations.add(operation);
      } 
    }

    userAclInfo.setQueueName(getQueueName());
    userAclInfo.setUserAcls(operations);
    return userAclInfo;
  }
  
  @Override
  public synchronized List<QueueUserACLInfo> getQueueUserAclInfo(
      UserGroupInformation user) {
    List<QueueUserACLInfo> userAcls = new ArrayList<QueueUserACLInfo>();
    
    // Add parent queue acls
    userAcls.add(getUserAclInfo(user));
    
    // Add children queue acls
    for (CSQueue child : childQueues) {
      userAcls.addAll(child.getQueueUserAclInfo(user));
    }
 
    return userAcls;
  }

  public String toString() {
    return queueName + ": " +
        "numChildQueue= " + childQueues.size() + ", " + 
        "capacity=" + capacity + ", " +  
        "absoluteCapacity=" + absoluteCapacity + ", " +
        "usedResources=" + usedResources + 
        "usedCapacity=" + getUsedCapacity() + ", " + 
        "numApps=" + getNumApplications() + ", " + 
        "numContainers=" + getNumContainers();
  }
  
  @Override
  public synchronized void reinitialize(
      CSQueue newlyParsedQueue, Resource clusterResource)
  throws IOException {
    // Sanity check
    if (!(newlyParsedQueue instanceof ParentQueue) ||
        !newlyParsedQueue.getQueuePath().equals(getQueuePath())) {
      throw new IOException("Trying to reinitialize " + getQueuePath() +
          " from " + newlyParsedQueue.getQueuePath());
    }

    ParentQueue newlyParsedParentQueue = (ParentQueue)newlyParsedQueue;

    // Set new configs
    setupQueueConfigs(clusterResource,
        newlyParsedParentQueue.capacity, 
        newlyParsedParentQueue.absoluteCapacity,
        newlyParsedParentQueue.maximumCapacity, 
        newlyParsedParentQueue.absoluteMaxCapacity,
        newlyParsedParentQueue.state, 
        newlyParsedParentQueue.acls,
        newlyParsedParentQueue.labels,
        newlyParsedParentQueue.defaultLabelExpression);

    // Re-configure existing child queues and add new ones
    // The CS has already checked to ensure all existing child queues are present!
    Map<String, CSQueue> currentChildQueues = getQueues(childQueues);
    Map<String, CSQueue> newChildQueues = 
        getQueues(newlyParsedParentQueue.childQueues);
    for (Map.Entry<String, CSQueue> e : newChildQueues.entrySet()) {
      String newChildQueueName = e.getKey();
      CSQueue newChildQueue = e.getValue();

      CSQueue childQueue = currentChildQueues.get(newChildQueueName);
      
      // Check if the child-queue already exists
      if (childQueue != null) {
        // Re-init existing child queues
        childQueue.reinitialize(newChildQueue, clusterResource);
        LOG.info(getQueueName() + ": re-configured queue: " + childQueue);
      } else {
        // New child queue, do not re-init
        
        // Set parent to 'this'
        newChildQueue.setParent(this);
        
        // Save in list of current child queues
        currentChildQueues.put(newChildQueueName, newChildQueue);
        
        LOG.info(getQueueName() + ": added new child queue: " + newChildQueue);
      }
    }

    // Re-sort all queues
    childQueues.clear();
    childQueues.addAll(currentChildQueues.values());
  }

  Map<String, CSQueue> getQueues(Set<CSQueue> queues) {
    Map<String, CSQueue> queuesMap = new HashMap<String, CSQueue>();
    for (CSQueue queue : queues) {
      queuesMap.put(queue.getQueueName(), queue);
    }
    return queuesMap;
  }

  @Override
  public void submitApplication(ApplicationId applicationId, String user,
      String queue) throws AccessControlException {
    
    synchronized (this) {
      // Sanity check
      if (queue.equals(queueName)) {
        throw new AccessControlException("Cannot submit application " +
            "to non-leaf queue: " + queueName);
      }
      
      if (state != QueueState.RUNNING) {
        throw new AccessControlException("Queue " + getQueuePath() +
            " is STOPPED. Cannot accept submission of application: " +
            applicationId);
      }

      addApplication(applicationId, user);
    }
    
    // Inform the parent queue
    if (parent != null) {
      try {
        parent.submitApplication(applicationId, user, queue);
      } catch (AccessControlException ace) {
        LOG.info("Failed to submit application to parent-queue: " + 
            parent.getQueuePath(), ace);
        removeApplication(applicationId, user);
        throw ace;
      }
    }
  }


  @Override
  public void submitApplicationAttempt(FiCaSchedulerApp application,
      String userName) {
    // submit attempt logic.
  }

  @Override
  public void finishApplicationAttempt(FiCaSchedulerApp application,
      String queue) {
    // finish attempt logic.
  }

  private synchronized void addApplication(ApplicationId applicationId,
      String user) {

    ++numApplications;

    LOG.info("Application added -" +
        " appId: " + applicationId + 
        " user: " + user + 
        " leaf-queue of parent: " + getQueueName() + 
        " #applications: " + getNumApplications());
  }
  
  @Override
  public void finishApplication(ApplicationId application, String user) {
    
    synchronized (this) {
      removeApplication(application, user);
    }
    
    // Inform the parent queue
    if (parent != null) {
      parent.finishApplication(application, user);
    }
  }

  public synchronized void removeApplication(ApplicationId applicationId, 
      String user) {
    
    --numApplications;

    LOG.info("Application removed -" +
        " appId: " + applicationId + 
        " user: " + user + 
        " leaf-queue of parent: " + getQueueName() + 
        " #applications: " + getNumApplications());
  }

  @Override
  public synchronized CSAssignment assignContainers(
      Resource clusterResource, FiCaSchedulerNode node) {
    CSAssignment assignment = 
        new CSAssignment(Resources.createResource(0, 0), NodeType.NODE_LOCAL);
    
    while (canAssign(clusterResource, node)) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Trying to assign containers to child-queue of "
          + getQueueName());
      }
      
      // Are we over maximum-capacity for this queue?
      if (!assignToQueue(clusterResource)) {
        break;
      }
      
      // Schedule
      CSAssignment assignedToChild = 
          assignContainersToChildQueues(clusterResource, node);
      assignment.setType(assignedToChild.getType());
      
      // Done if no child-queue assigned anything
      if (Resources.greaterThan(
              resourceCalculator, clusterResource, 
              assignedToChild.getResource(), Resources.none())) {
        // Track resource utilization for the parent-queue
        allocateResource(clusterResource, assignedToChild.getResource());
        
        // Track resource utilization in this pass of the scheduler
        Resources.addTo(assignment.getResource(), assignedToChild.getResource());
        
        LOG.info("assignedContainer" +
            " queue=" + getQueueName() + 
            " usedCapacity=" + getUsedCapacity() +
            " absoluteUsedCapacity=" + getAbsoluteUsedCapacity() +
            " used=" + usedResources + 
            " cluster=" + clusterResource);

      } else {
        break;
      }

      if (LOG.isDebugEnabled()) {
        LOG.debug("ParentQ=" + getQueueName()
          + " assignedSoFarInThisIteration=" + assignment.getResource()
          + " usedCapacity=" + getUsedCapacity()
          + " absoluteUsedCapacity=" + getAbsoluteUsedCapacity());
      }

      // Do not assign more than one container if this isn't the root queue
      // or if we've already assigned an off-switch container
      if (!rootQueue || assignment.getType() == NodeType.OFF_SWITCH) {
        if (LOG.isDebugEnabled()) {
          if (rootQueue && assignment.getType() == NodeType.OFF_SWITCH) {
            LOG.debug("Not assigning more than one off-switch container," +
                " assignments so far: " + assignment);
          }
        }
        break;
      }
    } 
    
    return assignment;
  }

  private synchronized boolean assignToQueue(Resource clusterResource) {
    // Check how of the cluster's absolute capacity we are currently using...
    float currentCapacity =
        Resources.divide(
            resourceCalculator, clusterResource, 
            usedResources, clusterResource);
    
    if (currentCapacity >= absoluteMaxCapacity) {
      LOG.info(getQueueName() + 
          " used=" + usedResources + 
          " current-capacity (" + currentCapacity + ") " +
          " >= max-capacity (" + absoluteMaxCapacity + ")");
      return false;
    }
    return true;

  }
  
  private boolean canAssign(Resource clusterResource, FiCaSchedulerNode node) {
    return (node.getReservedContainer() == null) && 
        Resources.greaterThanOrEqual(resourceCalculator, clusterResource, 
            node.getAvailableResource(), minimumAllocation);
  }
  
  synchronized CSAssignment assignContainersToChildQueues(Resource cluster, 
      FiCaSchedulerNode node) {
    CSAssignment assignment = 
        new CSAssignment(Resources.createResource(0, 0), NodeType.NODE_LOCAL);
    
    printChildQueues();

    // Try to assign to most 'under-served' sub-queue
    for (Iterator<CSQueue> iter=childQueues.iterator(); iter.hasNext();) {
      CSQueue childQueue = iter.next();
      if(LOG.isDebugEnabled()) {
        LOG.debug("Trying to assign to queue: " + childQueue.getQueuePath()
          + " stats: " + childQueue);
      }
      assignment = childQueue.assignContainers(cluster, node);
      if(LOG.isDebugEnabled()) {
        LOG.debug("Assigned to queue: " + childQueue.getQueuePath() +
          " stats: " + childQueue + " --> " + 
          assignment.getResource() + ", " + assignment.getType());
      }

      // If we do assign, remove the queue and re-insert in-order to re-sort
      if (Resources.greaterThan(
              resourceCalculator, cluster, 
              assignment.getResource(), Resources.none())) {
        // Remove and re-insert to sort
        iter.remove();
        LOG.info("Re-sorting assigned queue: " + childQueue.getQueuePath() + 
            " stats: " + childQueue);
        childQueues.add(childQueue);
        if (LOG.isDebugEnabled()) {
          printChildQueues();
        }
        break;
      }
    }
    
    return assignment;
  }

  String getChildQueuesToPrint() {
    StringBuilder sb = new StringBuilder();
    for (CSQueue q : childQueues) {
      sb.append(q.getQueuePath() + "(" + q.getUsedCapacity() + "), ");
    }
    return sb.toString();
  }
  void printChildQueues() {
    if (LOG.isDebugEnabled()) {
      LOG.debug("printChildQueues - queue: " + getQueuePath()
        + " child-queues: " + getChildQueuesToPrint());
    }
  }
  
  @Override
  public void completedContainer(Resource clusterResource,
      FiCaSchedulerApp application, FiCaSchedulerNode node, 
      RMContainer rmContainer, ContainerStatus containerStatus, 
      RMContainerEventType event, CSQueue completedChildQueue) {
    if (application != null) {
      // Careful! Locking order is important!
      // Book keeping
      synchronized (this) {
        releaseResource(clusterResource, 
            rmContainer.getContainer().getResource());

        LOG.info("completedContainer" +
            " queue=" + getQueueName() + 
            " usedCapacity=" + getUsedCapacity() +
            " absoluteUsedCapacity=" + getAbsoluteUsedCapacity() +
            " used=" + usedResources + 
            " cluster=" + clusterResource);
      }

      // reinsert the updated queue
      for (Iterator<CSQueue> iter=childQueues.iterator(); iter.hasNext();) {
        CSQueue csqueue = iter.next();
        if(csqueue.equals(completedChildQueue))
        {
          iter.remove();
          LOG.info("Re-sorting completed queue: " + csqueue.getQueuePath() + 
              " stats: " + csqueue);
          childQueues.add(csqueue);
          break;
        }
      }
      
      // Inform the parent
      if (parent != null) {
        // complete my parent
        parent.completedContainer(clusterResource, application, 
            node, rmContainer, null, event, this);
      }    
    }
  }
  
  synchronized void allocateResource(Resource clusterResource, 
      Resource resource) {
    Resources.addTo(usedResources, resource);
    CSQueueUtils.updateQueueStatistics(
        resourceCalculator, this, parent, clusterResource, minimumAllocation);
    ++numContainers;
  }
  
  synchronized void releaseResource(Resource clusterResource, 
      Resource resource) {
    Resources.subtractFrom(usedResources, resource);
    CSQueueUtils.updateQueueStatistics(
        resourceCalculator, this, parent, clusterResource, minimumAllocation);
    --numContainers;
  }

  @Override
  public synchronized void updateClusterResource(Resource clusterResource) {
    // Update all children
    for (CSQueue childQueue : childQueues) {
      childQueue.updateClusterResource(clusterResource);
    }
    
    // Update metrics
    CSQueueUtils.updateQueueStatistics(
        resourceCalculator, this, parent, clusterResource, minimumAllocation);
  }
  
  @Override
  public synchronized List<CSQueue> getChildQueues() {
    return new ArrayList<CSQueue>(childQueues);
  }
  
  @Override
  public void recoverContainer(Resource clusterResource,
      SchedulerApplicationAttempt attempt, RMContainer rmContainer) {
    if (rmContainer.getState().equals(RMContainerState.COMPLETED)) {
      return;
    }
    // Careful! Locking order is important! 
    synchronized (this) {
      allocateResource(clusterResource,rmContainer.getContainer().getResource());
    }
    if (parent != null) {
      parent.recoverContainer(clusterResource, attempt, rmContainer);
    }
  }
  
  @Override
  public ActiveUsersManager getActiveUsersManager() {
    // Should never be called since all applications are submitted to LeafQueues
    return null;
  }

  @Override
  public void collectSchedulerApplications(
      Collection<ApplicationAttemptId> apps) {
    for (CSQueue queue : childQueues) {
      queue.collectSchedulerApplications(apps);
    }
  }

  @Override
  public void attachContainer(Resource clusterResource,
      FiCaSchedulerApp application, RMContainer rmContainer) {
    if (application != null) {
      allocateResource(clusterResource, rmContainer.getContainer()
          .getResource());
      LOG.info("movedContainer" + " queueMoveIn=" + getQueueName()
          + " usedCapacity=" + getUsedCapacity() + " absoluteUsedCapacity="
          + getAbsoluteUsedCapacity() + " used=" + usedResources + " cluster="
          + clusterResource);
      // Inform the parent
      if (parent != null) {
        parent.attachContainer(clusterResource, application, rmContainer);
      }
    }
  }

  @Override
  public void detachContainer(Resource clusterResource,
      FiCaSchedulerApp application, RMContainer rmContainer) {
    if (application != null) {
      releaseResource(clusterResource, rmContainer.getContainer().getResource());
      LOG.info("movedContainer" + " queueMoveOut=" + getQueueName()
          + " usedCapacity=" + getUsedCapacity() + " absoluteUsedCapacity="
          + getAbsoluteUsedCapacity() + " used=" + usedResources + " cluster="
          + clusterResource);
      // Inform the parent
      if (parent != null) {
        parent.detachContainer(clusterResource, application, rmContainer);
      }
    }
  }

  public Set<String> getLabels() {
    return labels;
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
}
