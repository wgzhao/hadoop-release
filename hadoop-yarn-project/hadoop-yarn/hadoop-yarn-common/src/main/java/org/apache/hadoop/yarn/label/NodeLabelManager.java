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
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.ReadLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.WriteLock;
import java.util.regex.Pattern;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authorize.AccessControlList;
import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.event.AsyncDispatcher;
import org.apache.hadoop.yarn.event.Dispatcher;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.label.event.AddLabelsEvent;
import org.apache.hadoop.yarn.label.event.NodeLabelManagerEvent;
import org.apache.hadoop.yarn.label.event.NodeLabelManagerEventType;
import org.apache.hadoop.yarn.label.event.RemoveLabelsEvent;
import org.apache.hadoop.yarn.label.event.StoreNodeToLabelsEvent;
import org.apache.hadoop.yarn.state.InvalidStateTransitonException;
import org.apache.hadoop.yarn.state.SingleArcTransition;
import org.apache.hadoop.yarn.state.StateMachine;
import org.apache.hadoop.yarn.state.StateMachineFactory;
import org.apache.hadoop.yarn.util.resource.Resources;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;

public abstract class NodeLabelManager extends AbstractService {
  protected static final Log LOG = LogFactory.getLog(NodeLabelManager.class);
  private static final int MAX_LABEL_LENGTH = 255;
  public static final Set<String> EMPTY_STRING_SET = Collections
      .unmodifiableSet(new HashSet<String>(0));
  public static final String ANY = "*";
  public static final Set<String> ACCESS_ANY_LABEL_SET = ImmutableSet.of(ANY);
  private static final Pattern LABEL_PATTERN = Pattern
      .compile("^[0-9a-zA-Z][0-9a-zA-z-_]*");

  /**
   * If a user doesn't specify label of a queue or node, it belongs
   * DEFAULT_LABEL
   */
  public static final String NO_LABEL = "";

  private enum NodeLabelManagerState {
    DEFAULT
  };

  protected enum SerializedLogType {
    ADD_LABELS, NODE_TO_LABELS, REMOVE_LABELS
  }

  private static final StateMachineFactory<NodeLabelManager, NodeLabelManagerState, NodeLabelManagerEventType, NodeLabelManagerEvent> stateMachineFactory =
      new StateMachineFactory<NodeLabelManager, NodeLabelManagerState, NodeLabelManagerEventType, NodeLabelManagerEvent>(
          NodeLabelManagerState.DEFAULT)
          .addTransition(NodeLabelManagerState.DEFAULT,
              NodeLabelManagerState.DEFAULT,
              NodeLabelManagerEventType.STORE_NODE_TO_LABELS,
              new StoreNodeToLabelsTransition())
          .addTransition(NodeLabelManagerState.DEFAULT,
              NodeLabelManagerState.DEFAULT,
              NodeLabelManagerEventType.ADD_LABELS, new AddLabelsTransition())
          .addTransition(NodeLabelManagerState.DEFAULT,
              NodeLabelManagerState.DEFAULT,
              NodeLabelManagerEventType.REMOVE_LABELS,
              new RemoveLabelsTransition());

  private final StateMachine<NodeLabelManagerState, NodeLabelManagerEventType, NodeLabelManagerEvent> stateMachine;

  private static class StoreNodeToLabelsTransition implements
      SingleArcTransition<NodeLabelManager, NodeLabelManagerEvent> {
    @Override
    public void transition(NodeLabelManager store, NodeLabelManagerEvent event) {
      if (!(event instanceof StoreNodeToLabelsEvent)) {
        // should never happen
        LOG.error("Illegal event type: " + event.getClass());
        return;
      }
      StoreNodeToLabelsEvent e = (StoreNodeToLabelsEvent) event;
      try {
        store.persistNodeToLabelsChanges(e.getNodeToLabels());
      } catch (IOException ioe) {
        LOG.error("Error removing store node to label:" + ioe.getMessage());
        throw new YarnRuntimeException(ioe);
      }
    };
  }

  private static class AddLabelsTransition implements
      SingleArcTransition<NodeLabelManager, NodeLabelManagerEvent> {
    @Override
    public void transition(NodeLabelManager store, NodeLabelManagerEvent event) {
      if (!(event instanceof AddLabelsEvent)) {
        // should never happen
        LOG.error("Illegal event type: " + event.getClass());
        return;
      }
      AddLabelsEvent e = (AddLabelsEvent) event;
      try {
        store.persistAddingLabels(e.getLabels());
      } catch (IOException ioe) {
        LOG.error("Error storing new label:" + ioe.getMessage());
        throw new YarnRuntimeException(ioe);
      }
    };
  }

  private static class RemoveLabelsTransition implements
      SingleArcTransition<NodeLabelManager, NodeLabelManagerEvent> {
    @Override
    public void transition(NodeLabelManager store, NodeLabelManagerEvent event) {
      if (!(event instanceof RemoveLabelsEvent)) {
        // should never happen
        LOG.error("Illegal event type: " + event.getClass());
        return;
      }
      RemoveLabelsEvent e = (RemoveLabelsEvent) event;
      try {
        store.persistRemovingLabels(e.getLabels());
      } catch (IOException ioe) {
        LOG.error("Error removing label on filesystem:" + ioe.getMessage());
        throw new YarnRuntimeException(ioe);
      }
    };
  }

  protected Dispatcher dispatcher;

  // existing labels in the cluster
  protected Set<String> existingLabels = new ConcurrentSkipListSet<String>();

  // node to labels and label to nodes
  protected Map<String, Set<String>> nodeToLabels =
      new ConcurrentHashMap<String, Set<String>>();
  private Map<String, Set<String>> labelToNodes =
      new ConcurrentHashMap<String, Set<String>>();

  // running node and label to running nodes
  private ConcurrentMap<String, Set<String>> labelToActiveNodes =
      new ConcurrentHashMap<String, Set<String>>();
  private Set<String> runningNodes = new ConcurrentSkipListSet<String>();

  // recording label to queues and queue to Resource
  private ConcurrentMap<String, Set<String>> queueToLabels =
      new ConcurrentHashMap<String, Set<String>>();
  private ConcurrentMap<String, Resource> queueToResource =
      new ConcurrentHashMap<String, Resource>();

  // node name -> map<nodeId, resource>
  // This is used to calculate how much resource in each node, use a nested map
  // because it is possible multiple NMs launch in a node
  private Map<String, ConcurrentMap<NodeId, Resource>> nodeToResource =
      new ConcurrentHashMap<String, ConcurrentMap<NodeId, Resource>>();
  private Map<String, Resource> labelToResource =
      new ConcurrentHashMap<String, Resource>();

  private final ReadLock readLock;
  private final WriteLock writeLock;
  private AccessControlList adminAcl;

  public NodeLabelManager() {
    super(NodeLabelManager.class.getName());
    stateMachine = stateMachineFactory.make(this);
    ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    readLock = lock.readLock();
    writeLock = lock.writeLock();
  }

  // for UT purpose
  protected void initDispatcher(Configuration conf) {
    // create async handler
    dispatcher = new AsyncDispatcher();
    AsyncDispatcher asyncDispatcher = (AsyncDispatcher) dispatcher;
    asyncDispatcher.init(conf);
    asyncDispatcher.setDrainEventsOnStop();
  }

  @Override
  protected void serviceInit(Configuration conf) throws Exception {
    adminAcl = new AccessControlList(conf.get(
        YarnConfiguration.YARN_ADMIN_ACL,
        YarnConfiguration.DEFAULT_YARN_ADMIN_ACL));

    // recover from previous state
    recover();
  }

  public boolean checkAccess(UserGroupInformation user) {
    // make sure only admin can invoke
    // this method
    if (adminAcl.isUserAllowed(user)) {
      return true;
    }
    return false;
  }

  Map<String, Set<String>> getDefaultNodeToLabels(NodeLabelConfiguration conf)
      throws IOException {
    return conf.getNodeToLabels();
  }

  protected void addDefaultNodeToLabels(
      Map<String, Set<String>> defaultNodeToLabels) throws IOException {
    Set<String> labels = new HashSet<String>();
    for (Set<String> t : defaultNodeToLabels.values()) {
      labels.addAll(t);
    }
    addLabels(labels);

    setLabelsOnMultipleNodes(defaultNodeToLabels);
  }

  // for UT purpose
  protected void startDispatcher() {
    // start dispatcher
    AsyncDispatcher asyncDispatcher = (AsyncDispatcher) dispatcher;
    asyncDispatcher.start();
  }

  @Override
  protected void serviceStart() throws Exception {
    // init dispatcher only when service start, because recover will happen in
    // service init, we don't want to trigger any event handling at that time.
    initDispatcher(getConfig());

    dispatcher.register(NodeLabelManagerEventType.class,
        new ForwardingEventHandler());
    
    startDispatcher();
  }

  /**
   * Store node -> label to filesystem
   */
  public abstract void persistNodeToLabelsChanges(
      Map<String, Set<String>> nodeToLabels) throws IOException;

  /**
   * Store new label to filesystem
   */
  public abstract void persistAddingLabels(Set<String> label)
      throws IOException;

  /*
   * Remove label from filesystem
   */
  public abstract void persistRemovingLabels(Collection<String> labels)
      throws IOException;

  /**
   * Recover node label from file system
   */
  public abstract void recover()
      throws IOException;

  protected void checkLabelName(String label) throws IOException {    
    if (label == null || label.isEmpty() || label.length() > MAX_LABEL_LENGTH) {
      throw new IOException("label added is empty or exceeds "
          + MAX_LABEL_LENGTH + " character(s)");
    }
    label = label.trim();

    boolean match = LABEL_PATTERN.matcher(label).matches();

    if (!match) {
      throw new IOException("label name should only contains "
          + "{0-9, a-z, A-Z, -, _} and should not started with {-,_}"
          + ", now it is=" + label);
    }
  }

  protected String normalizeLabel(String label) {
    if (label != null) {
      return label.trim();
    }
    return NO_LABEL;
  }

  protected Set<String> normalizeLabels(Set<String> labels) {
    Set<String> newLabels = new HashSet<String>();
    for (String label : labels) {
      newLabels.add(normalizeLabel(label));
    }
    return newLabels;
  }

  /**
   * Add a label to repository
   * 
   * @param label
   *          label label
   */
  public void addLabel(String label) throws IOException {
    checkLabelName(label);
    addLabels(ImmutableSet.of(label));
  }

  /**
   * Add multiple labels to repository
   * 
   * @param labels new labels added
   */
  @SuppressWarnings("unchecked")
  public void addLabels(Set<String> labels) throws IOException {    
    if (null == labels || labels.isEmpty()) {
      return;
    }
    
    try {
      writeLock.lock();
      Set<String> normalizedLabels = new HashSet<String>();
      for (String label : labels) {
        checkLabelName(label);
        String normalizedLabel = normalizeLabel(label);
        this.existingLabels.add(normalizedLabel);
        normalizedLabels.add(normalizedLabel);
      }
      if (null != dispatcher) {
        dispatcher.getEventHandler().handle(
            new AddLabelsEvent(normalizedLabels));
      }

      LOG.info("Add labels: [" + StringUtils.join(labels.iterator(), ",") + "]");
    } finally {
      writeLock.unlock();
    }
  }

  /**
   * Remove a label from repository
   * 
   * @param labelToRemove
   * @throws IOException
   */
  public void removeLabel(String labelToRemove) throws IOException {
    removeLabels(Arrays.asList(labelToRemove));
  }

  private void addNMInNodeAlreadyHasNM(Set<String> labels, Resource newNMRes) {
    try {
      writeLock.lock();
      for (String label : labels) {
        Resource originalRes = labelToResource.get(label);
        labelToResource.put(label, Resources.add(newNMRes, originalRes));
      }
      for (String queueName : queueToLabels.keySet()) {
        if (isNodeUsableByQueue(labels, queueName)) {
          Resource res = queueToResource.get(queueName);
          Resources.addTo(res, newNMRes);
        }
      }
    } finally {
      writeLock.unlock();
    }
  }

  private void
      removeNMToNodeAlreadyHasNM(Set<String> labels, Resource newNMRes) {
    try {
      writeLock.lock();
      for (String label : labels) {
        Resource originalRes = labelToResource.get(label);
        labelToResource.put(label, Resources.subtract(originalRes, newNMRes));
      }
      for (String queueName : queueToLabels.keySet()) {
        if (isNodeUsableByQueue(labels, queueName)) {
          Resource res = queueToResource.get(queueName);
          Resources.subtractFrom(res, newNMRes);
        }
      }
    } finally {
      writeLock.unlock();
    }
  }
  
  private enum UpdateLabelResourceType {
    ACTIVE,
    DEACTIVE,
    UPDATE_LABEL
  }

  private void updateLabelResource(Map<String, Set<String>> addLabelToNodes,
      Map<String, Set<String>> removeLabelToNodes,
      Map<String, Set<String>> originalNodeToLabels,
      UpdateLabelResourceType updateType) {
    try {
      writeLock.lock();

      // process add label to nodes
      if (addLabelToNodes != null) {
        for (Entry<String, Set<String>> entry : addLabelToNodes.entrySet()) {
          String label = entry.getKey();
          Set<String> nodes = entry.getValue();

          // update label to active nodes
          labelToActiveNodes.putIfAbsent(label, new HashSet<String>());
          labelToActiveNodes.get(label).addAll(addLabelToNodes.get(label));

          // update label to resource
          Resource res = Resource.newInstance(0, 0);
          for (String node : nodes) {
            Resources.addTo(res, getResourceOfNode(node));
          }
          Resource originalRes = labelToResource.get(label);
          labelToResource.put(label,
              originalRes == null ? res : Resources.add(res, originalRes));
        }
      }

      // process remove label to nodes
      if (removeLabelToNodes != null) {
        for (Entry<String, Set<String>> entry : removeLabelToNodes.entrySet()) {
          String label = entry.getKey();
          Set<String> nodes = entry.getValue();

          // update label to active nodes
          if (labelToActiveNodes.get(label) != null) {
            labelToActiveNodes.get(label).removeAll(nodes);
          }

          // update label to resource
          Resource res = Resource.newInstance(0, 0);
          for (String node : nodes) {
            Resources.addTo(res, getResourceOfNode(node));
          }
          Resource originalRes = labelToResource.get(label);
          labelToResource.put(label, Resources.subtract(originalRes, res));
        }
      }
      
      // update queue to resource
      for (Entry<String, Set<String>> originEntry : originalNodeToLabels
          .entrySet()) {
        String node = originEntry.getKey();
        Set<String> originLabels = originEntry.getValue();
        Set<String> nowLabels = nodeToLabels.get(node);
        
        for (String q : queueToResource.keySet()) {
          Resource queueResource = queueToResource.get(q);
          boolean pastUsable = isNodeUsableByQueue(originLabels, q);
          boolean nowUsable = isNodeUsableByQueue(nowLabels, q);
          
          if (updateType == UpdateLabelResourceType.UPDATE_LABEL) {
            if (pastUsable && !nowUsable) {
              Resources.subtractFrom(queueResource, getResourceOfNode(node));
            } else if (!pastUsable && nowUsable) {
              Resources.addTo(queueResource, getResourceOfNode(node));
            }
          } else if (updateType == UpdateLabelResourceType.ACTIVE) {
            if (nowUsable) {
              Resources.addTo(queueResource, getResourceOfNode(node));
            }
          } else if (updateType == UpdateLabelResourceType.DEACTIVE) {
            if (nowUsable) {
              Resources.subtractFrom(queueResource, getResourceOfNode(node));
            }
          }
        }
      }
    } finally {
      writeLock.unlock();
    }
  }
  
  private boolean isNodeUsableByQueue(Set<String> nodeLabels, String queueName) {
    // node without any labels can be accessed by any queue
    if (nodeLabels == null || nodeLabels.isEmpty()
        || (nodeLabels.size() == 1 && nodeLabels.contains(NO_LABEL))) {
      return true;
    }
    
    for (String label : nodeLabels) {
      if (queueToLabels.containsKey(queueName)
          && queueToLabels.get(queueName).contains(label)) {
        return true;
      }
    }
    
    return false;
  }

  private void removeAll(Map<String, Set<String>> map, String key,
      Set<String> set) {
    if (set == null) {
      return;
    }
    if (!map.containsKey(key)) {
      return;
    }
    map.get(key).remove(set);
  }

  private void remove(Map<String, Set<String>> map, String key, String value) {
    if (value == null) {
      return;
    }
    if (!map.containsKey(key)) {
      return;
    }
    map.get(key).remove(value);
    if (map.get(key).isEmpty()) {
      map.remove(key);
    }
  }

  private void add(Map<String, Set<String>> map, String key, String value) {
    if (value == null) {
      return;
    }
    if (!map.containsKey(key)) {
      map.put(key, new HashSet<String>());
    }
    map.get(key).add(value);
  }

  /**
   * Remove multiple labels labels from repository
   * 
   * @param labelsToRemove
   * @throws IOException
   */
  @SuppressWarnings("unchecked")
  public void removeLabels(Collection<String> labelsToRemove)
      throws IOException {
    if (null == labelsToRemove || labelsToRemove.isEmpty()) {
      return;
    }
    
    try {
      writeLock.lock();

      Map<String, Set<String>> labelToActiveNodeAdded =
          new HashMap<String, Set<String>>();
      Map<String, Set<String>> labelToActiveNodeRemoved =
          new HashMap<String, Set<String>>();
      Map<String, Set<String>> originalNodeToLabels =
          new HashMap<String, Set<String>>();

      for (String label : labelsToRemove) {
        label = normalizeLabel(label);
        if (label == null || label.isEmpty() || !existingLabels.contains(label)) {
          throw new IOException("Label to be removed is null or empty");
        }

        // remove it from label
        this.existingLabels.remove(label);

        // remove it from labelToActiveNodes
        Set<String> activeNodes = labelToActiveNodes.remove(label);
        removeAll(labelToActiveNodeRemoved, label, activeNodes);

        // update node -> labels
        Set<String> nodes = labelToNodes.remove(label);

        // update node to labels
        if (nodes != null) {
          for (String node : nodes) {
            if (!originalNodeToLabels.containsKey(node)
                && nodeToLabels.containsKey(node)) {
              Set<String> originalLabels =
                  Sets.newHashSet(nodeToLabels.get(node));
              originalNodeToLabels.put(node, originalLabels);
            }
            remove(nodeToLabels, node, label);
            // if we don't have any labels in a node now, we will mark this node
            // as no label
            if (runningNodes.contains(node)
                && (nodeToLabels.get(node) == null || nodeToLabels.get(node)
                    .isEmpty())) {
              add(labelToActiveNodeAdded, NO_LABEL, node);
            }
          }
        }
      }

      // update resource
      updateLabelResource(labelToActiveNodeAdded, labelToActiveNodeRemoved,
          originalNodeToLabels, UpdateLabelResourceType.UPDATE_LABEL);

      // create event to remove labels
      if (null != dispatcher) {
        dispatcher.getEventHandler().handle(
            new RemoveLabelsEvent(labelsToRemove));
      }

      LOG.info("Remove labels: ["
          + StringUtils.join(labelsToRemove.iterator(), ",") + "]");
    } finally {
      writeLock.unlock();
    }
  }

  private void verifyNodeLabel(String node, String label) throws IOException {
    if (node == null || node.isEmpty()) {
      throw new IOException(
          "Trying to change label on a node, but node is null or empty");
    }
    if (label != null && !label.isEmpty() && !existingLabels.contains(label)) {
      throw new IOException("Label doesn't exist in repository, "
          + "have you added it before? label=" + label);
    }
  }

  private Set<String> emptyWhenNull(Set<String> s) {
    if (s == null) {
      return new HashSet<String>();
    }
    return s;
  }

  /**
   * Set node -> label, if label is null or empty, it means remove label on node
   * 
   * @param newNodeToLabels node -> label map
   */
  @SuppressWarnings("unchecked")
  public void
      setLabelsOnMultipleNodes(Map<String, Set<String>> newNodeToLabels)
          throws IOException {
    if (null == newNodeToLabels || newNodeToLabels.isEmpty()) {
      return;
    }
    
    try {
      writeLock.lock();

      Map<String, Set<String>> labelToActiveNodeAdded =
          new HashMap<String, Set<String>>();
      Map<String, Set<String>> labelToActiveNodeRemoved =
          new HashMap<String, Set<String>>();
      Map<String, Set<String>> originalNodeToLabels =
          new HashMap<String, Set<String>>();

      for (Entry<String, Set<String>> e : newNodeToLabels.entrySet()) {
        String node = e.getKey();
        Set<String> labels = e.getValue();

        // normalize and verify
        labels = normalizeLabels(labels);
        for (String label : labels) {
          verifyNodeLabel(node, label);
        }

        // handling labels removed
        Set<String> originalLabels = emptyWhenNull(nodeToLabels.get(node));
        Set<String> difference = Sets.difference(originalLabels, labels);
        for (String removedLabel : difference) {
          remove(labelToNodes, removedLabel, node);
          if (runningNodes.contains(node)) {
            add(labelToActiveNodeRemoved, removedLabel, node);
          }
        }

        // Mark this node as "no-label" if we set a empty set of label
        if (labels.isEmpty() && !originalLabels.isEmpty()
            && runningNodes.contains(node)) {
          add(labelToActiveNodeAdded, NO_LABEL, node);
        }

        // handling labels added
        for (String addedLabel : Sets.difference(labels, originalLabels)) {
          add(labelToNodes, addedLabel, node);
          if (runningNodes.contains(node)) {
            add(labelToActiveNodeAdded, addedLabel, node);
          }
        }

        // Mark this node not "no-label" if we set a non-empty set of label
        if (!labels.isEmpty() && originalLabels.isEmpty()
            && runningNodes.contains(node)) {
          add(labelToActiveNodeRemoved, NO_LABEL, node);
        }
      }
      
      // save original node to labels
      for (String node : newNodeToLabels.keySet()) {
        if (!originalNodeToLabels.containsKey(node)
            && nodeToLabels.containsKey(node)) {
          Set<String> originalLabels = Sets.newHashSet(nodeToLabels.get(node));
          originalNodeToLabels.put(node, originalLabels);
        }
      }
      
      // update node to labels and label to nodes
      nodeToLabels.putAll(newNodeToLabels);

      updateLabelResource(labelToActiveNodeAdded, labelToActiveNodeRemoved,
          originalNodeToLabels, UpdateLabelResourceType.UPDATE_LABEL);

      if (null != dispatcher) {
        dispatcher.getEventHandler().handle(
            new StoreNodeToLabelsEvent(newNodeToLabels));
      }

      // shows node->labels we added
      LOG.info("setLabelsOnMultipleNodes:");
      for (Entry<String, Set<String>> entry : newNodeToLabels.entrySet()) {
        LOG.info("  host=" + entry.getKey() + ", labels=["
            + StringUtils.join(entry.getValue().iterator(), ",") + "]");
      }
    } finally {
      writeLock.unlock();
    }
  }

  public void setLabelsOnSingleNode(String node, Set<String> labels)
      throws IOException {
    setLabelsOnMultipleNodes(ImmutableMap.of(node, labels));
  }

  private Resource getResourceOfNode(String node) {
    Resource res = Resource.newInstance(0, 0);
    if (nodeToResource.containsKey(node)) {
      for (Resource r : nodeToResource.get(node).values()) {
        Resources.addTo(res, r);
      }
    }
    return res;
  }

  /**
   * Set label on node, if label is null or empty, it means remove label on node
   * 
   * @param node
   * @param labels
   */
  public void removeLabelsOnNodes(String node, Set<String> labels)
      throws IOException {
    setLabelsOnMultipleNodes(ImmutableMap.of(node, labels));
  }

  public Resource getResourceWithNoLabel() throws IOException {
    return getResourceWithLabel(NO_LABEL);
  }

  public Resource getResourceWithLabel(String label) {
    label = normalizeLabel(label);
    try {
      readLock.lock();
      Resource res = labelToResource.get(label);
      return res == null ? Resources.none() : res;
    } finally {
      readLock.unlock();
    }
  }

  /*
   * Following methods are used for setting if a node is up and running, which
   * will be used by this#getActiveNodesByLabel and getLabelResource
   */
  public void activatedNode(NodeId node, Resource resource) {
    try {
      writeLock.lock();
      String nodeName = node.getHost();

      // put this node to nodeToResource
      if (!nodeToResource.containsKey(nodeName)) {
        nodeToResource.put(nodeName, new ConcurrentHashMap<NodeId, Resource>());
      }

      if (null != nodeToResource.get(nodeName).put(node, resource)) {
        String msg =
            "This shouldn't happen, trying to active node,"
                + " but there's already a node here, "
                + "please check what happened. NodeId=" + node.toString();
        LOG.warn(msg);
        return;
      }

      // add add it to running node
      runningNodes.add(nodeName);

      // update resources
      Set<String> labels = nodeToLabels.get(nodeName);
      labels =
          (labels == null || labels.isEmpty()) ? ImmutableSet.of(NO_LABEL)
              : labels;

      if (nodeToResource.get(nodeName).size() <= 1) {
        Map<String, Set<String>> labelToActiveNodeAdded =
            new HashMap<String, Set<String>>();
        for (String label : labels) {
          labelToActiveNodeAdded.put(label, ImmutableSet.of(nodeName));
        }
        Map<String, Set<String>> originalNodeTolabels =
            new HashMap<String, Set<String>>();
        if (nodeToLabels.containsKey(nodeName)) {
          originalNodeTolabels.put(nodeName, nodeToLabels.get(nodeName));
        } else {
          originalNodeTolabels.put(nodeName, NodeLabelManager.EMPTY_STRING_SET);
        }
        updateLabelResource(labelToActiveNodeAdded, null, originalNodeTolabels,
            UpdateLabelResourceType.ACTIVE); 
      } else {
        // Support more than two NMs in a same node
        addNMInNodeAlreadyHasNM(labels, resource);
      }
    } finally {
      writeLock.unlock();
    }
  }

  public void deactivateNode(NodeId node) {
    try {
      writeLock.lock();
      String nodeName = node.getHost();
      Resource res = null;

      // add add it to running node
      runningNodes.add(nodeName);

      // update resources
      Set<String> labels = nodeToLabels.get(nodeName);
      labels =
          labels == null || labels.isEmpty() ? ImmutableSet.of(NO_LABEL)
              : labels;

      // this is last NM in this node
      if (nodeToResource.get(nodeName).size() == 1) {
        Map<String, Set<String>> labelToActiveNodeRemoved =
            new HashMap<String, Set<String>>();
        for (String label : labels) {
          labelToActiveNodeRemoved.put(label, ImmutableSet.of(nodeName));
          labelToActiveNodes.get(label).remove(nodeName);
        }
        Map<String, Set<String>> originalNodeTolabels =
            new HashMap<String, Set<String>>();
        if (nodeToLabels.containsKey(nodeName)) {
          originalNodeTolabels.put(nodeName, nodeToLabels.get(nodeName));
        } else {
          originalNodeTolabels.put(nodeName, NodeLabelManager.EMPTY_STRING_SET);
        }
        updateLabelResource(null, labelToActiveNodeRemoved,
            originalNodeTolabels, UpdateLabelResourceType.DEACTIVE);
      }

      // update node to resource
      if (null == (res = nodeToResource.get(nodeName).remove(node))) {
        String msg =
            "Trying to deactive node,"
                + " but there's doesn't exist a node here."
                + " It may caused by re-registering a unhealthy node"
                + " (make it become healthy). "
                + "Please check what happened. NodeId=" + node.toString();
        LOG.warn(msg);
      }

      // if there's more NM remains
      if (nodeToResource.get(nodeName).size() > 0) {
        // Support more than two NMs in a same node
        removeNMToNodeAlreadyHasNM(labels, res);
      }
    } finally {
      writeLock.unlock();
    }
  }

  public void updateNodeResource(NodeId node, Resource newResource) {
    deactivateNode(node);
    activatedNode(node, newResource);
  }

  /**
   * Remove labels on given nodes
   * 
   * @param nodes
   *          to remove labels
   */
  public void removeLabelsOnNodes(Collection<String> nodes) throws IOException {
    Map<String, Set<String>> map =
        new HashMap<String, Set<String>>(nodes.size());
    for (String node : nodes) {
      map.put(node, EMPTY_STRING_SET);
    }
    setLabelsOnMultipleNodes(map);
  }

  /**
   * Remove label on given node
   * 
   * @param node
   *          to remove label
   */
  public void removeLabelOnNode(String node) throws IOException {
    removeLabelsOnNodes(Arrays.asList(node));
  }
  
  /**
   * Clear all labels and related mapping from NodeLabelManager
   * @throws IOException
   */
  public void clearAllLabels() throws IOException {
    try {
      writeLock.lock();
      Set<String> dupLabels = Sets.newHashSet(getLabels());
      removeLabels(dupLabels);
    } finally {
      writeLock.unlock();
    }
  }

  /**
   * Get nodes by given label
   * 
   * @param label
   * @return nodes has assigned give label label
   */
  public Collection<String> getActiveNodesByLabel(String label)
      throws IOException {
    label = normalizeLabel(label);
    try {
      readLock.lock();
      return Collections.unmodifiableCollection(labelToActiveNodes.get(label));
    } finally {
      readLock.unlock();
    }
  }

  /**
   * Get number of nodes by given label
   * 
   * @param label
   * @return Get number of nodes by given label
   */
  public int getNumOfNodesByLabel(String label) throws IOException {
    label = normalizeLabel(label);
    try {
      readLock.lock();
      Collection<String> nodes = labelToActiveNodes.get(label);
      return nodes == null ? 0 : nodes.size();
    } finally {
      readLock.unlock();
    }
  }

  /**
   * Get mapping of nodes to labels
   * 
   * @return nodes to labels map
   */
  public Map<String, Set<String>> getNodesToLabels() {
    try {
      readLock.lock();
      return Collections.unmodifiableMap(nodeToLabels);
    } finally {
      readLock.unlock();
    }
  }

  public Set<String> getLabelsOnNode(String node) {
    Set<String> label = nodeToLabels.get(node);
    return label == null ? EMPTY_STRING_SET : Collections
        .unmodifiableSet(label);
  }

  /**
   * Get existing valid labels in repository
   * 
   * @return existing valid labels in repository
   */
  public Set<String> getLabels() throws IOException {
    try {
      readLock.lock();
      return Collections.unmodifiableSet(existingLabels);
    } finally {
      readLock.unlock();
    }
  }

  public boolean containsLabel(String label) {
    try {
      readLock.lock();
      return label != null
          && (label.isEmpty() || existingLabels.contains(label));
    } finally {
      readLock.unlock();
    }
  }

  public void reinitializeQueueLabels(Map<String, Set<String>> queueToLabels) {
    try {
      writeLock.lock();
      // clear before set
      this.queueToLabels.clear();
      queueToResource.clear();

      for (Entry<String, Set<String>> entry : queueToLabels.entrySet()) {
        String queue = entry.getKey();
        Set<String> labels = entry.getValue();
        labels = labels.isEmpty() ? ImmutableSet.of(NO_LABEL) : labels;
        if (labels.contains(ANY)) {
          continue;
        }
        
        this.queueToLabels.put(queue, labels);
        
        // empty label node can be accessed by any queue
        Set<String> dupLabels = new HashSet<String>(labels);
        dupLabels.add("");
        Set<String> accessedNodes = new HashSet<String>();
        Resource totalResource = Resource.newInstance(0, 0);
        for (String label : dupLabels) {
          if (labelToActiveNodes.containsKey(label)) {
            for (String node : labelToActiveNodes.get(label)) {
              if (!accessedNodes.contains(node)) {
                accessedNodes.add(node);
                Resources.addTo(totalResource, getResourceOfNode(node));
              }
            }
          }
        }
        queueToResource.put(queue, totalResource);
      }
    } finally {
      writeLock.unlock();
    }
  }

  public Resource getQueueResource(String queueName, Set<String> queueLabels,
      Resource clusterResource) {
    if (queueLabels.contains(ANY)) {
      return clusterResource;
    }
    Resource res = queueToResource.get(queueName);
    return res == null ? clusterResource : res;
  }

  // Dispatcher related code
  protected void handleStoreEvent(NodeLabelManagerEvent event) {
    try {
      this.stateMachine.doTransition(event.getType(), event);
    } catch (InvalidStateTransitonException e) {
      LOG.error("Can't handle this event at current state", e);
    }
  }

  private final class ForwardingEventHandler implements
      EventHandler<NodeLabelManagerEvent> {

    @Override
    public void handle(NodeLabelManagerEvent event) {
      if (isInState(STATE.STARTED)) {
        handleStoreEvent(event);
      }
    }
  }
}
