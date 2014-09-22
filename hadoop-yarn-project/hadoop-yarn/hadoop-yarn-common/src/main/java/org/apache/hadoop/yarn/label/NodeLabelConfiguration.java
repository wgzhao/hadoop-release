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

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

public class NodeLabelConfiguration extends Configuration {
  public final static String PREFIX = "yarn.node-label.";

  public final static String LABELS_KEY = PREFIX + "labels";
  public final static String NODES_KEY = PREFIX + "nodes";

  public final static String NODE_LABELS_SUFFIX = ".labels";

  public static enum LoadStrategy {
    INITIAL, REPLACE, MERGE, CLEAR
  }

  public NodeLabelConfiguration(String absolutePath) {
    super(false);
    Path absoluteLocalPath = new Path("file", "", absolutePath);
    addResource(absoluteLocalPath);
  }

  public Set<String> getLabels() {
    Set<String> labelsSet = new HashSet<String>();
    String[] labels = getStrings(LABELS_KEY);
    if (null != labels) {
      for (String l : labels) {
        if (l.trim().isEmpty()) {
          continue;
        }
        labelsSet.add(l);
      }
    }
    return labelsSet;
  }

  public Map<String, Set<String>> getNodeToLabels() {
    Map<String, Set<String>> nodeToLabels = new HashMap<String, Set<String>>();

    String[] nodes = getStrings(NODES_KEY);
    if (null != nodes) {
      for (String n : nodes) {
        if (n.trim().isEmpty()) {
          continue;
        }
        String[] labels = getStrings(NODES_KEY + "." + n + NODE_LABELS_SUFFIX);
        nodeToLabels.put(n, new HashSet<String>());
        
        if (labels != null) {
          for (String l : labels) {
            if (l.trim().isEmpty()) {
              continue;
            }
            nodeToLabels.get(n).add(l);
          }
        }
      }
    }

    return nodeToLabels;
  }
}
