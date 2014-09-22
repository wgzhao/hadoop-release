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

package org.apache.hadoop.yarn.server.api.protocolrecords.impl.pb;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.hadoop.yarn.proto.YarnServerResourceManagerServiceProtos.GetNodeToLabelsResponseProto;
import org.apache.hadoop.yarn.proto.YarnServerResourceManagerServiceProtos.GetNodeToLabelsResponseProtoOrBuilder;
import org.apache.hadoop.yarn.proto.YarnServerResourceManagerServiceProtos.NodeToLabelsProto;
import org.apache.hadoop.yarn.server.api.protocolrecords.GetNodeToLabelsResponse;

import com.google.common.collect.Sets;

public class GetNodeToLabelsResponsePBImpl extends
    GetNodeToLabelsResponse {
  GetNodeToLabelsResponseProto proto = GetNodeToLabelsResponseProto
      .getDefaultInstance();
  GetNodeToLabelsResponseProto.Builder builder = null;
  boolean viaProto = false;

  private Map<String, Set<String>> nodeToLabels;
  
  public GetNodeToLabelsResponsePBImpl() {
    this.builder = GetNodeToLabelsResponseProto.newBuilder();
  }

  public GetNodeToLabelsResponsePBImpl(GetNodeToLabelsResponseProto proto) {
    this.proto = proto;
    this.viaProto = true;
  }

  private void initNodeToLabels() {
    if (this.nodeToLabels != null) {
      return;
    }
    GetNodeToLabelsResponseProtoOrBuilder p = viaProto ? proto : builder;
    List<NodeToLabelsProto> list = p.getNodeToLabelsList();
    this.nodeToLabels = new HashMap<String, Set<String>>();

    for (NodeToLabelsProto c : list) {
      this.nodeToLabels
          .put(c.getNode(), Sets.newHashSet(c.getLabelsList()));
    }
  }

  private void maybeInitBuilder() {
    if (viaProto || builder == null) {
      builder = GetNodeToLabelsResponseProto.newBuilder(proto);
    }
    viaProto = false;
  }

  private void addNodeToLabelsToProto() {
    maybeInitBuilder();
    builder.clearNodeToLabels();
    if (nodeToLabels == null) {
      return;
    }
    Iterable<NodeToLabelsProto> iterable = new Iterable<NodeToLabelsProto>() {
      @Override
      public Iterator<NodeToLabelsProto> iterator() {
        return new Iterator<NodeToLabelsProto>() {

          Iterator<Entry<String, Set<String>>> iter = nodeToLabels.entrySet()
              .iterator();

          @Override
          public void remove() {
            throw new UnsupportedOperationException();
          }

          @Override
          public NodeToLabelsProto next() {
            Entry<String, Set<String>> now = iter.next();
            return NodeToLabelsProto.newBuilder().setNode(now.getKey())
                .addAllLabels(now.getValue()).build();
          }

          @Override
          public boolean hasNext() {
            return iter.hasNext();
          }
        };
      }
    };
    builder.addAllNodeToLabels(iterable);
  }

  private void mergeLocalToBuilder() {
    if (this.nodeToLabels != null) {
      addNodeToLabelsToProto();
    }
  }

  private void mergeLocalToProto() {
    if (viaProto)
      maybeInitBuilder();
    mergeLocalToBuilder();
    proto = builder.build();
    viaProto = true;
  }

  public GetNodeToLabelsResponseProto getProto() {
    mergeLocalToProto();
    proto = viaProto ? proto : builder.build();
    viaProto = true;
    return proto;
  }

  @Override
  public Map<String, Set<String>> getNodeToLabels() {
    initNodeToLabels();
    return this.nodeToLabels;
  }

  @Override
  public void setNodeToLabels(Map<String, Set<String>> map) {
    initNodeToLabels();
    nodeToLabels.clear();
    nodeToLabels.putAll(map);
  }
}
