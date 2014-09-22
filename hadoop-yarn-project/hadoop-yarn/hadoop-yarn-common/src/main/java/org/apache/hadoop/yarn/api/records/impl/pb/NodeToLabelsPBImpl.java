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

package org.apache.hadoop.yarn.api.records.impl.pb;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.yarn.api.records.NodeToLabels;
import org.apache.hadoop.yarn.proto.YarnServerResourceManagerServiceProtos.NodeToLabelsProto;
import org.apache.hadoop.yarn.proto.YarnServerResourceManagerServiceProtos.NodeToLabelsProtoOrBuilder;

public class NodeToLabelsPBImpl extends NodeToLabels {
  List<String> labels;
  NodeToLabelsProto proto = NodeToLabelsProto
      .getDefaultInstance();
  NodeToLabelsProto.Builder builder = null;
  boolean viaProto = false;
  
  public NodeToLabelsPBImpl() {
    this.builder = NodeToLabelsProto.newBuilder();
  }
  
  public NodeToLabelsPBImpl(NodeToLabelsProto proto) {
    this.proto = proto;
    viaProto = true;
  }
  
  private void maybeInitBuilder() {
    if (viaProto || builder == null) {
      builder = NodeToLabelsProto.newBuilder(proto);
    }
    viaProto = false;
  }
  
  private void mergeLocalToBuilder() {
    if (this.labels != null && !this.labels.isEmpty()) {
      builder.addAllLabels(this.labels);
    }
  }
  
  private void mergeLocalToProto() {
    if (viaProto)
      maybeInitBuilder();
    mergeLocalToBuilder();
    proto = builder.build();
    viaProto = true;
  }
  
  public NodeToLabelsProto getProto() {
    mergeLocalToProto();
    proto = viaProto ? proto : builder.build();
    viaProto = true;
    return proto;
  }
  
  private void initLabels() {
    if (this.labels != null) {
      return;
    }
    NodeToLabelsProtoOrBuilder p = viaProto ? proto : builder;
    this.labels = new ArrayList<String>();
    this.labels.addAll(p.getLabelsList());
  }

  @Override
  public void setLabels(List<String> labels) {
    maybeInitBuilder();
    if (labels == null || labels.isEmpty()) {
      builder.clearLabels();
    }
    this.labels = labels;
  }

  @Override
  public List<String> getLabels() {
    initLabels();
    return this.labels;
  }

  @Override
  public void setNode(String node) {
    maybeInitBuilder();
    if (node == null) {
      builder.clearNode();
      return;
    }
    builder.setNode(node);
  }

  @Override
  public String getNode() {
    NodeToLabelsProtoOrBuilder p = viaProto ? proto : builder;
    if (!p.hasNode()) {
      return null;
    }
    return (p.getNode());
  }
}
