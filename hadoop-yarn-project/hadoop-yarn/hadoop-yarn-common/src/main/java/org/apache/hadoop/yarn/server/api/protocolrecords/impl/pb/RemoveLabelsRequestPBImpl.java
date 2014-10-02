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

import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.yarn.proto.YarnServerResourceManagerServiceProtos.RemoveLabelsRequestProto;
import org.apache.hadoop.yarn.proto.YarnServerResourceManagerServiceProtos.RemoveLabelsRequestProtoOrBuilder;
import org.apache.hadoop.yarn.server.api.protocolrecords.RemoveLabelsRequest;

public class RemoveLabelsRequestPBImpl extends RemoveLabelsRequest {
  Set<String> labels;
  RemoveLabelsRequestProto proto = RemoveLabelsRequestProto
      .getDefaultInstance();
  RemoveLabelsRequestProto.Builder builder = null;
  boolean viaProto = false;
  
  public RemoveLabelsRequestPBImpl() {
    this.builder = RemoveLabelsRequestProto.newBuilder();
  }
  
  public RemoveLabelsRequestPBImpl(RemoveLabelsRequestProto proto) {
    this.proto = proto;
    viaProto = true;
  }
  
  private void maybeInitBuilder() {
    if (viaProto || builder == null) {
      builder = RemoveLabelsRequestProto.newBuilder(proto);
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
  
  public RemoveLabelsRequestProto getProto() {
    mergeLocalToProto();
    proto = viaProto ? proto : builder.build();
    viaProto = true;
    return proto;
  }
  
  private void initLabels() {
    if (this.labels != null) {
      return;
    }
    RemoveLabelsRequestProtoOrBuilder p = viaProto ? proto : builder;
    this.labels = new HashSet<String>();
    this.labels.addAll(p.getLabelsList());
  }

  @Override
  public void setLabels(Set<String> partitions) {
    maybeInitBuilder();
    if (partitions == null || partitions.isEmpty()) {
      builder.clearLabels();
    }
    this.labels = partitions;
  }

  @Override
  public Set<String> getLabels() {
    initLabels();
    return this.labels;
  }

}
