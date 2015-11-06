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
 
 package org.apache.hadoop.yarn.api.records.timeline.impl.pb;
 
 import org.apache.hadoop.classification.InterfaceAudience.Private;
 import org.apache.hadoop.classification.InterfaceStability.Unstable;
 import org.apache.hadoop.yarn.api.records.ApplicationId;
 import org.apache.hadoop.yarn.api.records.impl.pb.ApplicationIdPBImpl;
 import org.apache.hadoop.yarn.api.records.timeline.TimelineEntityGroupId;
 import org.apache.hadoop.yarn.proto.YarnProtos.ApplicationIdProto;
 import org.apache.hadoop.yarn.proto.YarnProtos.TimelineEntityGroupIdProto;
 
 import com.google.common.base.Preconditions;
 
 @Private
 @Unstable
 public class TimelineEntityGroupIdPBImpl extends TimelineEntityGroupId{
   TimelineEntityGroupIdProto proto = null;
   TimelineEntityGroupIdProto.Builder builder = null;
   private ApplicationId applicationId = null;
 
   public TimelineEntityGroupIdPBImpl() {
     builder = TimelineEntityGroupIdProto.newBuilder();
   }
 
   public TimelineEntityGroupIdPBImpl(TimelineEntityGroupIdProto proto) {
     this.proto = proto;
     this.applicationId = convertFromProtoFormat(proto.getAppId());
   }
 
   public TimelineEntityGroupIdProto getProto() {
     return proto;
   }
 
   @Override
   public String getTimelineEntityGroupId() {
     Preconditions.checkNotNull(proto);
     return proto.getId();
   }
 
   @Override
   protected void setTimelineEntityGroupId(String id) {
     Preconditions.checkNotNull(builder);
     builder.setId((id));
   }
 
   @Override
   public ApplicationId getApplicationId() {
     return this.applicationId;
   }
 
   @Override
   protected void setApplicationId(ApplicationId appId) {
     if (appId != null) {
       Preconditions.checkNotNull(builder);
       builder.setAppId(convertToProtoFormat(appId));
     }
     this.applicationId = appId;
   }
 
   private ApplicationIdPBImpl convertFromProtoFormat(
       ApplicationIdProto p) {
     return new ApplicationIdPBImpl(p);
   }
 
   private ApplicationIdProto convertToProtoFormat(
       ApplicationId t) {
     return ((ApplicationIdPBImpl)t).getProto();
   }
 
   @Override
   protected void build() {
     proto = builder.build();
     builder = null;
   }
 }