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

package org.apache.hadoop.yarn.server.api.protocolrecords;

import java.util.Set;

import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Evolving;
import org.apache.hadoop.yarn.util.Records;

@Public
@Evolving
public abstract class RemoveLabelsRequest {
  public static RemoveLabelsRequest newInstance(Set<String> partitions) {
    RemoveLabelsRequest request =
        Records.newRecord(RemoveLabelsRequest.class);
    request.setLabels(partitions);
    return request;
  }

  @Public
  @Evolving
  public abstract void setLabels(Set<String> partitions);

  @Public
  @Evolving
  public abstract Set<String> getLabels();
}

