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

package org.apache.hadoop.yarn.api.records.timeline;

import java.util.Iterator;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Stable;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.util.Records;

import com.google.common.base.Splitter;


@Public
@Unstable
public abstract class TimelineEntityGroupId implements
    Comparable<TimelineEntityGroupId> {

  private static final Splitter _SPLITTER = Splitter.on('_').trimResults();

  @Public
  @Unstable
  public static final String timelineEntityGroupIdStrPrefix =
      "timelineEntityGroupId";

  @Public
  @Unstable
  public static TimelineEntityGroupId newInstance(ApplicationId applicationId,
      String id) {
    TimelineEntityGroupId timelineEntityGroupId =
        Records.newRecord(TimelineEntityGroupId.class);
    timelineEntityGroupId.setApplicationId(applicationId);
    timelineEntityGroupId.setTimelineEntityGroupId(id);
    timelineEntityGroupId.build();
    return timelineEntityGroupId;
  }

  /**
   * Get the <code>ApplicationId</code> of the
   * <code>TimelineEntityGroupId</code>.
   * 
   * @return <code>ApplicationId</code> of the
   *         <code>TimelineEntityGroupId</code>
   */
  @Public
  @Stable
  public abstract ApplicationId getApplicationId();

  @Private
  @Unstable
  protected abstract void setApplicationId(ApplicationId appID);

  /**
   * Get the <code>TimelineEntityGroupId</code>
   * 
   * @return <code>TimelineEntityGroupId</code>
   */
  @Public
  @Stable
  public abstract String getTimelineEntityGroupId();

  @Private
  @Unstable
  protected abstract void
      setTimelineEntityGroupId(String timelineEntityGroupId);

  @Override
  public int hashCode() {
    int result = getTimelineEntityGroupId().hashCode();
    result = 31 * result + getApplicationId().hashCode();
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (obj == null)
      return false;
    if (getClass() != obj.getClass())
      return false;
    TimelineEntityGroupId other = (TimelineEntityGroupId) obj;
    if (!this.getApplicationId().equals(other.getApplicationId()))
      return false;
    if (!this.getTimelineEntityGroupId().equals(
      other.getTimelineEntityGroupId())) {
      return false;
    }
    ;
    return true;
  }

  @Override
  public int compareTo(TimelineEntityGroupId other) {
    int compareAppIds =
        this.getApplicationId().compareTo(other.getApplicationId());
    if (compareAppIds == 0) {
      return this.getTimelineEntityGroupId().compareTo(
        other.getTimelineEntityGroupId());
    } else {
      return compareAppIds;
    }
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append(timelineEntityGroupIdStrPrefix + "_");
    ApplicationId appId = getApplicationId();
    sb.append(appId.getClusterTimestamp()).append("_");
    sb.append(appId.getId()).append("_");
    sb.append(getTimelineEntityGroupId());
    return sb.toString();
  }

  public static TimelineEntityGroupId
      fromString(String timelineEntityGroupIdStr) {
    Iterator<String> it = _SPLITTER.split(timelineEntityGroupIdStr).iterator();
    if (!it.next().equals(timelineEntityGroupIdStrPrefix)) {
      throw new IllegalArgumentException("Invalid CacheId prefix: "
          + timelineEntityGroupIdStr);
    }
    ApplicationId appId =
        ApplicationId.newInstance(Long.parseLong(it.next()),
          Integer.parseInt(it.next()));
    String id = it.next();
    while (it.hasNext()) {
      id += "_" + it.next();
    }
    return TimelineEntityGroupId.newInstance(appId, id);
  }

  protected abstract void build();

}

