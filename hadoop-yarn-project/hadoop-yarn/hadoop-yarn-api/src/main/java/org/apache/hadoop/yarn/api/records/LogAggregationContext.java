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

package org.apache.hadoop.yarn.api.records;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Stable;
import org.apache.hadoop.yarn.proto.YarnProtos.LogAggregationContextProto;
import org.apache.hadoop.yarn.util.Records;

import com.google.common.base.Preconditions;

/**
 * <p><code>LogAggregationContext</code> represents all of the
 * information needed by the <code>NodeManager</code> to handle
 * the logs for an application.</p>
 *
 * <p>It includes details such as:
 *   <ul>
 *     <li>includePattern. It defines the include pattern which is used to
 *     filter the log files. The log files which match the include
 *     pattern will be uploaded.</li>
 *     <li>excludePattern. It defines exclude pattern which is used to
 *     filter the log files. The log files which match the exclude
 *     pattern will not be uploaded. if the log file name matches both the
 *     include and the exclude pattern, the file will be excluded eventually</li>
 *     <li>rollingIntervalSeconds. The default value is 0. By default,
 *     the logAggregationService only uploads container logs when
 *     the application is finished. This configure defines
 *     how often the logAggregationSerivce uploads container logs in seconds
 *     By setting this configure, the logAggregationSerivce can upload container
 *     logs periodically when the application is running.
 *     </li>
 *   </ul>
 * </p>
 *
 * @see ApplicationSubmissionContext
 */

public abstract class LogAggregationContext {

  @Public
  @Stable
  public static LogAggregationContext newInstance(String includePattern,
      String excludePattern, long rollingIntervalSeconds) {
    Preconditions.checkArgument(rollingIntervalSeconds >= 0);
    LogAggregationContext context = Records.newRecord(LogAggregationContext.class);
    context.setIncludePattern(includePattern);
    context.setExcludePattern(excludePattern);
    context.setRollingIntervalSeconds(rollingIntervalSeconds);
    return context;
  }

  /**
   * Get include pattern
   *
   * @return include pattern
   */
  @Public
  @Stable
  public abstract String getIncludePattern();

  /**
   * Set include pattern
   *
   * @param includePattern
   *          to set
   */
  @Public
  @Stable
  public abstract void setIncludePattern(String includePattern);

  /**
   * Get exclude pattern
   *
   * @return exclude pattern
   */
  @Public
  @Stable
  public abstract String getExcludePattern();

  /**
   * Set exclude pattern
   *
   * @param excludePattern
   *          to set
   */
  @Public
  @Stable
  public abstract void setExcludePattern(String excludePattern);

  /**
   * Get rollingIntervalSeconds
   *
   * @return the rollingIntervalSeconds
   */
  @Public
  @Stable
  public abstract long getRollingIntervalSeconds();

  /**
   * Set rollingIntervalSeconds
   *
   * @param rollingIntervalSeconds
   */
  @Public
  @Stable
  public abstract void setRollingIntervalSeconds(long rollingIntervalSeconds);

  @Private
  public abstract LogAggregationContextProto getProto();
}
