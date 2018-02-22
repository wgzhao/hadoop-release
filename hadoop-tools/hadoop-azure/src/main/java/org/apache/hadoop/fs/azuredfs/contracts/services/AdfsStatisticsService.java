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

package org.apache.hadoop.fs.azuredfs.contracts.services;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.azuredfs.AzureDistributedFileSystem;

/**
 * AdfsStatistics Service to keep track of file system statistics.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public interface AdfsStatisticsService extends InjectableService {
  /**
   * Subscribes azureDistributedFileSystem to adfs statistics service.
   * @param azureDistributedFileSystem the file system.
   * @param statistics the file system statistics object.
   */
  void subscribe(AzureDistributedFileSystem azureDistributedFileSystem, FileSystem.Statistics statistics);

  /**
   * Unsubscribes azureDistributedFileSystem from adfs statistics service.
   * @param azureDistributedFileSystem the file system.
   */
  void unsubscribe(AzureDistributedFileSystem azureDistributedFileSystem);

  /**
   * Increments total bytes read for a filesystem.
   * @param azureDistributedFileSystem the file system.
   * @param newBytes number of bytes read.
   */
  void incrementBytesRead(AzureDistributedFileSystem azureDistributedFileSystem, long newBytes);

  /**
   * Increments total read operations count.
   * @param azureDistributedFileSystem the file system.
   * @param count the count of read operations.
   */
  void incrementReadOps(AzureDistributedFileSystem azureDistributedFileSystem, int count);

  /**
   * Increments total bytes written for a filesystem.
   * @param azureDistributedFileSystem the file system.
   * @param newBytes number of bytes written.
   */
  void incrementBytesWritten(AzureDistributedFileSystem azureDistributedFileSystem, long newBytes);

  /**
   * Increments total write operations count.
   * @param azureDistributedFileSystem the file system.
   * @param count the count of write operations.
   */
  void incrementWriteOps(AzureDistributedFileSystem azureDistributedFileSystem, int count);
}