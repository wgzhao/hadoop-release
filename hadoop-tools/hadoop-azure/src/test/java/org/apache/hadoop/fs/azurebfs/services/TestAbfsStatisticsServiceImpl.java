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

package org.apache.hadoop.fs.azurebfs.services;

import org.junit.Test;
import org.mockito.Mockito;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.azurebfs.AzureBlobFileSystem;

import static org.junit.Assert.assertEquals;

public class TestAbfsStatisticsServiceImpl {
  final AbfsStatisticsServiceImpl abfsStatisticsService;
  final AzureBlobFileSystem azureBlobFileSystem;
  final FileSystem.Statistics statistics;

  public TestAbfsStatisticsServiceImpl() throws Exception {
    this.abfsStatisticsService = new AbfsStatisticsServiceImpl();
    this.azureBlobFileSystem = Mockito.mock(AzureBlobFileSystem.class);
    this.statistics = new FileSystem.Statistics("test");
    this.abfsStatisticsService.subscribe(azureBlobFileSystem, this.statistics);
  }

  @Test
  public void testEnsureSubscription() throws Exception {
    assertEquals(1, abfsStatisticsService.getSubscribers().size());
    assertEquals(azureBlobFileSystem, abfsStatisticsService.getSubscribers().keys().nextElement());

    abfsStatisticsService.unsubscribe(azureBlobFileSystem);
    assertEquals(0, abfsStatisticsService.getSubscribers().size());
  }

  @Test
  public void testEnsureIncrementReadOps() throws Exception {
    abfsStatisticsService.incrementReadOps(azureBlobFileSystem, 100);
    assertEquals(100, statistics.getReadOps());
  }

  @Test
  public void testEnsureIncrementBytesRead() throws Exception {
    abfsStatisticsService.incrementBytesRead(azureBlobFileSystem, 200);
    assertEquals(200, statistics.getBytesRead());
  }

  @Test
  public void testEnsureIncrementWriteOps() throws Exception {
    abfsStatisticsService.incrementWriteOps(azureBlobFileSystem, 300);
    assertEquals(300, statistics.getWriteOps());
  }

  @Test
  public void testEnsureIncrementBytesWritten() throws Exception {
    abfsStatisticsService.incrementBytesWritten(azureBlobFileSystem, 400);
    assertEquals(400, statistics.getBytesWritten());
  }
}
