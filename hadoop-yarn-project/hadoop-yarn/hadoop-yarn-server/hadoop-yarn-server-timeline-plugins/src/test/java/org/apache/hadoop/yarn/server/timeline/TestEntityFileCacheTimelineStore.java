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

package org.apache.hadoop.yarn.server.timeline;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;

public class TestEntityFileCacheTimelineStore extends TimelineStoreTestUtils {
  private static final File TEST_DIR = new File(
      System.getProperty("test.build.data",
          System.getProperty("java.io.tmpdir")),
      TestEntityFileCacheTimelineStore.class.getSimpleName());
  private FileContext fsContext;
  private Configuration config = new YarnConfiguration();

  @Before
  public void setup() throws Exception {
    fsContext = FileContext.getLocalFSFileContext();
    fsContext.delete(new Path(TEST_DIR.getAbsolutePath()), true);
    config.set(YarnConfiguration.TIMELINE_SERVICE_LEVELDB_PATH,
        TEST_DIR.getAbsolutePath());
    config.setBoolean(YarnConfiguration.TIMELINE_SERVICE_TTL_ENABLE, false);
    config.set(YarnConfiguration.TIMELINE_SERVICE_ENTITYFILE_CACHE_SUMMARY_ENTITY_TYPES,
        "YARN_APPLICATION,YARN_APPLICATION_ATTEMPT,YARN_CONTAINER");
    config.set(YarnConfiguration.TIMELINE_SERVICE_ENTITYFILE_CACHE_ACTIVE_DIR,
        new File(TEST_DIR, "active").getAbsolutePath().toString());
    config.set(YarnConfiguration.TIMELINE_SERVICE_ENTITYFILE_CACHE_DONE_DIR,
        new File(TEST_DIR, "done").getAbsolutePath().toString());
    store = new EntityFileCacheTimelineStore();
    store.init(config);
    store.start();
    loadTestEntityData();
    loadVerificationEntityData();
    loadTestDomainData();
  }

  @After
  public void tearDown() throws Exception {
    store.stop();
    fsContext.delete(new Path(TEST_DIR.getAbsolutePath()), true);
  }

  @Test
  public void testScanApps() throws Exception {
    // TODO: add verifications
    ((EntityFileCacheTimelineStore) store).scanActiveLogs();
    Thread.sleep(5000);

  }

  public TimelineStore getTimelineStore() {
    return store;
  }

  @Test
  public void testGetSingleEntity() throws IOException {
    super.testGetSingleEntity();
  }

  @Test
  public void testGetEntities() throws IOException {
    super.testGetEntities();
  }

  @Test
  public void testGetEntitiesWithFromId() throws IOException {
    super.testGetEntitiesWithFromId();
  }

  @Test
  public void testGetEntitiesWithFromTs() throws IOException {
    super.testGetEntitiesWithFromTs();
  }

  @Test
  public void testGetEntitiesWithPrimaryFilters() throws IOException {
    super.testGetEntitiesWithPrimaryFilters();
  }

  @Test
  public void testGetEntitiesWithSecondaryFilters() throws IOException {
    super.testGetEntitiesWithSecondaryFilters();
  }

  @Test
  public void testGetEvents() throws IOException {
    super.testGetEvents();
  }

  @Test
  public void testGetDomain() throws IOException {
    super.testGetDomain();
  }

  @Test
  public void testGetDomains() throws IOException {
    super.testGetDomains();
  }
}
