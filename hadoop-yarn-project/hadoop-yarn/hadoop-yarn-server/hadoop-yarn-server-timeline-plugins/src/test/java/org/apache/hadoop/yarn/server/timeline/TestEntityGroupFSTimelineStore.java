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
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.timeline.TimelineEntities;
import org.apache.hadoop.yarn.api.records.timeline.TimelineEntity;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

import java.io.IOException;
import java.util.EnumSet;
import java.util.List;

import static org.apache.hadoop.yarn.server.timeline.EntityGroupFSTimelineStore.AppState;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class TestEntityGroupFSTimelineStore extends TimelineStoreTestUtils {

  private static final String SAMPLE_APP_NAME = "1234_5678";

  static final ApplicationId TEST_APPLICATION_ID
      = ConverterUtils.toApplicationId(
      ConverterUtils.APPLICATION_PREFIX + "_" + SAMPLE_APP_NAME);

  private static final String TEST_ATTEMPT_DIR_NAME
      = ApplicationAttemptId.appAttemptIdStrPrefix + SAMPLE_APP_NAME + "_1";
  private static final String TEST_SUMMARY_LOG_FILE_NAME
      = EntityGroupFSTimelineStore.SUMMARY_LOG_PREFIX + "test";
  private static final String TEST_ENTITY_LOG_FILE_NAME
      = EntityGroupFSTimelineStore.ENTITY_LOG_PREFIX
          + EntityGroupPlugInForTest.getStandardTimelineGroupId();
  private static final String TEST_DOMAIN_LOG_FILE_NAME
      = EntityGroupFSTimelineStore.DOMAIN_LOG_PREFIX + "test";

  private static final Path TEST_ROOT_DIR
      = new Path(System.getProperty("test.build.data",
          System.getProperty("java.io.tmpdir")),
      TestEntityGroupFSTimelineStore.class.getSimpleName());
  private static final Path TEST_APP_DIR_PATH
      = new Path(TEST_ROOT_DIR, TEST_APPLICATION_ID.toString());
  private static final Path TEST_ATTEMPT_DIR_PATH
      = new Path(TEST_APP_DIR_PATH, TEST_ATTEMPT_DIR_NAME);
  private static final Path TEST_DONE_DIR_PATH
      = new Path(TEST_ROOT_DIR, "done");

  private static Configuration config = new YarnConfiguration();
  private static MiniDFSCluster hdfsCluster;
  private static FileSystem fs;
  private EntityGroupFSTimelineStore store;
  private TimelineEntity entityNew;

  @Rule
  public TestName currTestName = new TestName();

  @BeforeClass
  public static void setupClass() throws Exception {
    config.setBoolean(YarnConfiguration.TIMELINE_SERVICE_TTL_ENABLE, false);
    config.set(
        YarnConfiguration
            .TIMELINE_SERVICE_ENTITYGROUP_FS_STORE_SUMMARY_ENTITY_TYPES,
        "YARN_APPLICATION,YARN_APPLICATION_ATTEMPT,YARN_CONTAINER");
    config.set(YarnConfiguration.TIMELINE_SERVICE_ENTITYGROUP_FS_STORE_DONE_DIR,
        TEST_DONE_DIR_PATH.toString());
    config.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, TEST_ROOT_DIR.toString());
    MiniDFSCluster.Builder builder = new MiniDFSCluster.Builder(config);
    hdfsCluster = builder.build();
    fs = hdfsCluster.getFileSystem();
  }

  @Before
  public void setup() throws Exception {
    createTestFiles();
    store = new EntityGroupFSTimelineStore();
    if (currTestName.getMethodName().contains("Plugin")) {
      config.set(YarnConfiguration.TIMELINE_SERVICE_ENTITY_GROUP_PLUGIN_CLASSES,
          EntityGroupPlugInForTest.class.getName());
    }
    store.init(config);
    store.start();
    store.setFs(fs);
  }

  @After
  public void tearDown() throws Exception {
    fs.delete(TEST_APP_DIR_PATH, true);
    store.stop();
  }

  @AfterClass
  public static void tearDownClass() throws Exception {
    hdfsCluster.shutdown();
    FileContext fileContext = FileContext.getLocalFSFileContext();
    fileContext.delete(new Path(
        config.get(YarnConfiguration.TIMELINE_SERVICE_LEVELDB_PATH)), true);
  }

  @Test
  public void testAppLogsScanLogs() throws Exception {
    EntityGroupFSTimelineStore.AppLogs appLogs =
        store.new AppLogs(TEST_APPLICATION_ID, TEST_APP_DIR_PATH,
        AppState.COMPLETED);
    appLogs.scanForLogs();
    List<LogInfo> summaryLogs = appLogs.getSummaryLogs();
    List<LogInfo> detailLogs = appLogs.getDetailLogs();
    assertEquals(2, summaryLogs.size());
    assertEquals(1, detailLogs.size());

    for (LogInfo log : summaryLogs) {
      String fileName = log.getFilename();
      assertTrue(fileName.equals(TEST_SUMMARY_LOG_FILE_NAME)
          || fileName.equals(TEST_DOMAIN_LOG_FILE_NAME));
    }

    for (LogInfo log : detailLogs) {
      String fileName = log.getFilename();
      assertEquals(fileName, TEST_ENTITY_LOG_FILE_NAME);
    }
  }

  @Test
  public void testMoveToDone() throws Exception {
    EntityGroupFSTimelineStore.AppLogs appLogs =
        store.new AppLogs(TEST_APPLICATION_ID, TEST_APP_DIR_PATH,
        AppState.COMPLETED);
    Path pathBefore = appLogs.getAppDirPath();
    appLogs.moveToDone();
    Path pathAfter = appLogs.getAppDirPath();
    assertNotEquals(pathBefore, pathAfter);
    assertTrue(pathAfter.toString().contains(TEST_DONE_DIR_PATH.toString()));
  }

  @Test
  public void testParseSummaryLogs() throws Exception {
    TimelineDataManager tdm = CacheStoreTestUtils.getTdmWithMemStore(config);
    EntityGroupFSTimelineStore.AppLogs appLogs =
        store.new AppLogs(TEST_APPLICATION_ID, TEST_APP_DIR_PATH,
        AppState.COMPLETED);
    appLogs.scanForLogs();
    appLogs.parseSummaryLogs(tdm);
    CacheStoreTestUtils.verifyTestEntities(tdm);
  }

  @Test
  public void testPluginRead() throws Exception {
    // Verify precondition
    assertEquals(EntityGroupPlugInForTest.class.getName(),
        store.getConfig().get(
            YarnConfiguration.TIMELINE_SERVICE_ENTITY_GROUP_PLUGIN_CLASSES));
    // Load data and cache item, prepare timeline store by making a cache item
    EntityGroupFSTimelineStore.AppLogs appLogs =
        store.new AppLogs(TEST_APPLICATION_ID, TEST_APP_DIR_PATH,
        AppState.COMPLETED);
    EntityCacheItem cacheItem = new EntityCacheItem(config, fs);
    cacheItem.setAppLogs(appLogs);
    store.setCachedLogs(EntityGroupPlugInForTest.getStandardTimelineGroupId(), cacheItem);
    // Generate TDM
    TimelineDataManager tdm = CacheStoreTestUtils.getTdmWithStore(config, store);

    // Verify single entity read
    TimelineEntity entity3 = tdm.getEntity("type_3", "id_3",
        EnumSet.allOf(TimelineReader.Field.class),
        UserGroupInformation.getLoginUser());
    assertNotNull(entity3);
    assertEquals(entityNew.getStartTime(), entity3.getStartTime());
    // Verify multiple entities read
    TimelineEntities entities = tdm.getEntities("type_3", null, null, null,
        null, null, null, null, EnumSet.allOf(TimelineReader.Field.class),
        UserGroupInformation.getLoginUser());
    assertEquals(entities.getEntities().size(), 1);
    for (TimelineEntity entity : entities.getEntities()) {
      assertEquals(entityNew.getStartTime(), entity.getStartTime());
    }
  }

  @Test
  public void testSummaryRead() throws Exception {
    // Load data
    EntityGroupFSTimelineStore.AppLogs appLogs =
        store.new AppLogs(TEST_APPLICATION_ID, TEST_APP_DIR_PATH,
        AppState.COMPLETED);
    TimelineDataManager tdm
        = CacheStoreTestUtils.getTdmWithStore(config, store);
    appLogs.scanForLogs();
    appLogs.parseSummaryLogs(tdm);

    // Verify single entity read
    CacheStoreTestUtils.verifyTestEntities(tdm);
    // Verify multiple entities read
    TimelineEntities entities = tdm.getEntities("type_1", null, null, null,
        null, null, null, null, EnumSet.allOf(TimelineReader.Field.class),
        UserGroupInformation.getLoginUser());
    assertEquals(entities.getEntities().size(), 1);
    for (TimelineEntity entity : entities.getEntities()) {
      assertEquals((Long) 123l, entity.getStartTime());
    }

  }

  private void createTestFiles() throws IOException {
    TimelineEntities entities = CacheStoreTestUtils.generateTestEntities();
    CacheStoreTestUtils.writeEntities(entities,
        new Path(TEST_ATTEMPT_DIR_PATH, TEST_SUMMARY_LOG_FILE_NAME), fs);

    entityNew = CacheStoreTestUtils
        .createEntity("id_3", "type_3", 789l, null, null,
            null, null, "domain_id_1");
    TimelineEntities entityList = new TimelineEntities();
    entityList.addEntity(entityNew);
    CacheStoreTestUtils.writeEntities(entityList,
        new Path(TEST_ATTEMPT_DIR_PATH, TEST_ENTITY_LOG_FILE_NAME), fs);

    FSDataOutputStream out = fs.create(
        new Path(TEST_ATTEMPT_DIR_PATH, TEST_DOMAIN_LOG_FILE_NAME));
    out.close();
  }

}
