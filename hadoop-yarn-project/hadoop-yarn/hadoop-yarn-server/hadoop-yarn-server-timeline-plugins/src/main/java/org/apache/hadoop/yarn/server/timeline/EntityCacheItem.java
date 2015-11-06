/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.hadoop.yarn.server.timeline;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Time;
import org.apache.hadoop.yarn.api.records.timeline.TimelineEntityGroupId;
import org.apache.hadoop.yarn.server.timeline.security.TimelineACLsManager;
import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class EntityCacheItem {
  private static final Logger LOG
      = LoggerFactory.getLogger(EntityCacheItem.class);

  private TimelineStore store;
  private EntityGroupFSTimelineStore.AppLogs appLogs;
  private long lastRefresh;
  private Configuration config;
  private FileSystem fs;

  public EntityCacheItem(Configuration config, FileSystem fs) {
    this.config = config;
    this.fs = fs;
  }

  public synchronized EntityGroupFSTimelineStore.AppLogs getAppLogs() {
    return this.appLogs;
  }

  public synchronized void setAppLogs(
      EntityGroupFSTimelineStore.AppLogs appLogs) {
    this.appLogs = appLogs;
  }

  public boolean needRefresh() {
    //TODO: make a config for cache freshness
    return (Time.monotonicNow() - lastRefresh > 10000);
  }

  public void updateRefreshTimeToNow() {
    this.lastRefresh = Time.monotonicNow();
  }

  public synchronized TimelineStore refreshCache(TimelineEntityGroupId groupId,
      TimelineACLsManager aclManager, JsonFactory jsonFactory,
      ObjectMapper objMapper) throws IOException {
    if (needRefresh()) {
      if (!appLogs.isDone()) {
        appLogs.parseSummaryLogs();
      } else if (appLogs.getDetailLogs().isEmpty()) {
        appLogs.scanForLogs();
      }
      if (!appLogs.getDetailLogs().isEmpty()) {
        if (store == null) {
          store = new LevelDBCacheTimelineStore(groupId.toString(),
              "LeveldbCache." + groupId);
          //store = new MemoryTimelineStore("MemoryStore." + cacheId);
          store.init(config);
          store.start();
        }
        TimelineDataManager tdm = new TimelineDataManager(store,
            aclManager);
        tdm.init(config);
        tdm.start();
        if (appLogs.getDetailLogs().isEmpty()) {
          LOG.debug("cache id {}'s detail log is empty! ", groupId);
        }
        List<LogInfo> removeList = new ArrayList<LogInfo>();
        for (LogInfo log : appLogs.getDetailLogs()) {
          LOG.debug("Try refresh logs for {}", log.getFilename());
          // Only refresh the log that matches the cache id
          if (log.getFilename().contains(groupId.toString())) {
            Path appDirPath = appLogs.getAppDirPath();
            if (fs.exists(log.getPath(appDirPath))) {
              LOG.debug("Refresh logs for cache id {}", groupId);
              log.parseForStore(tdm, appDirPath, appLogs.isDone(), jsonFactory,
                  objMapper, fs);
            } else {
              // The log may have been removed, remove the log
              removeList.add(log);
              LOG.info("File {} no longer exists, remove it from log list",
                  log.getPath(appDirPath));
            }
            appLogs.getDetailLogs().removeAll(removeList);
          }
        }
        tdm.close();
      }
      updateRefreshTimeToNow();
    } else {
      LOG.debug("Cache new enough, skip refreshing");
    }

    return store;
  }

  public synchronized void releaseCache(TimelineEntityGroupId groupId) {
    try {
      if (store != null) {
        store.close();
      }
    } catch (IOException e) {
      LOG.warn("Error closing datamanager", e);
    }
    store = null;
    // reset offsets so next time logs are re-parsed
    for (LogInfo log : appLogs.getDetailLogs()) {
      if (log.getFilename().contains(groupId.toString())) {
        log.offset = 0;
      }
    }
  }
}
