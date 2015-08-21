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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.SortedSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.Time;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.api.records.timeline.TimelineDomain;
import org.apache.hadoop.yarn.api.records.timeline.TimelineDomains;
import org.apache.hadoop.yarn.api.records.timeline.TimelineEntities;
import org.apache.hadoop.yarn.api.records.timeline.TimelineEntity;
import org.apache.hadoop.yarn.api.records.timeline.TimelineEvents;
import org.apache.hadoop.yarn.api.records.timeline.TimelinePutResponse;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.exceptions.ApplicationNotFoundException;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.timeline.security.TimelineACLsManager;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.JsonParser;
import org.codehaus.jackson.map.MappingIterator;
import org.codehaus.jackson.map.MappingJsonFactory;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.xc.JaxbAnnotationIntrospector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

public class EntityFileTimelineStore extends AbstractService implements TimelineStore {
  // TODO: Move to YarnConfiguration
  public static final String TIMELINE_SERVICE_ENTITYFILE_PREFIX =
      "yarn.timeline-service.entity-file-store.";
  public static final String TIMELINE_SERVICE_ENTITYFILE_SUMMARY_STORE =
      TIMELINE_SERVICE_ENTITYFILE_PREFIX + "summary-store";
  public static final String
      TIMELINE_SERVICE_ENTITYFILE_SCAN_INTERVAL_SECONDS =
      TIMELINE_SERVICE_ENTITYFILE_PREFIX + "scan-interval-seconds";
  public static final long
      TIMELINE_SERVICE_ENTITYFILE_SCAN_INTERVAL_SECONDS_DEFAULT = 5 * 60;
  public static final String TIMELINE_SERVICE_ENTITYFILE_THREADS =
      TIMELINE_SERVICE_ENTITYFILE_PREFIX + "threads";
  public static final int TIMELINE_SERVICE_ENTITYFILE_THREADS_DEFAULT = 16;
  public static final String TIMELINE_SERVICE_ENTITYFILE_SUMMARY_ENTITY_TYPES =
      TIMELINE_SERVICE_ENTITYFILE_PREFIX + "summary-entity-types";
  public static final String TIMELINE_SERVICE_ENTITYFILE_APP_CACHE_SIZE =
      TIMELINE_SERVICE_ENTITYFILE_PREFIX + "app-cache-size";
  public static final int
      TIMELINE_SERVICE_ENTITYFILE_APP_CACHE_SIZE_DEFAULT = 5;
  public static final String
      TIMELINE_SERVICE_ENTITYFILE_CLEANER_INTERVAL_SECONDS =
      TIMELINE_SERVICE_ENTITYFILE_PREFIX + "cleaner-interval-seconds";
  public static final int
      TIMELINE_SERVICE_ENTITYFILE_CLEANER_INTERVAL_SECONDS_DEFAULT = 60 * 60;
  public static final String TIMELINE_SERVICE_ENTITYFILE_RETAIN_SECONDS =
      TIMELINE_SERVICE_ENTITYFILE_PREFIX + "retain-seconds";
  public static final int TIMELINE_SERVICE_ENTITYFILE_RETAIN_SECONDS_DEFAULT =
      7 * 24 * 60 * 60;
  // how old the most recent log of an UNKNOWN app needs to be in the active
  // directory before we treat it as COMPLETED
  public static final String
      TIMELINE_SERVICE_ENTITYFILE_UNKNOWN_ACTIVE_SECONDS =
      TIMELINE_SERVICE_ENTITYFILE_PREFIX + "unknown-active-seconds";
  public static final int
      TIMELINE_SERVICE_ENTITYFILE_UNKNOWN_ACTIVE_SECONDS_DEFAULT = 24 * 60 * 60;
  public static final String TIMELINE_SERVICE_ENTITYFILE_ACTIVE_DIR =
      TIMELINE_SERVICE_ENTITYFILE_PREFIX + "active-dir";
  public static final String TIMELINE_SERVICE_ENTITYFILE_ACTIVE_DIR_DEFAULT =
      "/tmp/entity-file-history/active";
  public static final String TIMELINE_SERVICE_ENTITYFILE_DONE_DIR =
      TIMELINE_SERVICE_ENTITYFILE_PREFIX + "done-dir";
  public static final String TIMELINE_SERVICE_ENTITYFILE_DONE_DIR_DEFAULT =
      "/tmp/entity-file-history/done";

  private static final Logger LOG = LoggerFactory.getLogger(
      EntityFileTimelineStore.class);
  private static final String DOMAIN_LOG_PREFIX = "domainlog-";
  private static final String SUMMARY_LOG_PREFIX = "summarylog-";
  private static final String ENTITY_LOG_PREFIX = "entitylog-";
  private static final FsPermission ACTIVE_DIR_PERMISSION =
      new FsPermission((short)01777);
  private static final FsPermission DONE_DIR_PERMISSION =
      new FsPermission((short)0700);

  private static final EnumSet<YarnApplicationState>
    APP_FINAL_STATES = EnumSet.of(
        YarnApplicationState.FAILED,
        YarnApplicationState.KILLED,
        YarnApplicationState.FINISHED);
  private static final String APP_DONE_DIR_FORMAT =
      "%d" + Path.SEPARATOR     // cluster timestamp
      + "%04d" + Path.SEPARATOR // app num / 10000000
      + "%03d" + Path.SEPARATOR // (app num / 1000) % 1000
      + "%s";                   // full app ID

  private static final String APP_ID_PATTERN = "[^0-9]_([0-9]{13,}_[0-9]+)";

  private YarnClient yarnClient;
  private TimelineStore summaryStore;
  private TimelineACLsManager aclManager;
  private TimelineDataManager summaryTdm;
  private ConcurrentMap<ApplicationId, AppLogs> appLogMap =
      new ConcurrentHashMap<ApplicationId, AppLogs>();
  private Map<ApplicationId, AppLogs> cachedApps;
  private ScheduledThreadPoolExecutor executor;
  private FileSystem fs;
  private ObjectMapper objMapper;
  private JsonFactory jsonFactory;
  private Path activeRootPath;
  private Path doneRootPath;
  private long logRetainMillis;
  private long unknownActiveMillis;
  private int appCacheMaxSize;
  private Pattern appIdPattern = Pattern.compile(APP_ID_PATTERN);
  private Set<String> summaryEntityTypes;

  public EntityFileTimelineStore() {
    super(EntityFileTimelineStore.class.getSimpleName());
  }

  @SuppressWarnings("serial")
  @Override
  protected void serviceInit(Configuration conf) throws Exception {
    yarnClient = YarnClient.createYarnClient();
    yarnClient.init(conf);
    summaryStore = createSummaryStore();
    summaryStore.init(conf);
    long logRetainSecs = conf.getLong(
        TIMELINE_SERVICE_ENTITYFILE_RETAIN_SECONDS,
        TIMELINE_SERVICE_ENTITYFILE_RETAIN_SECONDS_DEFAULT);
    logRetainMillis = logRetainSecs * 1000;
    LOG.info("Cleaner set to delete logs older than {} seconds", logRetainSecs);
    long unknownActiveSecs = conf.getLong(
        TIMELINE_SERVICE_ENTITYFILE_UNKNOWN_ACTIVE_SECONDS,
        TIMELINE_SERVICE_ENTITYFILE_UNKNOWN_ACTIVE_SECONDS_DEFAULT);
    unknownActiveMillis = unknownActiveSecs * 1000;
    LOG.info("Unknown apps will be treated as complete after {} seconds",
        unknownActiveSecs);
    Collection<String> filterStrings = conf.getStringCollection(
        TIMELINE_SERVICE_ENTITYFILE_SUMMARY_ENTITY_TYPES);
    if (filterStrings.isEmpty()) {
      throw new IllegalArgumentException(
          TIMELINE_SERVICE_ENTITYFILE_SUMMARY_ENTITY_TYPES + " is not set");
    }
    LOG.info("Entity types for summary store: {}", filterStrings);
    summaryEntityTypes = new HashSet<String>(filterStrings);
    appCacheMaxSize = conf.getInt(TIMELINE_SERVICE_ENTITYFILE_APP_CACHE_SIZE,
        TIMELINE_SERVICE_ENTITYFILE_APP_CACHE_SIZE_DEFAULT);
    LOG.info("Application cache size is {}", appCacheMaxSize);
    cachedApps = Collections.synchronizedMap(
        new LinkedHashMap<ApplicationId, AppLogs>(appCacheMaxSize + 1,
            0.75f, true) {
              @Override
              protected boolean removeEldestEntry(
                  Entry<ApplicationId, AppLogs> eldest) {
                if (super.size() > appCacheMaxSize) {
                  ApplicationId appId = eldest.getKey();
                  AppLogs appLogs = eldest.getValue();
                  appLogs.releaseCache();
                  if (appLogs.isDone()) {
                    appLogMap.remove(appId);
                  }
                  return true;
                }
                return false;
              }
        });
    super.serviceInit(conf);
  }

  protected TimelineStore createSummaryStore() {
    return ReflectionUtils.newInstance(getConfig().getClass(
        TIMELINE_SERVICE_ENTITYFILE_SUMMARY_STORE, LeveldbTimelineStore.class,
        TimelineStore.class), getConfig());
  }

  @Override
  protected void serviceStart() throws Exception {
    LOG.info("Starting {}",getName());
    yarnClient.start();
    summaryStore.start();

    Configuration conf = getConfig();
    aclManager = new TimelineACLsManager(conf);
    aclManager.setTimelineStore(summaryStore);
    summaryTdm = new TimelineDataManager(summaryStore, aclManager);
    summaryTdm.init(conf);
    summaryTdm.start();
    activeRootPath = new Path(conf.get(TIMELINE_SERVICE_ENTITYFILE_ACTIVE_DIR,
        TIMELINE_SERVICE_ENTITYFILE_ACTIVE_DIR_DEFAULT));
    doneRootPath = new Path(conf.get(TIMELINE_SERVICE_ENTITYFILE_DONE_DIR,
        TIMELINE_SERVICE_ENTITYFILE_DONE_DIR_DEFAULT));
    fs = activeRootPath.getFileSystem(conf);
    fs.mkdirs(activeRootPath);
    fs.setPermission(activeRootPath, ACTIVE_DIR_PERMISSION);
    fs.mkdirs(doneRootPath);
    fs.setPermission(doneRootPath, DONE_DIR_PERMISSION);

    objMapper = new ObjectMapper();
    objMapper.setAnnotationIntrospector(new JaxbAnnotationIntrospector());
    jsonFactory = new MappingJsonFactory(objMapper);
    final long scanIntervalSecs = conf.getLong(
        TIMELINE_SERVICE_ENTITYFILE_SCAN_INTERVAL_SECONDS,
        TIMELINE_SERVICE_ENTITYFILE_SCAN_INTERVAL_SECONDS_DEFAULT);
    final long cleanerIntervalSecs = conf.getLong(
        TIMELINE_SERVICE_ENTITYFILE_CLEANER_INTERVAL_SECONDS,
        TIMELINE_SERVICE_ENTITYFILE_CLEANER_INTERVAL_SECONDS_DEFAULT);
    final int numThreads = conf.getInt(TIMELINE_SERVICE_ENTITYFILE_THREADS,
        TIMELINE_SERVICE_ENTITYFILE_THREADS_DEFAULT);
    LOG.info("Scanning active directory every {} seconds", scanIntervalSecs);
    LOG.info("Cleaning logs every {} seconds", cleanerIntervalSecs);

    executor = new ScheduledThreadPoolExecutor(numThreads,
        new ThreadFactoryBuilder().setNameFormat("EntityLogPluginWorker #%d")
            .build());
    executor.scheduleAtFixedRate(new EntityLogScanner(), 0, scanIntervalSecs,
        TimeUnit.SECONDS);
    executor.scheduleAtFixedRate(new EntityLogCleaner(), cleanerIntervalSecs,
        cleanerIntervalSecs, TimeUnit.SECONDS);
    super.serviceStart();
  }

  @Override
  protected void serviceStop() throws Exception {
    LOG.info("Stopping {}", getName());
    if (executor != null) {
      executor.shutdown();
      if (executor.isTerminating()) {
        LOG.info("Waiting for executor to terminate");
        boolean terminated = executor.awaitTermination(10, TimeUnit.SECONDS);
        if (terminated) {
          LOG.info("Executor terminated");
        } else {
          LOG.warn("Executor did not terminate");
          executor.shutdownNow();
        }
      }
    }
    if (summaryTdm != null) {
      summaryTdm.stop();
    }
    if (summaryStore != null) {
      summaryStore.stop();
    }
    if (yarnClient != null) {
      yarnClient.stop();
    }
    super.serviceStop();
  }

  // converts the String to an ApplicationId or null if conversion failed
  private ApplicationId toAppId(String appIdStr) {
    ApplicationId appId = null;
    if (appIdStr.startsWith(ApplicationId.appIdStrPrefix)) {
      try {
        appId = ConverterUtils.toApplicationId(appIdStr);
      } catch (IllegalArgumentException e) {
        appId = null;
      }
    }
    return appId;
  }

  private void scanActiveLogs() throws IOException {
    RemoteIterator<FileStatus> iter = fs.listStatusIterator(activeRootPath);
    while (iter.hasNext()) {
      FileStatus stat = iter.next();
      ApplicationId appId = toAppId(stat.getPath().getName());
      if (appId != null) {
        AppLogs logs = getActiveApp(appId, stat.getPath());
        executor.execute(new ActiveLogParser(logs));
      }
    }
  }

  private AppLogs getActiveApp(ApplicationId appId, Path dirPath) {
    AppLogs appLogs = appLogMap.get(appId);
    if (appLogs == null) {
      appLogs = new AppLogs(appId, dirPath, AppState.ACTIVE);
      AppLogs oldAppLogs = appLogMap.putIfAbsent(appId, appLogs);
      if (oldAppLogs != null) {
        appLogs = oldAppLogs;
      }
    }
    return appLogs;
  }

  // searches for the app logs and returns it if found else null
  private AppLogs findAppLogs(ApplicationId appId) throws IOException {
    AppLogs appLogs = appLogMap.get(appId);
    if (appLogs == null) {
      AppState appState = AppState.UNKNOWN;
      Path dirPath = getDonePath(appId);
      if (fs.exists(dirPath)) {
        appState = AppState.COMPLETED;
      } else {
        dirPath = new Path(activeRootPath, appId.toString());
        if (fs.exists(dirPath)) {
          appState = AppState.ACTIVE;
        }
      }
      if (appState != AppState.UNKNOWN) {
        appLogs = new AppLogs(appId, dirPath, appState);
        AppLogs oldAppLogs = appLogMap.putIfAbsent(appId, appLogs);
        if (oldAppLogs != null) {
          appLogs = oldAppLogs;
        }
      }
    }
    return appLogs;
  }

  private void cleanLogs(Path dirpath) throws IOException {
    long now = Time.now();

    // check if this directory is an app dir
    ApplicationId appId = toAppId(dirpath.getName());
    boolean shouldClean = (appId != null);
    RemoteIterator<FileStatus> iter = fs.listStatusIterator(dirpath);
    while (iter.hasNext()) {
      FileStatus stat = iter.next();
      if (shouldClean) {
        if (now - stat.getModificationTime() <= logRetainMillis) {
          // found a dir entry that is fresh enough to prevent
          // cleaning this directory.
          LOG.debug("{} not being cleaned due to {}", dirpath, stat.getPath());
          shouldClean = false;
          break;
        }
      } else if (stat.isDirectory()) {
        // recurse into subdirectories if they aren't app directories
        cleanLogs(stat.getPath());
      }
    }

    if (shouldClean) {
      try {
        LOG.info("Deleting {}", dirpath);
        if (!fs.delete(dirpath, true)) {
          LOG.error("Unable to remove " + dirpath);
        }
      } catch (IOException e) {
        LOG.error("Unable to remove " + dirpath, e);
      }
    }
  }

  private Path getDonePath(ApplicationId appId) {
    // cut up the app ID into mod(1000) buckets
    int appNum = appId.getId();
    appNum /= 1000;
    int bucket2 = appNum % 1000;
    int bucket1 = appNum / 1000;
    return new Path(doneRootPath,
        String.format(APP_DONE_DIR_FORMAT, appId.getClusterTimestamp(),
            bucket1, bucket2, appId.toString()));
  }

  private AppState getAppState(ApplicationId appId) throws IOException {
    AppState appState = AppState.ACTIVE;
    try {
      ApplicationReport report = yarnClient.getApplicationReport(appId);
      YarnApplicationState yarnState = report.getYarnApplicationState();
      if (APP_FINAL_STATES.contains(yarnState)) {
        appState = AppState.COMPLETED;
      }
    } catch (ApplicationNotFoundException e) {
      appState = AppState.UNKNOWN;
    } catch (YarnException e) {
      throw new IOException(e);
    }
    return appState;
  }

  private enum AppState {
    ACTIVE,
    UNKNOWN,
    COMPLETED
  }

  private class AppLogs {
    private ApplicationId appId;
    private Path dirPath;
    private AppState appState;
    private List<LogInfo> summaryLogs = new ArrayList<LogInfo>();
    private List<LogInfo> detailLogs = new ArrayList<LogInfo>();
    private TimelineStore cacheStore = null;
    private long cacheRefreshTime = 0;
    private boolean cacheCompleted = false;

    public AppLogs(ApplicationId applicationId, Path path, AppState state) {
      appId = applicationId;
      dirPath = path;
      appState = state;
    }

    public synchronized boolean isDone() {
      return appState == AppState.COMPLETED;
    }

    public synchronized ApplicationId getAppId() {
      return appId;
    }

    public synchronized void parseSummaryLogs() throws IOException {
      if (!isDone()) {
        appState = getAppState(appId);
        long recentLogModTime = scanForLogs();
        if (appState == AppState.UNKNOWN) {
          if (Time.now() - recentLogModTime > unknownActiveMillis) {
            LOG.info(
                "{} state is UNKNOWN and logs are stale, assuming COMPLETED",
                appId);
            appState = AppState.COMPLETED;
          }
        }
      }
      for (LogInfo log : summaryLogs) {
        log.parseForSummary(dirPath, isDone());
      }
    }

    // scans for new logs and returns the modification timestamp of the
    // most recently modified log
    private long scanForLogs() throws IOException {
      long newestModTime = 0;
      RemoteIterator<FileStatus> iter = fs.listStatusIterator(dirPath);
      while (iter.hasNext()) {
        FileStatus stat = iter.next();
        newestModTime = Math.max(stat.getModificationTime(), newestModTime);
        if (!stat.isFile()) {
          continue;
        }
        String filename = stat.getPath().getName();
        if (filename.startsWith(DOMAIN_LOG_PREFIX)) {
          addSummaryLog(filename, stat.getOwner(), true);
        } else if (filename.startsWith(SUMMARY_LOG_PREFIX)) {
          addSummaryLog(filename, stat.getOwner(), false);
        } else if (filename.startsWith(ENTITY_LOG_PREFIX)) {
          addDetailLog(filename, stat.getOwner());
        }
      }

      // if there are no logs in the directory then use the modification
      // time of the directory itself
      if (newestModTime == 0) {
        newestModTime = fs.getFileStatus(dirPath).getModificationTime();
      }

      return newestModTime;
    }

    private void addSummaryLog(String filename, String owner,
        boolean isDomainLog) {
      for (LogInfo log : summaryLogs) {
        if (log.filename.equals(filename)) {
          return;
        }
      }
      LogInfo log = null;
      if (isDomainLog) {
        log = new DomainLogInfo(filename, owner);
      } else {
        log = new EntityLogInfo(filename, owner);
      }
      summaryLogs.add(log);
    }

    private void addDetailLog(String filename, String owner) {
      for (LogInfo log : detailLogs) {
        if (log.filename.equals(filename)) {
          return;
        }
      }
      detailLogs.add(new EntityLogInfo(filename, owner));
    }

    public synchronized TimelineStore refreshCache()
        throws IOException {
      //TODO: make a config for cache freshness
      if (!cacheCompleted && Time.monotonicNow() - cacheRefreshTime > 10000) {
        if (!isDone()) {
          parseSummaryLogs();
        } else if (detailLogs.isEmpty()) {
          scanForLogs();
        }
        if (!detailLogs.isEmpty()) {
          if (cacheStore == null) {
            cacheStore = new MemoryTimelineStore();
            cacheStore.init(getConfig());
            cacheStore.start();
          }
          TimelineDataManager tdm = new TimelineDataManager(cacheStore,
              aclManager);
          tdm.init(getConfig());
          tdm.start();
          for (LogInfo log : detailLogs) {
            log.parseForCache(tdm, dirPath, isDone());
          }
          tdm.close();
        }
        cacheRefreshTime = Time.monotonicNow();
        cacheCompleted = isDone();
      }

      return cacheStore;
    }

    public synchronized void releaseCache() {
      try {
        if (cacheStore != null) {
          cacheStore.close();
        }
      } catch (IOException e) {
        LOG.warn("Error closing datamanager", e);
      }
      cacheStore = null;
      cacheRefreshTime = 0;
      cacheCompleted = false;
      // reset offsets so next time logs are re-parsed
      for (LogInfo log : detailLogs) {
        log.offset = 0;
      }
    }

    public synchronized void moveToDone() throws IOException {
      Path donePath = getDonePath(appId);
      if (!donePath.equals(dirPath)) {
        Path donePathParent = donePath.getParent();
        if (!fs.exists(donePathParent)) {
          fs.mkdirs(donePathParent);
        }
        if (!fs.rename(dirPath, donePath)) {
          throw new IOException("Rename " + dirPath + " to " + donePath
              + " failed");
        } else {
          LOG.info("Moved {} to {}", dirPath, donePath);
        }
        dirPath = donePath;
      }
    }
  }

  private abstract class LogInfo {
    protected String filename;
    protected String user;
    protected long offset = 0;

    public LogInfo(String file, String owner) {
      filename = file;
      user = owner;
    }

    public void parseForSummary(Path dirPath, boolean appCompleted)
        throws IOException {
      Path logPath = new Path(dirPath, filename);
      String dirname = dirPath.getName();
      long startTime = Time.monotonicNow();
      LOG.debug("Parsing {}/{} at offset {}", dirname, filename, offset);
      long count = parsePath(summaryTdm, logPath, appCompleted);
      LOG.info("Parsed {} entities from {}/{} in {} msec",
          count, dirname, filename, Time.monotonicNow() - startTime);
    }

    public void parseForCache(TimelineDataManager tdm, Path dirPath,
        boolean appCompleted) throws IOException {
      Path logPath = new Path(dirPath, filename);
      String dirname = dirPath.getName();
      long startTime = Time.monotonicNow();
      LOG.debug("Parsing {}/{} at offset {}", dirname, filename, offset);
      long count = parsePath(tdm, logPath, appCompleted);
      LOG.info("Parsed {} entities from {}/{} in {} msec",
            count, dirname, filename, Time.monotonicNow() - startTime);
    }

    private long parsePath(TimelineDataManager tdm, Path logPath,
        boolean appCompleted) throws IOException {
      UserGroupInformation ugi =
          UserGroupInformation.createRemoteUser(user);
      FSDataInputStream in = fs.open(logPath);
      JsonParser parser = null;
      try {
        in.seek(offset);
        try {
          parser = jsonFactory.createJsonParser(in);
          parser.configure(JsonParser.Feature.AUTO_CLOSE_SOURCE, false);
        } catch (IOException e) {
          // if app hasn't completed then there may be errors due to the
          // incomplete file which are treated as EOF until app completes
          if (appCompleted) {
            throw e;
          }
        }

        return doParse(tdm, parser, ugi, appCompleted);
      } finally {
        IOUtils.closeStream(parser);
        IOUtils.closeStream(in);
      }
    }

    protected abstract long doParse(TimelineDataManager tdm, JsonParser parser,
        UserGroupInformation ugi, boolean appCompleted) throws IOException;
  }

  private class EntityLogInfo extends LogInfo {
    public EntityLogInfo(String file, String owner) {
      super(file, owner);
    }

    @Override
    protected long doParse(TimelineDataManager tdm, JsonParser parser,
        UserGroupInformation ugi, boolean appCompleted) throws IOException {
      long count = 0;
      TimelineEntities entities = new TimelineEntities();
      ArrayList<TimelineEntity> entityList = new ArrayList<TimelineEntity>(1);
      long bytesParsed = 0;
      long bytesParsedLastBatch = 0;
      boolean postError = false;
      try {
        //TODO: Wrapper class around MappingIterator could help clean
        //      up some of the messy exception handling below
        MappingIterator<TimelineEntity> iter = objMapper.readValues(parser,
            TimelineEntity.class);

        while (iter.hasNext()) {
          TimelineEntity entity = iter.next();
          String etype = entity.getEntityType();
          LOG.trace("Read entity {}", etype);
          ++count;
          bytesParsed = parser.getCurrentLocation().getCharOffset() + 1;
          LOG.trace("Parser now at offset {}", bytesParsed);

          try {
            LOG.debug("Adding {} to cache store", etype);
            entityList.add(entity);
            entities.setEntities(entityList);
            tdm.postEntities(entities, ugi);
            offset += bytesParsed - bytesParsedLastBatch;
            bytesParsedLastBatch = bytesParsed;
            entityList.clear();
          } catch (YarnException e) {
            postError = true;
            throw new IOException("Error posting entities", e);
          } catch (IOException e) {
            postError = true;
            throw new IOException("Error posting entities", e);
          }
        }
      } catch (IOException e) {
        // if app hasn't completed then there may be errors due to the
        // incomplete file which are treated as EOF until app completes
        if (appCompleted || postError) {
          throw e;
        }
      } catch (RuntimeException e) {
        if (appCompleted || !(e.getCause() instanceof JsonParseException)) {
          throw e;
        }
      }
      return count;
    }
  }

  private class DomainLogInfo extends LogInfo {
    public DomainLogInfo(String file, String owner) {
      super(file, owner);
    }

    protected long doParse(TimelineDataManager tdm, JsonParser parser,
        UserGroupInformation ugi, boolean appCompleted) throws IOException {
      long count = 0;
      long bytesParsed = 0;
      long bytesParsedLastBatch = 0;
      boolean putError = false;
      try {
        //TODO: Wrapper class around MappingIterator could help clean
        //      up some of the messy exception handling below
        MappingIterator<TimelineDomain> iter = objMapper.readValues(parser,
            TimelineDomain.class);

        while (iter.hasNext()) {
          TimelineDomain domain = iter.next();
          domain.setOwner(ugi.getShortUserName());
          LOG.trace("Read domain {}", domain.getId());
          ++count;
          bytesParsed = parser.getCurrentLocation().getCharOffset() + 1;
          LOG.trace("Parser now at offset {}", bytesParsed);

          try {
            tdm.putDomain(domain, ugi);
            offset += bytesParsed - bytesParsedLastBatch;
            bytesParsedLastBatch = bytesParsed;
          } catch (YarnException e) {
            putError = true;
            throw new IOException("Error posting domain", e);
          } catch (IOException e) {
            putError = true;
            throw new IOException("Error posting domain", e);
          }
        }
      } catch (IOException e) {
        // if app hasn't completed then there may be errors due to the
        // incomplete file which are treated as EOF until app completes
        if (appCompleted || putError) {
          throw e;
        }
      } catch (RuntimeException e) {
        if (appCompleted || !(e.getCause() instanceof JsonParseException)) {
          throw e;
        }
      }
      return count;
    }
  }

  private class EntityLogScanner implements Runnable {
    @Override
    public void run() {
      LOG.debug("Active scan starting");
      try {
        scanActiveLogs();
      } catch (Exception e) {
        LOG.error("Error scanning active files", e);
      }
      LOG.debug("Active scan complete");
    }
  }

  private class ActiveLogParser implements Runnable {
    private AppLogs appLogs;

    public ActiveLogParser(AppLogs logs) {
      appLogs = logs;
    }

    @Override
    public void run() {
      try {
        appLogs.parseSummaryLogs();
        if (appLogs.isDone()) {
          appLogs.moveToDone();
          appLogMap.remove(appLogs.getAppId());
        }
      } catch (Exception e) {
        LOG.error("Error processing logs for " + appLogs.getAppId(), e);
      }
    }
  }

  private class EntityLogCleaner implements Runnable {
    @Override
    public void run() {
      LOG.debug("Cleaner starting");
      try {
        cleanLogs(doneRootPath);
      } catch (Exception e) {
        LOG.error("Error cleaning files", e);
      }
      LOG.debug("Cleaner finished");
    }
  }

  private TimelineStore getTimelineStoreForRead(String entityType, Object arg)
      throws IOException {
    TimelineStore store = null;
    if (arg != null && !summaryEntityTypes.contains(entityType)) {
      if (arg instanceof CharSequence) {
        Matcher m = appIdPattern.matcher((CharSequence) arg);
        if (m.find()) {
          ApplicationId appId = null;
          try {
            appId = ConverterUtils.toApplicationId(ApplicationId.appIdStrPrefix
                + m.group(1));
          } catch (IllegalArgumentException e) {
            LOG.warn("Unable to determine app ID from " + arg);
            appId = null;
          }
          if (appId != null) {
            store = getCachedStore(appId);
            if (store != null) {
              LOG.debug("Using cache store from {} for {}", appId, entityType);
            } else {
              LOG.info("Failed to load cached store for {}", appId);
            }
          }
        }
      }
    }
    if (store == null) {
      LOG.debug("Using summary store for {}", entityType);
      store = this.summaryStore;
    }
    return store;
  }

  // find a cached timeline store or null if it cannot be located
  private TimelineStore getCachedStore(ApplicationId appId) throws IOException {
    AppLogs appLogs = null;
    synchronized (cachedApps) {
      appLogs = cachedApps.get(appId);
      if (appLogs == null) {
        appLogs = findAppLogs(appId);
        if (appLogs != null) {
          cachedApps.put(appId, appLogs);
        }
      }
    }
    TimelineStore store = null;
    if (appLogs != null) {
      store = appLogs.refreshCache();
    }
    return store;
  }

  @Override
  public TimelineEntities getEntities(String entityType, Long limit,
      Long windowStart, Long windowEnd, String fromId, Long fromTs,
      NameValuePair primaryFilter, Collection<NameValuePair> secondaryFilters,
      EnumSet<Field> fieldsToRetrieve) throws IOException {
    LOG.debug("getEntities type={} primary={}", entityType, primaryFilter);
    Object arg = (primaryFilter == null) ? null : primaryFilter.getValue();
    TimelineStore store = getTimelineStoreForRead(entityType, arg);
    return store.getEntities(entityType, limit, windowStart, windowEnd,
        fromId, fromTs, primaryFilter, secondaryFilters, fieldsToRetrieve);
  }

  @Override
  public TimelineEntity getEntity(String entityId, String entityType,
      EnumSet<Field> fieldsToRetrieve) throws IOException {
    LOG.debug("getEntity type={} id={}", entityType, entityId);
    TimelineStore store = getTimelineStoreForRead(entityType, entityId);
    return store.getEntity(entityId, entityType, fieldsToRetrieve);
  }

  @Override
  public TimelineEvents getEntityTimelines(String entityType,
      SortedSet<String> entityIds, Long limit, Long windowStart,
      Long windowEnd, Set<String> eventTypes) throws IOException {
    // TODO: This should coalesce lookups from multiple cache stores
    LOG.debug("getEntityTimelines type={} ids={}", entityType, entityIds);
    return summaryStore.getEntityTimelines(entityType, entityIds, limit,
        windowStart, windowEnd, eventTypes);
  }

  @Override
  public TimelineDomain getDomain(String domainId) throws IOException {
    return summaryStore.getDomain(domainId);
  }

  @Override
  public TimelineDomains getDomains(String owner) throws IOException {
    return summaryStore.getDomains(owner);
  }

  @Override
  public TimelinePutResponse put(TimelineEntities data) throws IOException {
    return summaryStore.put(data);
  }

  @Override
  public void put(TimelineDomain domain) throws IOException {
    summaryStore.put(domain);
  }
}
