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
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.CacheId;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.api.records.timeline.TimelineDomain;
import org.apache.hadoop.yarn.api.records.timeline.TimelineDomains;
import org.apache.hadoop.yarn.api.records.timeline.TimelineEntities;
import org.apache.hadoop.yarn.api.records.timeline.TimelineEntity;
import org.apache.hadoop.yarn.api.records.timeline.TimelineEvents;
import org.apache.hadoop.yarn.api.records.timeline.TimelinePutResponse;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.ApplicationNotFoundException;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.timeline.TimelineDataManager.CheckAcl;
import org.apache.hadoop.yarn.server.timeline.security.TimelineACLsManager;
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
      + "%s"   + Path.SEPARATOR // full app id
      + "%s"   + Path.SEPARATOR // app attempt id
      + "%s";                   // full cache id

  private static final String APP_ID_PATTERN = "[^0-9]_([0-9]{13,}_[0-9]+)";

  private YarnClient yarnClient;
  private TimelineStore summaryStore;
  private TimelineACLsManager aclManager;
  private TimelineDataManager summaryTdm;
  private ConcurrentMap<CacheId, CachedLogs> cacheIdLogMap =
      new ConcurrentHashMap<>();
  private Map<CacheId, CachedLogs> cachedLogs;
  private ScheduledThreadPoolExecutor executor;
  private FileSystem fs;
  private ObjectMapper objMapper;
  private JsonFactory jsonFactory;
  private Path activeRootPath;
  private Path doneRootPath;
  private long logRetainMillis;
  private long unknownActiveMillis;
  private int appCacheMaxSize;
  private TimelineCacheIdPlugin cacheIdPlugin;

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
        YarnConfiguration.TIMELINE_SERVICE_ENTITYFILE_RETAIN_SECONDS,
        YarnConfiguration.TIMELINE_SERVICE_ENTITYFILE_RETAIN_SECONDS_DEFAULT);
    logRetainMillis = logRetainSecs * 1000;
    LOG.info("Cleaner set to delete logs older than {} seconds", logRetainSecs);
    long unknownActiveSecs = conf.getLong(
        YarnConfiguration.TIMELINE_SERVICE_ENTITYFILE_UNKNOWN_ACTIVE_SECONDS,
        YarnConfiguration
            .TIMELINE_SERVICE_ENTITYFILE_UNKNOWN_ACTIVE_SECONDS_DEFAULT);
    unknownActiveMillis = unknownActiveSecs * 1000;
    LOG.info("Unknown apps will be treated as complete after {} seconds",
        unknownActiveSecs);
    Collection<String> filterStrings = conf.getStringCollection(
        YarnConfiguration.TIMELINE_SERVICE_ENTITYFILE_SUMMARY_ENTITY_TYPES);
    if (filterStrings.isEmpty()) {
      throw new IllegalArgumentException(
          YarnConfiguration.TIMELINE_SERVICE_ENTITYFILE_SUMMARY_ENTITY_TYPES
              + " is not set");
    }
    LOG.info("Entity types for summary store: {}", filterStrings);
    appCacheMaxSize = conf.getInt(
        YarnConfiguration.TIMELINE_SERVICE_ENTITYFILE_APP_CACHE_SIZE,
        YarnConfiguration.TIMELINE_SERVICE_ENTITYFILE_APP_CACHE_SIZE_DEFAULT);
    LOG.info("Application cache size is {}", appCacheMaxSize);
    cachedLogs = Collections.synchronizedMap(
        new LinkedHashMap<CacheId, CachedLogs>(appCacheMaxSize + 1,
            0.75f, true) {
              @Override
              protected boolean removeEldestEntry(
                  Map.Entry<CacheId, CachedLogs> eldest) {
                if (super.size() > appCacheMaxSize) {
                  CacheId cacheId = eldest.getKey();
                  CachedLogs cachedLogs = eldest.getValue();
                  cachedLogs.releaseCache();
                  if (cachedLogs.isDone()) {
                    cacheIdLogMap.remove(cacheId);
                  }
                  return true;
                }
                return false;
              }
        });
    cacheIdPlugin = ReflectionUtils.newInstance(conf.getClass(
        YarnConfiguration.TIMELINE_SERVICE_CACHE_ID_PLUGIN_CLASS,
        EmptyTimelineCacheIdPlugin.class,
        TimelineCacheIdPlugin.class), conf);
    super.serviceInit(conf);
  }

  protected TimelineStore createSummaryStore() {
    return ReflectionUtils.newInstance(getConfig().getClass(
        YarnConfiguration.TIMELINE_SERVICE_ENTITYFILE_SUMMARY_STORE,
        LeveldbTimelineStore.class, TimelineStore.class), getConfig());
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
    activeRootPath = new Path(conf.get(
        YarnConfiguration.TIMELINE_SERVICE_ENTITYFILE_ACTIVE_DIR,
        YarnConfiguration.TIMELINE_SERVICE_ENTITYFILE_ACTIVE_DIR_DEFAULT));
    doneRootPath = new Path(conf.get(
        YarnConfiguration.TIMELINE_SERVICE_ENTITYFILE_DONE_DIR,
        YarnConfiguration.TIMELINE_SERVICE_ENTITYFILE_DONE_DIR_DEFAULT));
    fs = activeRootPath.getFileSystem(conf);
    if (!fs.exists(activeRootPath)) {
      fs.mkdirs(activeRootPath);
      fs.setPermission(activeRootPath, ACTIVE_DIR_PERMISSION);
    }
    if (!fs.exists(doneRootPath)) {
      fs.mkdirs(doneRootPath);
      fs.setPermission(doneRootPath, DONE_DIR_PERMISSION);
    }

    objMapper = new ObjectMapper();
    objMapper.setAnnotationIntrospector(new JaxbAnnotationIntrospector());
    jsonFactory = new MappingJsonFactory(objMapper);
    final long scanIntervalSecs = conf.getLong(
        YarnConfiguration.TIMELINE_SERVICE_ENTITYFILE_SCAN_INTERVAL_SECONDS,
        YarnConfiguration
            .TIMELINE_SERVICE_ENTITYFILE_SCAN_INTERVAL_SECONDS_DEFAULT);
    final long cleanerIntervalSecs = conf.getLong(
        YarnConfiguration.TIMELINE_SERVICE_ENTITYFILE_CLEANER_INTERVAL_SECONDS,
        YarnConfiguration
            .TIMELINE_SERVICE_ENTITYFILE_CLEANER_INTERVAL_SECONDS_DEFAULT);
    final int numThreads = conf.getInt(
        YarnConfiguration.TIMELINE_SERVICE_ENTITYFILE_THREADS,
        YarnConfiguration.TIMELINE_SERVICE_ENTITYFILE_THREADS_DEFAULT);
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

  private void scanActiveLogs() throws IOException {
    RemoteIterator<FileStatus> iterApp = fs.listStatusIterator(activeRootPath);
    // Active logs are in <activeRootPath>/appId/attemptId/cacheid
    // All apps in the root dir
    while (iterApp.hasNext()) {
      FileStatus statApp = iterApp.next();
      if (statApp.isDirectory()) {
        RemoteIterator<FileStatus> iterAttempt
            = fs.listStatusIterator(statApp.getPath());
        // All attempts in the app dir
        while (iterAttempt.hasNext()) {
          FileStatus statAttempt = iterAttempt.next();
          if (statAttempt.isDirectory()) {
            RemoteIterator<FileStatus> iterCache
                = fs.listStatusIterator(statAttempt.getPath());
            // All cache ids in an attempt
            while (iterCache.hasNext()) {
              FileStatus statCache = iterCache.next();
              CacheId cacheId = parseCacheId(statCache.getPath().getName());
              if (cacheId != null) {
                CachedLogs logs = getActiveLog(cacheId, statCache.getPath());
                executor.execute(new ActiveLogParser(logs));
              }
            }
          }
        }
      }
    }
  }

  private CachedLogs getActiveLog(CacheId cacheId, Path dirPath) {
    CachedLogs cachedLogs = cacheIdLogMap.get(cacheId);
    if (cachedLogs == null) {
      cachedLogs = new CachedLogs(cacheId, dirPath, AppState.ACTIVE);
      CachedLogs oldCachedLogs = cacheIdLogMap.putIfAbsent(cacheId, cachedLogs);
      if (oldCachedLogs != null) {
        cachedLogs = oldCachedLogs;
      }
    }
    return cachedLogs;
  }

  // searches for the app logs and returns it if found else null
  private CachedLogs findCacheLogs(CacheId cacheId) throws IOException {
    CachedLogs cachedLogs = cacheIdLogMap.get(cacheId);
    if (cachedLogs == null) {
      AppState appState = AppState.UNKNOWN;
      Path dirPath = getDonePath(cacheId);
      if (fs.exists(dirPath)) {
        appState = AppState.COMPLETED;
      } else {
        dirPath = getActivePath(cacheId);
        if (fs.exists(dirPath)) {
          appState = AppState.ACTIVE;
        }
      }
      if (appState != AppState.UNKNOWN) {
        cachedLogs = new CachedLogs(cacheId, dirPath, appState);
        CachedLogs oldCachedLogs = cacheIdLogMap.putIfAbsent(cacheId, cachedLogs);
        if (oldCachedLogs != null) {
          cachedLogs = oldCachedLogs;
        }
      }
    }
    return cachedLogs;
  }

  private void cleanLogs(Path dirpath) throws IOException {
    long now = Time.now();

    // check if this directory is an app dir
    CacheId cacheId = parseCacheId(dirpath.getName());
    boolean shouldClean = (cacheId != null);
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

  // converts the String to a CacheId or null if conversion failed
  private CacheId parseCacheId(String dirName) {
    CacheId cacheId = null;
    if (dirName.startsWith(CacheId.cacheIdStrPrefix)) {
      try {
        cacheId = CacheId.fromString(dirName);
      } catch (IllegalArgumentException e) {
        cacheId = null;
      }
    }
    return cacheId;
  }

  private Path getActivePath(CacheId cacheId) {
    Path appDirPath = new Path(activeRootPath,
        cacheId.getApplicationAttemptId().getApplicationId().toString());
    Path attemptDirPath = new Path(appDirPath,
        cacheId.getApplicationAttemptId().toString());
    return new Path(attemptDirPath, cacheId.toString());
  }

  private Path getDonePath(CacheId cacheId) {
    ApplicationAttemptId attemptId = cacheId.getApplicationAttemptId();
    ApplicationId applicationId = attemptId.getApplicationId();
    // cut up the cache ID into mod(1000) buckets
    int appNum = applicationId.getId();
    appNum /= 1000;
    int bucket2 = appNum % 1000;
    int bucket1 = appNum / 1000;
    return new Path(doneRootPath,
        String.format(APP_DONE_DIR_FORMAT, applicationId.getClusterTimestamp(),
            bucket1, bucket2, applicationId.toString(), attemptId,
            cacheId.toString()));
  }

  private static AppState getAppState(ApplicationId appId,
      YarnClient yarnClient) throws IOException {
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

  private class CachedLogs {
    private CacheId cacheId;
    private Path dirPath;
    private AppState appState;
    private List<LogInfo> summaryLogs = new ArrayList<LogInfo>();
    private List<LogInfo> detailLogs = new ArrayList<LogInfo>();
    private TimelineStore cacheStore = null;
    private long cacheRefreshTime = 0;
    private boolean cacheCompleted = false;

    public CachedLogs(CacheId cacheId, Path path, AppState state) {
      this.cacheId = cacheId;
      dirPath = path;
      appState = state;
    }

    public synchronized boolean isDone() {
      return appState == AppState.COMPLETED;
    }

    public synchronized CacheId getCacheId() {
      return cacheId;
    }

    public synchronized void parseSummaryLogs() throws IOException {
      if (!isDone()) {
        appState = EntityFileTimelineStore.getAppState(
            cacheId.getApplicationAttemptId().getApplicationId(), yarnClient);
        long recentLogModTime = scanForLogs();
        if (appState == AppState.UNKNOWN) {
          if (Time.now() - recentLogModTime > unknownActiveMillis) {
            LOG.info(
                "{} state is UNKNOWN and logs are stale, assuming COMPLETED",
                cacheId);
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

    public synchronized TimelineStore refreshCache(CacheId cacheId)
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
            cacheStore = new LevelDBCacheTimelineStore(cacheId.toString());
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
      Path donePath = getDonePath(cacheId);
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
    private CachedLogs cachedLogs;

    public ActiveLogParser(CachedLogs logs) {
      cachedLogs = logs;
    }

    @Override
    public void run() {
      try {
        cachedLogs.parseSummaryLogs();
        if (cachedLogs.isDone()) {
          cachedLogs.moveToDone();
          cacheIdLogMap.remove(cachedLogs.getCacheId());
        }
      } catch (Exception e) {
        LOG.error("Error processing logs for " + cachedLogs.getCacheId(), e);
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

  private TimelineStore getTimelineStoreForReadGeneral(Set<CacheId> cacheIds,
      String entityType) throws IOException {
    TimelineStore store = null;
    // For now we just handle one store in a context. We return the first
    // non-null storage for the cache ids.
    for (CacheId cacheId : cacheIds) {
      TimelineStore storeForId = getCachedStore(cacheId);
      if (storeForId != null) {
        store = storeForId;
      }
    }
    if (store == null) {
      LOG.debug("Using summary store for {}", entityType);
      store = this.summaryStore;
    }
    return store;
  }

  private TimelineStore getTimelineStoreForRead(String entityId,
      String entityType) throws IOException {
    Set<CacheId> cacheIds = cacheIdPlugin.getCacheId(entityId, entityType);
    return getTimelineStoreForReadGeneral(cacheIds, entityType);
  }

  private TimelineStore getTimelineStoreForRead(String entityType,
      NameValuePair primaryFilter, Collection<NameValuePair> secondaryFilters)
    throws IOException {
    Set<CacheId> cacheIds = cacheIdPlugin.getCacheId(entityType, primaryFilter,
        secondaryFilters);
    return getTimelineStoreForReadGeneral(cacheIds, entityType);
  }


  // find a cached timeline store or null if it cannot be located
  private TimelineStore getCachedStore(CacheId cacheId)
      throws IOException {
    CachedLogs cachedLogs = null;
    synchronized (this.cachedLogs) {
      cachedLogs = this.cachedLogs.get(cacheId);
      if (cachedLogs == null) {
        cachedLogs = findCacheLogs(cacheId);
        if (cachedLogs != null) {
          this.cachedLogs.put(cacheId, cachedLogs);
        }
      }
    }
    TimelineStore store = null;
    if (cachedLogs != null) {
      store = cachedLogs.refreshCache(cacheId);
    }
    return store;
  }

  @Override
  public TimelineEntities getEntities(String entityType, Long limit,
      Long windowStart, Long windowEnd, String fromId, Long fromTs,
      NameValuePair primaryFilter, Collection<NameValuePair> secondaryFilters,
      EnumSet<Field> fieldsToRetrieve, CheckAcl checkAcl) throws IOException {
    LOG.debug("getEntities type={} primary={}", entityType, primaryFilter);
    TimelineStore store = getTimelineStoreForRead(entityType,
        primaryFilter, secondaryFilters);
    return store.getEntities(entityType, limit, windowStart, windowEnd,
        fromId, fromTs, primaryFilter, secondaryFilters, fieldsToRetrieve,
        checkAcl);
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
