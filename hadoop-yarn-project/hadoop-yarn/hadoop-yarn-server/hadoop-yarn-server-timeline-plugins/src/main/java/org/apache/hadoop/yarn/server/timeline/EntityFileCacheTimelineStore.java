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
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

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

public class EntityFileCacheTimelineStore extends AbstractService
    implements TimelineStore {

  private static final Logger LOG = LoggerFactory.getLogger(
      EntityFileCacheTimelineStore.class);
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
  // Active dir: <activeRoot>/appId/attemptId/cacheId.log
  // Done dir: <doneRoot>/cluster_ts/hash1/hash2/appId/attemptId/cacheId.log
  private static final String APP_DONE_DIR_PREFIX_FORMAT =
      "%d" + Path.SEPARATOR     // cluster timestamp
      + "%04d" + Path.SEPARATOR // app num / 10000000
      + "%03d" + Path.SEPARATOR // (app num / 1000) % 1000
      + "%s"   + Path.SEPARATOR;// full app id

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
  private List<TimelineCacheIdPlugin> cacheIdPlugins;

  public EntityFileCacheTimelineStore() {
    super(EntityFileCacheTimelineStore.class.getSimpleName());
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
    cacheIdPlugins = loadPlugIns(conf);
    super.serviceInit(conf);
  }

  private List<TimelineCacheIdPlugin> loadPlugIns(Configuration conf)
      throws RuntimeException {
    Collection<String> pluginNames = conf.getStringCollection(
        YarnConfiguration.TIMELINE_SERVICE_CACHE_ID_PLUGIN_CLASS);
    List<TimelineCacheIdPlugin> pluginList = new LinkedList<>();
    for (final String name : pluginNames) {
      TimelineCacheIdPlugin cacheIdPlugin = ReflectionUtils.newInstance(
          conf.getClass(name, EmptyTimelineCacheIdPlugin.class,
              TimelineCacheIdPlugin.class), conf);
      if (cacheIdPlugin == null) {
        throw new RuntimeException("No class defined for " + name);
      }
      pluginList.add(cacheIdPlugin);
    }
    return pluginList;
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
                // We need to pass in the application's dir to build up a
                // CachedLogs
                CachedLogs logs = getActiveLog(cacheId, statApp.getPath());
                executor.execute(new ActiveLogParser(logs));
              }
            }
          }
        }
      }
    }
  }

  private CachedLogs getActiveLog(CacheId cacheId, Path appDirPath) {
    CachedLogs cachedLogs = cacheIdLogMap.get(cacheId);
    if (cachedLogs == null) {
      cachedLogs = new CachedLogs(cacheId, appDirPath, AppState.ACTIVE);
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
      Path appDirPath = getDoneAppPath(cacheId.getApplicationId());
      if (fs.exists(appDirPath)) {
        appState = AppState.COMPLETED;
      } else {
        appDirPath = getActiveAppPath(cacheId.getApplicationId());
        if (fs.exists(appDirPath)) {
          appState = AppState.ACTIVE;
        }
      }
      if (appState != AppState.UNKNOWN) {
        cachedLogs = new CachedLogs(cacheId, appDirPath, appState);
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

  private Path getActiveAppPath(ApplicationId appId) {
    return new Path(activeRootPath, appId.toString());
  }

  private Path getDoneAppPath(ApplicationId appId) {
    // cut up the cache ID into mod(1000) buckets
    int appNum = appId.getId();
    appNum /= 1000;
    int bucket2 = appNum % 1000;
    int bucket1 = appNum / 1000;
    return new Path(doneRootPath,
        String.format(APP_DONE_DIR_PREFIX_FORMAT, appId.getClusterTimestamp(),
            bucket1, bucket2, appId.toString()));
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
    private Path appDirPath;
    private AppState appState;
    private List<LogInfo> summaryLogs = new ArrayList<LogInfo>();
    private List<LogInfo> detailLogs = new ArrayList<LogInfo>();
    private TimelineStore cacheStore = null;
    private long cacheRefreshTime = 0;
    private boolean cacheCompleted = false;

    public CachedLogs(CacheId cacheId, Path appPath, AppState state) {
      this.cacheId = cacheId;
      appDirPath = appPath;
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
        appState = EntityFileCacheTimelineStore.getAppState(
            cacheId.getApplicationId(), yarnClient);
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
        log.parseForSummary(appDirPath, isDone());
      }
    }

    // scans for new logs and returns the modification timestamp of the
    // most recently modified log
    private long scanForLogs() throws IOException {
      long newestModTime = 0;
      RemoteIterator<FileStatus> iter = fs.listStatusIterator(appDirPath);
      while (iter.hasNext()) {
        // TODO: Iterate one more layer for app attempts
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
        newestModTime = fs.getFileStatus(appDirPath).getModificationTime();
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
            log.parseForCache(tdm, appDirPath, isDone());
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
      Path doneAppPath = getDoneAppPath(cacheId.getApplicationId());
      if (!doneAppPath.equals(appDirPath)) {
        Path donePathParent = doneAppPath.getParent();
        if (!fs.exists(donePathParent)) {
          fs.mkdirs(donePathParent);
        }
        if (!fs.rename(appDirPath, doneAppPath)) {
          throw new IOException("Rename " + appDirPath + " to " + doneAppPath
              + " failed");
        } else {
          LOG.info("Moved {} to {}", appDirPath, doneAppPath);
        }
        appDirPath = doneAppPath;
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

    public void parseForSummary(Path appDirPath, boolean appCompleted)
        throws IOException {
      // Iterate in app dir for all attempts
      RemoteIterator<FileStatus> iterAttempt = fs.listStatusIterator(appDirPath);
      while (iterAttempt.hasNext()) {
        FileStatus attemptStat = iterAttempt.next();
        Path attemptPath = attemptStat.getPath();
        Path logPath = new Path(attemptPath, filename);
        String dirname = attemptPath.getName();
        long startTime = Time.monotonicNow();
        LOG.debug("Parsing {}/{} at offset {}", dirname, filename, offset);
        long count = parsePath(summaryTdm, logPath, appCompleted);
        LOG.info("Parsed {} entities from {}/{} in {} msec",
            count, dirname, filename, Time.monotonicNow() - startTime);
      }
    }

    public void parseForCache(TimelineDataManager tdm, Path appDirPath,
        boolean appCompleted) throws IOException {
      // Iterate in app dir for all attempts
      RemoteIterator<FileStatus> iterAttempt = fs.listStatusIterator(appDirPath);
      while (iterAttempt.hasNext()) {
        FileStatus attemptStat = iterAttempt.next();
        Path attemptPath = attemptStat.getPath();
        Path logPath = new Path(attemptPath, filename);
        String dirname = appDirPath.getName();
        long startTime = Time.monotonicNow();
        LOG.debug("Parsing {}/{} at offset {}", dirname, filename, offset);
        long count = parsePath(tdm, logPath, appCompleted);
        LOG.info("Parsed {} entities from {}/{} in {} msec",
            count, dirname, filename, Time.monotonicNow() - startTime);
      }
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

  private List<TimelineStore> getTimelineStoresFromCacheIds(Set<CacheId> cacheIds,
      String entityType) throws IOException {
    List<TimelineStore> stores = new LinkedList<>();
    // For now we just handle one store in a context. We return the first
    // non-null storage for the cache ids.
    for (CacheId cacheId : cacheIds) {
      TimelineStore storeForId = getCachedStore(cacheId);
      if (storeForId != null) {
        stores.add(storeForId);
      }
    }
    if (stores.size() == 0) {
      LOG.debug("Using summary store for {}", entityType);
      stores.add(this.summaryStore);
    }
    return stores;
  }


  private List<TimelineStore> getTimelineStoresForRead(String entityId,
      String entityType) throws IOException {
    Set<CacheId> cacheIds = new HashSet<>();
    for (TimelineCacheIdPlugin cacheIdPlugin : cacheIdPlugins) {
      Set<CacheId> idsFromPlugin
          = cacheIdPlugin.getCacheId(entityId, entityType);
      if (idsFromPlugin != null) {
        cacheIds.addAll(idsFromPlugin);
      }
    }
    return getTimelineStoresFromCacheIds(cacheIds, entityType);
  }

  private List<TimelineStore> getTimelineStoresForRead(String entityType,
      NameValuePair primaryFilter, Collection<NameValuePair> secondaryFilters)
    throws IOException {
    Set<CacheId> cacheIds = new HashSet<>();
    for (TimelineCacheIdPlugin cacheIdPlugin : cacheIdPlugins) {
      Set<CacheId> idsFromPlugin =
          cacheIdPlugin.getCacheId(entityType, primaryFilter, secondaryFilters);
      if (idsFromPlugin != null) {
        cacheIds.addAll(idsFromPlugin);
      }
    }
    return getTimelineStoresFromCacheIds(cacheIds, entityType);
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
    List<TimelineStore> stores = getTimelineStoresForRead(entityType,
        primaryFilter, secondaryFilters);
    TimelineEntities returnEntities = new TimelineEntities();
    for (TimelineStore store : stores) {
      returnEntities.addEntities(
          store.getEntities(entityType, limit, windowStart, windowEnd, fromId,
              fromTs, primaryFilter, secondaryFilters, fieldsToRetrieve,
              checkAcl).getEntities());
    }
    return returnEntities;
  }

  @Override
  public TimelineEntity getEntity(String entityId, String entityType,
      EnumSet<Field> fieldsToRetrieve) throws IOException {
    LOG.debug("getEntity type={} id={}", entityType, entityId);
    List<TimelineStore> stores = getTimelineStoresForRead(entityType, entityId);
    for (TimelineStore store : stores) {
      return store.getEntity(entityId, entityType, fieldsToRetrieve);
    }
    return null;
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
