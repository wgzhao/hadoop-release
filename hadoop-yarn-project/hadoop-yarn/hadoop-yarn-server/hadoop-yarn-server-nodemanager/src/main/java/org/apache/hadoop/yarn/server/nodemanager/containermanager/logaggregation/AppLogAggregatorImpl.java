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

package org.apache.hadoop.yarn.server.nodemanager.containermanager.logaggregation;

import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationAccessType;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.LogAggregationContext;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.event.Dispatcher;
import org.apache.hadoop.yarn.logaggregation.ContainerLogsRetentionPolicy;
import org.apache.hadoop.yarn.logaggregation.AggregatedLogFormat.LogKey;
import org.apache.hadoop.yarn.logaggregation.AggregatedLogFormat.LogValue;
import org.apache.hadoop.yarn.logaggregation.AggregatedLogFormat.LogWriter;
import org.apache.hadoop.yarn.logaggregation.LogAggregationUtils;
import org.apache.hadoop.yarn.server.nodemanager.Context;
import org.apache.hadoop.yarn.server.nodemanager.DeletionService;
import org.apache.hadoop.yarn.server.nodemanager.LocalDirsHandlerService;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.application.ApplicationEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.application.ApplicationEventType;
import org.apache.hadoop.yarn.util.ConverterUtils;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;


public class AppLogAggregatorImpl implements AppLogAggregator {

  private static final Log LOG = LogFactory
      .getLog(AppLogAggregatorImpl.class);
  private static final int THREAD_SLEEP_TIME = 1000;

  private final LocalDirsHandlerService dirsHandler;
  private final Dispatcher dispatcher;
  private final ApplicationId appId;
  private final NodeId nodeId;
  private final String applicationId;
  private boolean logAggregationDisabled = false;
  private final Configuration conf;
  private final DeletionService delService;
  private final UserGroupInformation userUgi;
  private final Path remoteNodeLogFileForApp;
  private Path remoteNodeTmpLogFileForApp;
  private final ContainerLogsRetentionPolicy retentionPolicy;

  private final BlockingQueue<ContainerId> pendingContainers;
  private final AtomicBoolean appFinishing = new AtomicBoolean();
  private final AtomicBoolean appAggregationFinished = new AtomicBoolean();
  private final AtomicBoolean aborted = new AtomicBoolean();
  private final Map<ApplicationAccessType, String> appAcls;
  private final LogAggregationContext logAggregationContext;
  private final Context context;
  private final int retentionSize;
  private Set<Path> currentUploadedFiles = new HashSet<Path>();
  private Set<String> alreadyUploadedLogs = new HashSet<String>();
  private Set<String> currentExistingLogFiles = new HashSet<String>();

  public AppLogAggregatorImpl(Dispatcher dispatcher,
      DeletionService deletionService, Configuration conf, NodeId nodeId,
      ApplicationId appId, UserGroupInformation userUgi,
      LocalDirsHandlerService dirsHandler, Path remoteNodeLogDirForApp,
      ContainerLogsRetentionPolicy retentionPolicy,
      Map<ApplicationAccessType, String> appAcls,
      LogAggregationContext logAggregationContext,
      Context context) {
    this.dispatcher = dispatcher;
    this.conf = conf;
    this.delService = deletionService;
    this.nodeId = nodeId;
    this.appId = appId;
    this.applicationId = ConverterUtils.toString(appId);
    this.userUgi = userUgi;
    this.dirsHandler = dirsHandler;
    this.remoteNodeLogFileForApp = remoteNodeLogDirForApp;
    this.retentionPolicy = retentionPolicy;
    this.pendingContainers = new LinkedBlockingQueue<ContainerId>();
    this.appAcls = appAcls;
    this.logAggregationContext = logAggregationContext;
    this.context = context;
    this.retentionSize =
        conf.getInt(YarnConfiguration.HISTORY_LOG_RETENTION_SIZE,
            YarnConfiguration.DEFAULT_HISTORY_LOG_RETENTION_SIZE);
  }

  private void uploadLogsForContainer(ContainerId containerId, LogWriter writer) {

    LOG.info("Uploading logs for container " + containerId
        + ". Current good log dirs are "
        + StringUtils.join(",", dirsHandler.getLogDirs()));
    LogKey logKey = new LogKey(containerId);
    LogValue logValue =
        new LogValue(dirsHandler.getLogDirs(), containerId,
          userUgi.getShortUserName(), this.logAggregationContext,
          this.alreadyUploadedLogs);
    try {
      writer.append(logKey, logValue);
    } catch (IOException e) {
      LOG.error("Couldn't upload logs for " + containerId
          + ". Skipping this container.");
    }
    currentUploadedFiles.addAll(logValue.getCurrentUpLoadedFilesPath());
    alreadyUploadedLogs.addAll(logValue.getCurrentUpLoadedFileMeta());
    currentExistingLogFiles.addAll(logValue.getAllExistingFilesMeta());
  }

  private void uploadLogsForContainers(Set<ContainerId> containers) {
    if (this.logAggregationDisabled || containers.isEmpty()) {
      return;
    }
    this.remoteNodeTmpLogFileForApp =
        getRemoteNodeTmpLogFileForApp(this.remoteNodeLogFileForApp);

    LogWriter writer = null;
    try {
      try {
        writer =
            new LogWriter(this.conf, this.remoteNodeTmpLogFileForApp,
              this.userUgi);
        // Write ACLs once when the writer is created.
        writer.writeApplicationACLs(appAcls);
        writer.writeApplicationOwner(this.userUgi.getShortUserName());

      } catch (IOException e1) {
        LOG.error("Cannot create writer for app " + this.applicationId
            + ". Skip log upload this time. ");
        return;
      }

      currentUploadedFiles.clear();
      currentExistingLogFiles.clear();
      for (ContainerId container : containers) {
        uploadLogsForContainer(container, writer);
      }

      this.delService.delete(this.userUgi.getShortUserName(), null,
        currentUploadedFiles.toArray(new Path[currentUploadedFiles.size()]));

      // if any of the previous uoloaded logs have been deleted,
      // we need to remove them from alreadyUploadedLogs
      Iterable<String> mask =
          Iterables.filter(alreadyUploadedLogs, new Predicate<String>() {
            @Override
            public boolean apply(String next) {
              return currentExistingLogFiles.contains(next);
            }
          });

      alreadyUploadedLogs = Sets.newHashSet(mask);

      if (writer != null) {
        writer.close();
      }

      final Path renamedPath = logAggregationContext == null ||
          logAggregationContext.getRollingIntervalSeconds() <= 0
              ? remoteNodeLogFileForApp : new Path(
                remoteNodeLogFileForApp.getParent(),
                remoteNodeLogFileForApp.getName() + "_"
                    + System.currentTimeMillis());
      try {
        userUgi.doAs(new PrivilegedExceptionAction<Object>() {
          @Override
          public Object run() throws Exception {
            FileSystem remoteFS = FileSystem.get(conf);
            if (remoteFS.exists(remoteNodeTmpLogFileForApp)) {
              remoteFS.rename(remoteNodeTmpLogFileForApp, renamedPath);
            }
            return null;
          }
        });
        cleanOldLogs();
      } catch (Exception e) {
        LOG.error(
          "Failed to move temporary log file to final location: ["
              + remoteNodeTmpLogFileForApp + "] to ["
              + renamedPath + "]", e);
      }
    } finally {
      if (writer != null) {
        writer.close();
      }
    }
  }

  private void cleanOldLogs() {
    try {
      final FileSystem remoteFS = FileSystem.get(conf);
      Path appDir =
          this.remoteNodeLogFileForApp.getParent().makeQualified(
            remoteFS.getUri(), remoteFS.getWorkingDirectory());
      Set<FileStatus> status =
          new HashSet<FileStatus>(Arrays.asList(remoteFS.listStatus(appDir)));

      Iterable<FileStatus> mask =
          Iterables.filter(status, new Predicate<FileStatus>() {
            @Override
            public boolean apply(FileStatus next) {
              return next.getPath().getName()
                .contains(LogAggregationUtils.getNodeString(nodeId));
            }
          });
      status = Sets.newHashSet(mask);
      if (status.size() > this.retentionSize) {
        List<FileStatus> statusList = new ArrayList<FileStatus>(status);
        Collections.sort(statusList, new Comparator<FileStatus>() {
          public int compare(FileStatus s1, FileStatus s2) {
            return s1.getModificationTime() < s2.getModificationTime() ? -1
                : s1.getModificationTime() > s2.getModificationTime() ? 1 : 0;
          }
        });
        final FileStatus remove = statusList.get(0);
        try {
          userUgi.doAs(new PrivilegedExceptionAction<Object>() {
            @Override
            public Object run() throws Exception {
              remoteFS.delete(remove.getPath(), false);
              return null;
            }
          });
        } catch (Exception e) {
          LOG.error("Failed to delete " + remove, e);
        }
      }
    } catch (Exception e) {
      LOG.error("Failed to clean old logs", e);
    }
  }

  @Override
  public void run() {
    try {
      doAppLogAggregation();
    } finally {
      if (!this.appAggregationFinished.get()) {
        LOG.warn("Aggregation did not complete for application " + appId);
      }
      this.appAggregationFinished.set(true);
    }
  }

  @SuppressWarnings("unchecked")
  private void doAppLogAggregation() {
    while (!this.appFinishing.get() && !this.aborted.get()) {
      synchronized(this) {
        try {
          if (this.logAggregationContext != null && this.logAggregationContext
              .getRollingIntervalSeconds() > 0) {
            wait(this.logAggregationContext.getRollingIntervalSeconds() * 1000);
            if (this.appFinishing.get() || this.aborted.get()) {
              break;
            }
            uploadLogsForContainers(this.context.getApplications()
              .get(this.appId).getContainers().keySet());
          } else {
            wait(THREAD_SLEEP_TIME);
          }
        } catch (InterruptedException e) {
          LOG.warn("PendingContainers queue is interrupted");
          this.appFinishing.set(true);
        }
      }
    }

    if (this.aborted.get()) {
      return;
    }

    // App is finished, upload the container logs.
    Set<ContainerId> ids = new HashSet<ContainerId>();
    ContainerId containerId;
    while ((containerId = this.pendingContainers.poll()) != null) {
      ids.add(containerId);
    }
    uploadLogsForContainers(ids);

    // Remove the local app-log-dirs
    List<String> rootLogDirs = dirsHandler.getLogDirs();
    Path[] localAppLogDirs = new Path[rootLogDirs.size()];
    int index = 0;
    for (String rootLogDir : rootLogDirs) {
      localAppLogDirs[index] = new Path(rootLogDir, this.applicationId);
      index++;
    }
    this.delService.delete(this.userUgi.getShortUserName(), null,
        localAppLogDirs);
    
    this.dispatcher.getEventHandler().handle(
        new ApplicationEvent(this.appId,
            ApplicationEventType.APPLICATION_LOG_HANDLING_FINISHED));
    this.appAggregationFinished.set(true);    
  }

  private Path getRemoteNodeTmpLogFileForApp(Path remoteNodeLogDirForApp) {
    return new Path(remoteNodeLogFileForApp.getParent(),
      (remoteNodeLogFileForApp.getName() + LogAggregationUtils.TMP_FILE_SUFFIX));
  }

  private boolean shouldUploadLogs(ContainerId containerId,
      boolean wasContainerSuccessful) {

    // All containers
    if (this.retentionPolicy
        .equals(ContainerLogsRetentionPolicy.ALL_CONTAINERS)) {
      return true;
    }

    // AM Container only
    if (this.retentionPolicy
        .equals(ContainerLogsRetentionPolicy.APPLICATION_MASTER_ONLY)) {
      if (containerId.getId() == 1) {
        return true;
      }
      return false;
    }

    // AM + Failing containers
    if (this.retentionPolicy
        .equals(ContainerLogsRetentionPolicy.AM_AND_FAILED_CONTAINERS_ONLY)) {
      if (containerId.getId() == 1) {
        return true;
      } else if(!wasContainerSuccessful) {
        return true;
      }
      return false;
    }
    return false;
  }

  @Override
  public void startContainerLogAggregation(ContainerId containerId,
      boolean wasContainerSuccessful) {
    if (shouldUploadLogs(containerId, wasContainerSuccessful)) {
      LOG.info("Considering container " + containerId
          + " for log-aggregation");
      this.pendingContainers.add(containerId);
    }
  }

  @Override
  public synchronized void finishLogAggregation() {
    LOG.info("Application just finished : " + this.applicationId);
    this.appFinishing.set(true);
    this.notifyAll();
  }

  @Override
  public synchronized void abortLogAggregation() {
    LOG.info("Aborting log aggregation for " + this.applicationId);
    this.aborted.set(true);
    this.notifyAll();
  }

  @Private
  @VisibleForTesting
  public synchronized void doLogAggregationOutOfBand() {
    LOG.info("Do OutOfBand log aggregation");
    this.notifyAll();
  }
}
