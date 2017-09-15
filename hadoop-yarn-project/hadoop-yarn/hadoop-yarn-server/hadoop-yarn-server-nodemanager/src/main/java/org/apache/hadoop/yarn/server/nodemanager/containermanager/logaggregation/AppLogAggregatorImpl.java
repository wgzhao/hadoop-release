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
import java.util.ArrayList;
import java.util.HashMap;
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
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.UnsupportedFileSystemException;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.yarn.api.records.ApplicationAccessType;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.LogAggregationContext;
import org.apache.hadoop.yarn.api.records.LogAggregationStatus;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.event.Dispatcher;
import org.apache.hadoop.yarn.logaggregation.AggregatedLogFormat.LogKey;
import org.apache.hadoop.yarn.logaggregation.AggregatedLogFormat.LogValue;
import org.apache.hadoop.yarn.logaggregation.filecontroller.LogAggregationFileController;
import org.apache.hadoop.yarn.logaggregation.filecontroller.LogAggregationFileControllerContext;
import org.apache.hadoop.yarn.logaggregation.filecontroller.tfile.LogAggregationTFileController;
import org.apache.hadoop.yarn.logaggregation.ContainerLogsRetentionPolicy;
import org.apache.hadoop.yarn.logaggregation.LogAggregationUtils;
import org.apache.hadoop.yarn.server.api.protocolrecords.LogAggregationReport;
import org.apache.hadoop.yarn.server.nodemanager.Context;
import org.apache.hadoop.yarn.server.nodemanager.DeletionService;
import org.apache.hadoop.yarn.server.nodemanager.LocalDirsHandlerService;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.application.ApplicationEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.application.ApplicationEventType;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.util.Records;
import org.apache.hadoop.yarn.util.Times;

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
  private final String applicationId;
  private boolean logAggregationDisabled = false;
  private final DeletionService delService;
  private final UserGroupInformation userUgi;
  private Path remoteNodeLogFileForApp;
  private Path remoteNodeTmpLogFileForApp;
  private final ContainerLogsRetentionPolicy retentionPolicy;

  private final BlockingQueue<ContainerId> pendingContainers;
  private final AtomicBoolean appFinishing = new AtomicBoolean();
  private final AtomicBoolean appAggregationFinished = new AtomicBoolean();
  private final AtomicBoolean aborted = new AtomicBoolean();
  private final Map<ApplicationAccessType, String> appAcls;
  private final FileContext lfs;
  private final LogAggregationContext logAggregationContext;
  private final Context context;
  private final NodeId nodeId;
  private final LogAggregationFileControllerContext logControllerContext;
  private final LogAggregationFileController logAggregationFileController;

  // This variable is only for testing
  private final AtomicBoolean waiting = new AtomicBoolean(false);

  private boolean renameTemporaryLogFileFailed = false;

  private final Map<ContainerId, ContainerLogAggregator> containerLogAggregators =
      new HashMap<ContainerId, ContainerLogAggregator>();

  public AppLogAggregatorImpl(Dispatcher dispatcher,
      DeletionService deletionService, Configuration conf,
      ApplicationId appId, UserGroupInformation userUgi, NodeId nodeId,
      LocalDirsHandlerService dirsHandler, Path remoteNodeLogFileForApp,
      ContainerLogsRetentionPolicy retentionPolicy,
      Map<ApplicationAccessType, String> appAcls,
      LogAggregationContext logAggregationContext, Context context,
      FileContext lfs, long rollingMonitorInterval,
      LogAggregationFileController logAggregationFileController) {
    this.dispatcher = dispatcher;
    this.delService = deletionService;
    this.appId = appId;
    this.applicationId = ConverterUtils.toString(appId);
    this.userUgi = userUgi;
    this.dirsHandler = dirsHandler;
    this.remoteNodeLogFileForApp = remoteNodeLogFileForApp;
    this.remoteNodeTmpLogFileForApp = getRemoteNodeTmpLogFileForApp();
    this.retentionPolicy = retentionPolicy;
    this.pendingContainers = new LinkedBlockingQueue<ContainerId>();
    this.appAcls = appAcls;
    this.lfs = lfs;
    this.logAggregationContext = logAggregationContext;
    this.context = context;
    this.nodeId = nodeId;
    if (logAggregationFileController == null) {
      // by default, use T-File Controller
      this.logAggregationFileController = new LogAggregationTFileController();
      this.logAggregationFileController.initialize(conf, "TFile");
      this.logAggregationFileController.verifyAndCreateRemoteLogDir();
      this.logAggregationFileController.createAppDir(
          this.userUgi.getShortUserName(), appId, userUgi);
      this.remoteNodeLogFileForApp = this.logAggregationFileController
          .getRemoteNodeLogFileForApp(appId,
              this.userUgi.getShortUserName(), nodeId);
      this.remoteNodeTmpLogFileForApp = getRemoteNodeTmpLogFileForApp();
    } else {
      this.logAggregationFileController = logAggregationFileController;
      this.remoteNodeLogFileForApp = remoteNodeLogFileForApp;
      this.remoteNodeTmpLogFileForApp = getRemoteNodeTmpLogFileForApp();
    }
    boolean logAggregationInRolling =
        rollingMonitorInterval <= 0 || this.logAggregationContext == null
            || this.logAggregationContext.getRolledLogsIncludePattern() == null
            || this.logAggregationContext.getRolledLogsIncludePattern()
                .isEmpty() ? false : true;
    logControllerContext = new LogAggregationFileControllerContext(
        this.remoteNodeLogFileForApp,
        this.remoteNodeTmpLogFileForApp,
        logAggregationInRolling,
        rollingMonitorInterval,
        this.appId, this.appAcls, this.nodeId, this.userUgi);
  }

  private void uploadLogsForContainers(boolean appFinished) {
    if (this.logAggregationDisabled) {
      return;
    }

    if (UserGroupInformation.isSecurityEnabled()) {
      Credentials systemCredentials =
          context.getSystemCredentialsForApps().get(appId);
      if (systemCredentials != null) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Adding new framework-token for " + appId
              + " for log-aggregation: " + systemCredentials.getAllTokens()
              + "; userUgi=" + userUgi);
        }
        // this will replace old token
        userUgi.addCredentials(systemCredentials);
      }
    }

    // Create a set of Containers whose logs will be uploaded in this cycle.
    // It includes:
    // a) all containers in pendingContainers: those containers are finished
    //    and satisfy the retentionPolicy.
    // b) some set of running containers: For all the Running containers,
    // we have ContainerLogsRetentionPolicy.AM_AND_FAILED_CONTAINERS_ONLY,
    // so simply set wasContainerSuccessful as true to
    // bypass FAILED_CONTAINERS check and find the running containers 
    // which satisfy the retentionPolicy.
    Set<ContainerId> pendingContainerInThisCycle = new HashSet<ContainerId>();
    this.pendingContainers.drainTo(pendingContainerInThisCycle);
    Set<ContainerId> finishedContainers =
        new HashSet<ContainerId>(pendingContainerInThisCycle);
    if (this.context.getApplications().get(this.appId) != null) {
      for (ContainerId container : this.context.getApplications()
        .get(this.appId).getContainers().keySet()) {
        if (shouldUploadLogs(container, true)) {
          pendingContainerInThisCycle.add(container);
        }
      }
    }

    try {
      try {
        logAggregationFileController.initializeWriter(logControllerContext);
      } catch (IOException e1) {
        LOG.error("Cannot create writer for app " + this.applicationId
            + ". Skip log upload this time. ", e1);
        return;
      }

      boolean uploadedLogsInThisCycle = false;
      for (ContainerId container : pendingContainerInThisCycle) {
        ContainerLogAggregator aggregator = null;
        if (containerLogAggregators.containsKey(container)) {
          aggregator = containerLogAggregators.get(container);
        } else {
          aggregator = new ContainerLogAggregator(container);
          containerLogAggregators.put(container, aggregator);
        }
        Set<Path> uploadedFilePathsInThisCycle =
            aggregator.doContainerLogAggregation(logAggregationFileController,
            appFinished, finishedContainers.contains(container));
        if (uploadedFilePathsInThisCycle.size() > 0) {
          uploadedLogsInThisCycle = true;
          this.delService.delete(this.userUgi.getShortUserName(), null,
              uploadedFilePathsInThisCycle
                  .toArray(new Path[uploadedFilePathsInThisCycle.size()]));
        }

        // This container is finished, and all its logs have been uploaded,
        // remove it from containerLogAggregators.
        if (finishedContainers.contains(container)) {
          containerLogAggregators.remove(container);
        }
      }

      logControllerContext.setUploadedLogsInThisCycle(uploadedLogsInThisCycle);
      logControllerContext.setLogUploadTimeStamp(System.currentTimeMillis());
      logControllerContext.increLogAggregationTimes();
      String diagnosticMessage = "";
      boolean logAggregationSucceedInThisCycle = true;
      try {
        this.logAggregationFileController.postWrite(logControllerContext);
        diagnosticMessage = "Log uploaded successfully for Application: "
            + appId + " in NodeManager: "
            + LogAggregationUtils.getNodeString(nodeId) + " at "
            + Times.format(logControllerContext.getLogUploadTimeStamp())
            + "\n";
      } catch (Exception e) {
        diagnosticMessage = e.getMessage();
        renameTemporaryLogFileFailed = true;
        logAggregationSucceedInThisCycle = false;
      }

      LogAggregationReport report =
          Records.newRecord(LogAggregationReport.class);
      report.setApplicationId(appId);
      report.setDiagnosticMessage(diagnosticMessage);
      report.setLogAggregationStatus(logAggregationSucceedInThisCycle
          ? LogAggregationStatus.RUNNING
          : LogAggregationStatus.RUNNING_WITH_FAILURE);
      this.context.getLogAggregationStatusForApps().add(report);
      if (appFinished) {
        // If the app is finished, one extra final report with log aggregation
        // status SUCCEEDED/FAILED will be sent to RM to inform the RM
        // that the log aggregation in this NM is completed.
        LogAggregationReport finalReport =
            Records.newRecord(LogAggregationReport.class);
        finalReport.setApplicationId(appId);
        finalReport.setLogAggregationStatus(renameTemporaryLogFileFailed
            ? LogAggregationStatus.FAILED : LogAggregationStatus.SUCCEEDED);
        this.context.getLogAggregationStatusForApps().add(finalReport);
      }
    } finally {
      logAggregationFileController.closeWriter();
    }
  }

  @SuppressWarnings("unchecked")
  @Override
  public void run() {
    try {
      doAppLogAggregation();
    } catch (Exception e) {
      // do post clean up of log directories on any exception
      LOG.error("Error occured while aggregating the log for the application "
          + appId, e);
      doAppLogAggregationPostCleanUp();
    } finally {
      if (!this.appAggregationFinished.get() && !this.aborted.get()) {
        LOG.warn("Log aggregation did not complete for application " + appId);
        this.dispatcher.getEventHandler().handle(
            new ApplicationEvent(this.appId,
                ApplicationEventType.APPLICATION_LOG_HANDLING_FAILED));
      }
      this.appAggregationFinished.set(true);
    }
  }

  @SuppressWarnings("unchecked")
  private void doAppLogAggregation() {
    while (!this.appFinishing.get() && !this.aborted.get()) {
      synchronized(this) {
        try {
          waiting.set(true);
          if (logControllerContext.isLogAggregationInRolling()) {
            wait(logControllerContext.getRollingMonitorInterval() * 1000);
            if (this.appFinishing.get() || this.aborted.get()) {
              break;
            }
            uploadLogsForContainers(false);
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
    uploadLogsForContainers(true);

    doAppLogAggregationPostCleanUp();

    this.dispatcher.getEventHandler().handle(
        new ApplicationEvent(this.appId,
            ApplicationEventType.APPLICATION_LOG_HANDLING_FINISHED));
    this.appAggregationFinished.set(true);
  }

  private void doAppLogAggregationPostCleanUp() {
    // Remove the local app-log-dirs
    List<Path> localAppLogDirs = new ArrayList<Path>();
    for (String rootLogDir : dirsHandler.getLogDirsForCleanup()) {
      Path logPath = new Path(rootLogDir, applicationId);
      try {
        // check if log dir exists
        lfs.getFileStatus(logPath);
        localAppLogDirs.add(logPath);
      } catch (UnsupportedFileSystemException ue) {
        LOG.warn("Log dir " + rootLogDir + "is an unsupported file system", ue);
        continue;
      } catch (IOException fe) {
        continue;
      }
    }

    if (localAppLogDirs.size() > 0) {
      this.delService.delete(this.userUgi.getShortUserName(), null,
        localAppLogDirs.toArray(new Path[localAppLogDirs.size()]));
    }
  }

  private Path getRemoteNodeTmpLogFileForApp() {
    return new Path(remoteNodeLogFileForApp.getParent(),
      (remoteNodeLogFileForApp.getName() + LogAggregationUtils.TMP_FILE_SUFFIX));
  }

  // TODO: The condition: containerId.getId() == 1 to determine an AM container
  // is not always true.
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
      if ((containerId.getContainerId()
          & ContainerId.CONTAINER_ID_BITMASK)== 1) {
        return true;
      }
      return false;
    }

    // AM + Failing containers
    if (this.retentionPolicy
        .equals(ContainerLogsRetentionPolicy.AM_AND_FAILED_CONTAINERS_ONLY)) {
      if ((containerId.getContainerId()
          & ContainerId.CONTAINER_ID_BITMASK) == 1) {
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

  @Override
  public void disableLogAggregation() {
    this.logAggregationDisabled = true;
  }

  @Private
  @VisibleForTesting
  // This is only used for testing.
  // This will wake the log aggregation thread that is waiting for
  // rollingMonitorInterval.
  // To use this method, make sure the log aggregation thread is running
  // and waiting for rollingMonitorInterval.
  public synchronized void doLogAggregationOutOfBand() {
    while(!waiting.get()) {
      try {
        wait(200);
      } catch (InterruptedException e) {
        // Do Nothing
      }
    }
    LOG.info("Do OutOfBand log aggregation");
    this.notifyAll();
  }

  private class ContainerLogAggregator {
    private final ContainerId containerId;
    private Set<String> uploadedFileMeta =
        new HashSet<String>();
    
    public ContainerLogAggregator(ContainerId containerId) {
      this.containerId = containerId;
    }

    public Set<Path> doContainerLogAggregation(
        LogAggregationFileController logAggregationFileController,
        boolean appFinished, boolean containerFinished) {
      LOG.info("Uploading logs for container " + containerId
          + ". Current good log dirs are "
          + StringUtils.join(",", dirsHandler.getLogDirsForRead()));
      final LogKey logKey = new LogKey(containerId);
      final LogValue logValue =
          new LogValue(dirsHandler.getLogDirsForRead(), containerId,
              userUgi.getShortUserName(), logAggregationContext,
              this.uploadedFileMeta, appFinished, containerFinished);
      try {
        logAggregationFileController.write(logKey, logValue);
      } catch (Exception e) {
        LOG.error("Couldn't upload logs for " + containerId
            + ". Skipping this container.", e);
        return new HashSet<Path>();
      }
      this.uploadedFileMeta.addAll(logValue
        .getCurrentUpLoadedFileMeta());
      // if any of the previous uploaded logs have been deleted,
      // we need to remove them from alreadyUploadedLogs
      Iterable<String> mask =
          Iterables.filter(uploadedFileMeta, new Predicate<String>() {
            @Override
            public boolean apply(String next) {
              return logValue.getAllExistingFilesMeta().contains(next);
            }
          });

      this.uploadedFileMeta = Sets.newHashSet(mask);
      return logValue.getCurrentUpLoadedFilesPath();
    }
  }

  // only for test
  @VisibleForTesting
  public UserGroupInformation getUgi() {
    return this.userUgi;
  }

  @VisibleForTesting
  public LogAggregationFileController getLogAggregationFileController() {
    return this.logAggregationFileController;
  }

  @VisibleForTesting
  public LogAggregationFileControllerContext
      getLogAggregationFileControllerContext() {
    return this.logControllerContext;
  }
}
