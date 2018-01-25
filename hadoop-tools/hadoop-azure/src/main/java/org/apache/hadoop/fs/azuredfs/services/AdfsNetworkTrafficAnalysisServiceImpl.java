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

package org.apache.hadoop.fs.azuredfs.services;

import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import com.google.common.base.Preconditions;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import org.threadly.concurrent.collections.ConcurrentArrayList;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.azuredfs.contracts.services.AdfsHttpClientSession;
import org.apache.hadoop.fs.azuredfs.contracts.services.AdfsNetworkThroughputAnalysisResult;
import org.apache.hadoop.fs.azuredfs.contracts.services.AdfsNetworkTrafficAnalysisResult;
import org.apache.hadoop.fs.azuredfs.contracts.services.AdfsNetworkThroughputMetrics;
import org.apache.hadoop.fs.azuredfs.contracts.services.AdfsNetworkTrafficAnalysisService;
import org.apache.hadoop.fs.azuredfs.contracts.services.AdfsNetworkTrafficMetrics;
import org.apache.hadoop.fs.azuredfs.contracts.services.LoggingService;

@Singleton
@InterfaceAudience.Private
@InterfaceStability.Evolving
final class AdfsNetworkTrafficAnalysisServiceImpl implements AdfsNetworkTrafficAnalysisService {
  private static final int DEFAULT_ANALYSIS_PERIOD_MS = 10 * 1000;
  private static final double MIN_ACCEPTABLE_ERROR_PERCENTAGE = .1;
  private static final double MAX_EQUILIBRIUM_ERROR_PERCENTAGE = 1;
  private static final double RAPID_SLEEP_DECREASE_FACTOR = .75;
  private static final double RAPID_SLEEP_DECREASE_TRANSITION_PERIOD_MS = 150 * 1000;
  private static final double SLEEP_DECREASE_FACTOR = .975;
  private static final double SLEEP_INCREASE_FACTOR = 1.05;
  private static final int MIN_ANALYSIS_PERIOD_MS = 1000;
  private static final int MAX_ANALYSIS_PERIOD_MS = 30000;

  private final LoggingService loggingService;
  private final ConcurrentHashMap<String, AtomicReference<AdfsNetworkTrafficMetrics>> adfsNetworkTrafficMetricsCache;
  private final ConcurrentHashMap<String, AdfsNetworkTrafficAnalysisResultImpl> adfsNetworkTrafficAnalysisResultCache;
  private final ConcurrentHashMap<String, Timer> adfsNetworkTrafficAnalysisTimers;
  private final ConcurrentHashMap<String, ConcurrentArrayList<AdfsHttpClientSession>> registeredAdfsClientSessions;

  @Inject
  AdfsNetworkTrafficAnalysisServiceImpl(
      final LoggingService loggingService) {
    Preconditions.checkNotNull(loggingService, "loggingService");

    this.loggingService = loggingService.get(AdfsNetworkTrafficAnalysisService.class);
    this.adfsNetworkTrafficMetricsCache = new ConcurrentHashMap<>();
    this.adfsNetworkTrafficAnalysisResultCache = new ConcurrentHashMap<>();
    this.adfsNetworkTrafficAnalysisTimers = new ConcurrentHashMap<>();
    this.registeredAdfsClientSessions = new ConcurrentHashMap<>();
  }

  @Override
  public synchronized void subscribeForAnalysis(AdfsHttpClientSession adfsHttpClientSession) {
    this.subscribeForAnalysis(adfsHttpClientSession, DEFAULT_ANALYSIS_PERIOD_MS);
  }

  @Override
  public synchronized void subscribeForAnalysis(final AdfsHttpClientSession adfsHttpClientSession, final int analysisFrequencyInMs) {
    Preconditions.checkArgument(
        analysisFrequencyInMs >= MIN_ANALYSIS_PERIOD_MS && analysisFrequencyInMs <= MAX_ANALYSIS_PERIOD_MS,
        "The argument 'period' must be between 1000 and 30000.");

    final String accountName = adfsHttpClientSession.getStorageCredentialsAccountAndKey().getAccountName();

    if (this.adfsNetworkTrafficAnalysisResultCache.get(accountName) != null
        && this.adfsNetworkTrafficMetricsCache.get(accountName) != null
        && this.adfsNetworkTrafficAnalysisTimers.get(accountName) != null
        && this.registeredAdfsClientSessions.get(accountName) != null) {

      ConcurrentArrayList<AdfsHttpClientSession> registeredSessions = this.registeredAdfsClientSessions.get(accountName);
      if(!registeredSessions.contains(adfsHttpClientSession)) {
        registeredSessions.add(adfsHttpClientSession);
      }

      return;
    }

    if (this.adfsNetworkTrafficAnalysisResultCache.get(accountName) != null
        || this.adfsNetworkTrafficMetricsCache.get(accountName) != null
        || this.adfsNetworkTrafficAnalysisTimers.get(accountName) != null
        || this.registeredAdfsClientSessions.get(accountName) != null) {
      throw new IllegalStateException("Corrupted state");
    }

    final AtomicReference<AdfsNetworkTrafficMetrics> networkTrafficMetrics =
        new AtomicReference<AdfsNetworkTrafficMetrics>(new AdfsNetworkTrafficMetricsImpl(System.currentTimeMillis()));

    final AdfsNetworkTrafficAnalysisResultImpl networkTrafficAnalysisResult =
        new AdfsNetworkTrafficAnalysisResultImpl();

    this.adfsNetworkTrafficAnalysisResultCache.put(accountName, networkTrafficAnalysisResult);
    this.adfsNetworkTrafficMetricsCache.put(accountName, networkTrafficMetrics);

    final Timer timer = new Timer(String.format(accountName + "-AdfsNetworkTrafficAnalysisServiceImplTimer"));
    final AnalysisBackgroundTask task = new AnalysisBackgroundTask(
        accountName,
        this.adfsNetworkTrafficMetricsCache,
        this.adfsNetworkTrafficAnalysisResultCache,
        analysisFrequencyInMs);

    timer.schedule(
        task,
        analysisFrequencyInMs,
        analysisFrequencyInMs);

    this.adfsNetworkTrafficAnalysisTimers.put(accountName, timer);

    ConcurrentArrayList<AdfsHttpClientSession> subscribedSessions = new ConcurrentArrayList<>();
    subscribedSessions.add(adfsHttpClientSession);

    this.registeredAdfsClientSessions.put(accountName, subscribedSessions);
  }

  @Override
  public synchronized void unsubscribeFromAnalysis(final AdfsHttpClientSession adfsHttpClientSession) {
    final String accountName = adfsHttpClientSession.getStorageCredentialsAccountAndKey().getAccountName();

    final ConcurrentArrayList<AdfsHttpClientSession> subscribedSessions = this.registeredAdfsClientSessions.get(accountName);

    if (subscribedSessions == null) {
      return;
    }

    if (subscribedSessions.contains(adfsHttpClientSession)) {
      subscribedSessions.remove(adfsHttpClientSession);
    }

    if (subscribedSessions.size() != 0) {
      return;
    }

    final Timer timer = this.adfsNetworkTrafficAnalysisTimers.remove(accountName);

    if (timer == null) {
      return;
    }

    timer.cancel();

    this.adfsNetworkTrafficAnalysisResultCache.remove(accountName);
    this.adfsNetworkTrafficMetricsCache.remove(accountName);
    this.registeredAdfsClientSessions.remove(accountName);
  }

  @Override
  public AdfsNetworkTrafficMetrics getAdfsNetworkThroughputMetrics(AdfsHttpClientSession adfsHttpClientSession) {
    final String accountName = adfsHttpClientSession.getStorageCredentialsAccountAndKey().getAccountName();
    return adfsNetworkTrafficMetricsCache.get(accountName).get();
  }

  @Override
  public AdfsNetworkTrafficAnalysisResult getAdfsNetworkTrafficAnalysisResult(AdfsHttpClientSession adfsHttpClientSession) {
    final String accountName = adfsHttpClientSession.getStorageCredentialsAccountAndKey().getAccountName();
    return adfsNetworkTrafficAnalysisResultCache.get(accountName);
  }

  private AdfsNetworkThroughputAnalysisResult analyzeMetricsAndUpdateSleepDuration(
      final String accountName,
      final AdfsNetworkThroughputAnalysisResult adfsNetworkTrafficAnalysisResult,
      final AdfsNetworkThroughputMetrics metrics,
      final long timeElapsed,
      final int analysisFrequencyInMs) {
    final double percentageConversionFactor = 100;
    final double bytesFailed = metrics.getBytesFailed().get();
    final double bytesSuccessful = metrics.getBytesSuccessful().get();
    final double operationsFailed = metrics.getOperationsFailed().get();
    final double operationsSuccessful = metrics.getOperationsSuccessful().get();
    final double errorPercentage = (bytesFailed <= 0)
        ? 0
        : percentageConversionFactor
        * bytesFailed
        / (bytesFailed + bytesSuccessful);

    double newSleepDuration;

    if (errorPercentage < MIN_ACCEPTABLE_ERROR_PERCENTAGE) {
      adfsNetworkTrafficAnalysisResult.incrementConsecutiveNoErrorCount();
      // Decrease sleepDuration in order to increase throughput.
      final double reductionFactor =
          (adfsNetworkTrafficAnalysisResult.getConsecutiveNoErrorCount() * analysisFrequencyInMs
              >= RAPID_SLEEP_DECREASE_TRANSITION_PERIOD_MS)
              ? RAPID_SLEEP_DECREASE_FACTOR
              : SLEEP_DECREASE_FACTOR;

      newSleepDuration = adfsNetworkTrafficAnalysisResult.getSleepDuration() * reductionFactor;
    } else if (errorPercentage < MAX_EQUILIBRIUM_ERROR_PERCENTAGE) {
      // Do not modify sleepDuration in order to stabilize throughput.
      newSleepDuration = adfsNetworkTrafficAnalysisResult.getSleepDuration();
    } else {
      // Increase sleepDuration in order to minimize error rate.
      adfsNetworkTrafficAnalysisResult.resetConsecutiveNoErrorCount();

      // Increase sleep duration in order to reduce throughput and error rate.
      // First, calculate target throughput: bytesSuccessful / periodMs.
      // Next, calculate time required to send *all* data (assuming next period
      // is similar to previous) at the target throughput: (bytesSuccessful
      // + bytesFailed) * periodMs / bytesSuccessful. Next, subtract periodMs to
      // get the total additional delay needed.
      double additionalDelayNeeded = 5 * analysisFrequencyInMs;
      if (bytesSuccessful > 0) {
        additionalDelayNeeded = (bytesSuccessful + bytesFailed)
            * timeElapsed
            / bytesSuccessful
            - timeElapsed;
      }

      // amortize the additional delay needed across the estimated number of
      // requests during the next period
      newSleepDuration = additionalDelayNeeded
          / (operationsFailed + operationsSuccessful);

      final double maxSleepDuration = analysisFrequencyInMs;
      final double minSleepDuration = adfsNetworkTrafficAnalysisResult.getSleepDuration() * SLEEP_INCREASE_FACTOR;

      // Add 1 ms to avoid rounding down and to decrease proximity to the server
      // side ingress/egress limit.  Ensure that the new sleep duration is
      // larger than the current one to more quickly reduce the number of
      // errors.  Don't allow the sleep duration to grow unbounded, after a
      // certain point throttling won't help, for example, if there are far too
      // many tasks/containers/nodes no amount of throttling will help.
      newSleepDuration = Math.max(newSleepDuration, minSleepDuration) + 1;
      newSleepDuration = Math.min(newSleepDuration, maxSleepDuration);
    }

    this.loggingService.debug(String.format(
          "%5.5s, %10d, %10d, %10d, %10d, %6.2f, %5d, %5d, %5d",
          accountName,
          (int) bytesFailed,
          (int) bytesSuccessful,
          (int) operationsFailed,
          (int) operationsSuccessful,
          errorPercentage,
          timeElapsed,
          (int) adfsNetworkTrafficAnalysisResult.getSleepDuration(),
          (int) newSleepDuration));

    adfsNetworkTrafficAnalysisResult.setSleepDuration((int) newSleepDuration);
    return adfsNetworkTrafficAnalysisResult;
  }

  class AnalysisBackgroundTask extends TimerTask {
    private final AtomicInteger doingWork;
    private final ConcurrentHashMap<String, AtomicReference<AdfsNetworkTrafficMetrics>> adfsNetworkTrafficMetricsCache;
    private final ConcurrentHashMap<String, AdfsNetworkTrafficAnalysisResultImpl> adfsNetworkTrafficAnalysisResultCache;
    private final int analysisFrequencyInMs;
    private final String accountName;

    AnalysisBackgroundTask(
        final String accountName,
        final ConcurrentHashMap<String, AtomicReference<AdfsNetworkTrafficMetrics>> adfsNetworkTrafficMetricsCache,
        final ConcurrentHashMap<String, AdfsNetworkTrafficAnalysisResultImpl> adfsNetworkTrafficAnalysisResultCache,
        final int analysisFrequencyInMs) {
      Preconditions.checkNotNull(adfsNetworkTrafficMetricsCache, "adfsNetworkTrafficMetricsCache");
      Preconditions.checkNotNull(adfsNetworkTrafficAnalysisResultCache, "adfsNetworkTrafficAnalysisResultCache");

      this.adfsNetworkTrafficMetricsCache = adfsNetworkTrafficMetricsCache;
      this.adfsNetworkTrafficAnalysisResultCache = adfsNetworkTrafficAnalysisResultCache;
      this.accountName = accountName;
      this.doingWork = new AtomicInteger(0);
      this.analysisFrequencyInMs = analysisFrequencyInMs;
    }

    @Override
    public void run() {
      boolean doWork = false;
      try {
        doWork = doingWork.compareAndSet(0, 1);

        // prevent concurrent execution of this task
        if (!doWork) {
          return;
        }

        final long now = System.currentTimeMillis();
        final AtomicReference<AdfsNetworkTrafficMetrics> adfsNetworkTrafficMetrics =
            this.adfsNetworkTrafficMetricsCache.get(this.accountName);
        final AdfsNetworkTrafficAnalysisResultImpl adfsNetworkTrafficAnalysisResult =
            this.adfsNetworkTrafficAnalysisResultCache.get(this.accountName);

        if (now - adfsNetworkTrafficMetrics.get().getStartTime() >= this.analysisFrequencyInMs) {
          final AdfsNetworkTrafficMetrics oldMetrics = adfsNetworkTrafficMetrics.getAndSet(
              new AdfsNetworkTrafficMetricsImpl(System.currentTimeMillis()));
          oldMetrics.end();

          final long timeElapsed = oldMetrics.getEndTime() - oldMetrics.getStartTime();

          analyzeMetricsAndUpdateSleepDuration(
              this.accountName,
              adfsNetworkTrafficAnalysisResult.getWriteAnalysisResult(),
              oldMetrics.getWriteMetrics(),
              timeElapsed,
              this.analysisFrequencyInMs);

          analyzeMetricsAndUpdateSleepDuration(
              this.accountName,
              adfsNetworkTrafficAnalysisResult.getReadAnalysisResult(),
              oldMetrics.getReadMetrics(),
              timeElapsed,
              this.analysisFrequencyInMs);
        }
      }
      finally {
        if (doWork) {
          doingWork.set(0);
        }
      }
    }
  }
}