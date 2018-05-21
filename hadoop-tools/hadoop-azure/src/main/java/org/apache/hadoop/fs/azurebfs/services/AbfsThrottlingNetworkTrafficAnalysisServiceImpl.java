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
import org.apache.hadoop.fs.azurebfs.contracts.services.AbfsHttpClientSession;
import org.apache.hadoop.fs.azurebfs.contracts.services.AbfsThrottlingNetworkThroughputAnalysisResult;
import org.apache.hadoop.fs.azurebfs.contracts.services.AbfsThrottlingNetworkTrafficAnalysisResult;
import org.apache.hadoop.fs.azurebfs.contracts.services.AbfsThrottlingNetworkThroughputMetrics;
import org.apache.hadoop.fs.azurebfs.contracts.services.AbfsThrottlingNetworkTrafficAnalysisService;
import org.apache.hadoop.fs.azurebfs.contracts.services.AbfsThrottlingNetworkTrafficMetrics;
import org.apache.hadoop.fs.azurebfs.contracts.services.LoggingService;

@Singleton
@InterfaceAudience.Private
@InterfaceStability.Evolving
final class AbfsThrottlingNetworkTrafficAnalysisServiceImpl implements AbfsThrottlingNetworkTrafficAnalysisService {
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
  private final ConcurrentHashMap<String, AtomicReference<AbfsThrottlingNetworkTrafficMetrics>> abfsThrottlingNetworkTrafficMetricsCache;
  private final ConcurrentHashMap<String, AbfsThrottlingNetworkTrafficAnalysisResultImpl> abfsThrottlingNetworkTrafficAnalysisResultCache;
  private final ConcurrentHashMap<String, Timer> abfsThrottlingNetworkTrafficAnalysisTimers;
  private final ConcurrentHashMap<String, ConcurrentArrayList<AbfsHttpClientSession>> registeredAbfsClientSessions;

  @Inject
  AbfsThrottlingNetworkTrafficAnalysisServiceImpl(
      final LoggingService loggingService) {
    Preconditions.checkNotNull(loggingService, "loggingService");

    this.loggingService = loggingService.get(AbfsThrottlingNetworkTrafficAnalysisService.class);
    this.abfsThrottlingNetworkTrafficMetricsCache = new ConcurrentHashMap<>();
    this.abfsThrottlingNetworkTrafficAnalysisResultCache = new ConcurrentHashMap<>();
    this.abfsThrottlingNetworkTrafficAnalysisTimers = new ConcurrentHashMap<>();
    this.registeredAbfsClientSessions = new ConcurrentHashMap<>();
  }

  @Override
  public synchronized void subscribeForAnalysis(AbfsHttpClientSession abfsHttpClientSession) {
    this.subscribeForAnalysis(abfsHttpClientSession, DEFAULT_ANALYSIS_PERIOD_MS);
  }

  @Override
  public synchronized void subscribeForAnalysis(final AbfsHttpClientSession abfsHttpClientSession, final int analysisFrequencyInMs) {
    Preconditions.checkArgument(
        analysisFrequencyInMs >= MIN_ANALYSIS_PERIOD_MS && analysisFrequencyInMs <= MAX_ANALYSIS_PERIOD_MS,
        "The argument 'period' must be between 1000 and 30000.");

    final String accountName = abfsHttpClientSession.getStorageCredentialsAccountAndKey().getAccountName();

    if (this.abfsThrottlingNetworkTrafficAnalysisResultCache.get(accountName) != null
        && this.abfsThrottlingNetworkTrafficMetricsCache.get(accountName) != null
        && this.abfsThrottlingNetworkTrafficAnalysisTimers.get(accountName) != null
        && this.registeredAbfsClientSessions.get(accountName) != null) {

      ConcurrentArrayList<AbfsHttpClientSession> registeredSessions = this.registeredAbfsClientSessions.get(accountName);
      if (!registeredSessions.contains(abfsHttpClientSession)) {
        registeredSessions.add(abfsHttpClientSession);
      }

      return;
    }

    if (this.abfsThrottlingNetworkTrafficAnalysisResultCache.get(accountName) != null
        || this.abfsThrottlingNetworkTrafficMetricsCache.get(accountName) != null
        || this.abfsThrottlingNetworkTrafficAnalysisTimers.get(accountName) != null
        || this.registeredAbfsClientSessions.get(accountName) != null) {
      throw new IllegalStateException("Corrupted state");
    }

    final AtomicReference<AbfsThrottlingNetworkTrafficMetrics> networkTrafficMetrics =
        new AtomicReference<AbfsThrottlingNetworkTrafficMetrics>(new AbfsThrottlingNetworkTrafficMetricsImpl(System.currentTimeMillis()));

    final AbfsThrottlingNetworkTrafficAnalysisResultImpl networkTrafficAnalysisResult =
        new AbfsThrottlingNetworkTrafficAnalysisResultImpl();

    this.abfsThrottlingNetworkTrafficAnalysisResultCache.put(accountName, networkTrafficAnalysisResult);
    this.abfsThrottlingNetworkTrafficMetricsCache.put(accountName, networkTrafficMetrics);

    final Timer timer = new Timer(String.format(accountName + "-AbfsThrottlingNetworkTrafficAnalysisServiceImplTimer"), true);
    final AnalysisBackgroundTask task = new AnalysisBackgroundTask(
        accountName,
        this.abfsThrottlingNetworkTrafficMetricsCache,
        this.abfsThrottlingNetworkTrafficAnalysisResultCache,
        analysisFrequencyInMs);

    timer.schedule(
        task,
        analysisFrequencyInMs,
        analysisFrequencyInMs);

    this.abfsThrottlingNetworkTrafficAnalysisTimers.put(accountName, timer);

    ConcurrentArrayList<AbfsHttpClientSession> subscribedSessions = new ConcurrentArrayList<>();
    subscribedSessions.add(abfsHttpClientSession);

    this.registeredAbfsClientSessions.put(accountName, subscribedSessions);
  }

  @Override
  public synchronized void unsubscribeFromAnalysis(final AbfsHttpClientSession abfsHttpClientSession) {
    final String accountName = abfsHttpClientSession.getStorageCredentialsAccountAndKey().getAccountName();

    final ConcurrentArrayList<AbfsHttpClientSession> subscribedSessions = this.registeredAbfsClientSessions.get(accountName);

    if (subscribedSessions == null) {
      return;
    }

    if (subscribedSessions.contains(abfsHttpClientSession)) {
      subscribedSessions.remove(abfsHttpClientSession);
    }

    if (subscribedSessions.size() != 0) {
      return;
    }

    final Timer timer = this.abfsThrottlingNetworkTrafficAnalysisTimers.remove(accountName);

    if (timer == null) {
      return;
    }

    timer.cancel();

    this.abfsThrottlingNetworkTrafficAnalysisResultCache.remove(accountName);
    this.abfsThrottlingNetworkTrafficMetricsCache.remove(accountName);
    this.registeredAbfsClientSessions.remove(accountName);
  }

  @Override
  public AbfsThrottlingNetworkTrafficMetrics getAbfsThrottlingNetworkThroughputMetrics(AbfsHttpClientSession abfsHttpClientSession) {
    final String accountName = abfsHttpClientSession.getStorageCredentialsAccountAndKey().getAccountName();
    return abfsThrottlingNetworkTrafficMetricsCache.get(accountName).get();
  }

  @Override
  public AbfsThrottlingNetworkTrafficAnalysisResult getAbfsThrottlingNetworkTrafficAnalysisResult(AbfsHttpClientSession abfsHttpClientSession) {
    final String accountName = abfsHttpClientSession.getStorageCredentialsAccountAndKey().getAccountName();
    return abfsThrottlingNetworkTrafficAnalysisResultCache.get(accountName);
  }

  private AbfsThrottlingNetworkThroughputAnalysisResult analyzeMetricsAndUpdateSleepDuration(
      final String accountName,
      final AbfsThrottlingNetworkThroughputAnalysisResult abfsThrottlingNetworkTrafficAnalysisResult,
      final AbfsThrottlingNetworkThroughputMetrics metrics,
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
      abfsThrottlingNetworkTrafficAnalysisResult.incrementConsecutiveNoErrorCount();
      // Decrease sleepDuration in order to increase throughput.
      final double reductionFactor =
          (abfsThrottlingNetworkTrafficAnalysisResult.getConsecutiveNoErrorCount() * analysisFrequencyInMs
              >= RAPID_SLEEP_DECREASE_TRANSITION_PERIOD_MS)
              ? RAPID_SLEEP_DECREASE_FACTOR
              : SLEEP_DECREASE_FACTOR;

      newSleepDuration = abfsThrottlingNetworkTrafficAnalysisResult.getSleepDuration() * reductionFactor;
    } else if (errorPercentage < MAX_EQUILIBRIUM_ERROR_PERCENTAGE) {
      // Do not modify sleepDuration in order to stabilize throughput.
      newSleepDuration = abfsThrottlingNetworkTrafficAnalysisResult.getSleepDuration();
    } else {
      // Increase sleepDuration in order to minimize error rate.
      abfsThrottlingNetworkTrafficAnalysisResult.resetConsecutiveNoErrorCount();

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
      final double minSleepDuration = abfsThrottlingNetworkTrafficAnalysisResult.getSleepDuration() * SLEEP_INCREASE_FACTOR;

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
          (int) abfsThrottlingNetworkTrafficAnalysisResult.getSleepDuration(),
          (int) newSleepDuration));

    abfsThrottlingNetworkTrafficAnalysisResult.setSleepDuration((int) newSleepDuration);
    return abfsThrottlingNetworkTrafficAnalysisResult;
  }

  class AnalysisBackgroundTask extends TimerTask {
    private final AtomicInteger doingWork;
    private final ConcurrentHashMap<String, AtomicReference<AbfsThrottlingNetworkTrafficMetrics>> abfsThrottlingNetworkTrafficMetricsCache;
    private final ConcurrentHashMap<String, AbfsThrottlingNetworkTrafficAnalysisResultImpl> abfsThrottlingNetworkTrafficAnalysisResultCache;
    private final int analysisFrequencyInMs;
    private final String accountName;

    AnalysisBackgroundTask(
        final String accountName,
        final ConcurrentHashMap<String, AtomicReference<AbfsThrottlingNetworkTrafficMetrics>> abfsThrottlingNetworkTrafficMetricsCache,
        final ConcurrentHashMap<String, AbfsThrottlingNetworkTrafficAnalysisResultImpl> abfsThrottlingNetworkTrafficAnalysisResultCache,
        final int analysisFrequencyInMs) {
      Preconditions.checkNotNull(abfsThrottlingNetworkTrafficMetricsCache, "abfsThrottlingNetworkTrafficMetricsCache");
      Preconditions.checkNotNull(abfsThrottlingNetworkTrafficAnalysisResultCache, "abfsThrottlingNetworkTrafficAnalysisResultCache");

      this.abfsThrottlingNetworkTrafficMetricsCache = abfsThrottlingNetworkTrafficMetricsCache;
      this.abfsThrottlingNetworkTrafficAnalysisResultCache = abfsThrottlingNetworkTrafficAnalysisResultCache;
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
        final AtomicReference<AbfsThrottlingNetworkTrafficMetrics> abfsThrottlingNetworkTrafficMetrics =
            this.abfsThrottlingNetworkTrafficMetricsCache.get(this.accountName);
        final AbfsThrottlingNetworkTrafficAnalysisResultImpl abfsThrottlingNetworkTrafficAnalysisResult =
            this.abfsThrottlingNetworkTrafficAnalysisResultCache.get(this.accountName);

        if (now - abfsThrottlingNetworkTrafficMetrics.get().getStartTime() >= this.analysisFrequencyInMs) {
          final AbfsThrottlingNetworkTrafficMetrics oldMetrics = abfsThrottlingNetworkTrafficMetrics.getAndSet(
              new AbfsThrottlingNetworkTrafficMetricsImpl(System.currentTimeMillis()));
          oldMetrics.end();

          final long timeElapsed = oldMetrics.getEndTime() - oldMetrics.getStartTime();

          analyzeMetricsAndUpdateSleepDuration(
              this.accountName,
              abfsThrottlingNetworkTrafficAnalysisResult.getWriteAnalysisResult(),
              oldMetrics.getWriteMetrics(),
              timeElapsed,
              this.analysisFrequencyInMs);

          analyzeMetricsAndUpdateSleepDuration(
              this.accountName,
              abfsThrottlingNetworkTrafficAnalysisResult.getReadAnalysisResult(),
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