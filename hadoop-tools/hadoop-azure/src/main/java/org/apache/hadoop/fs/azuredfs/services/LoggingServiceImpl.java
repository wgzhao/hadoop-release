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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.azuredfs.contracts.log.LogLevel;
import org.apache.hadoop.fs.azuredfs.contracts.services.LoggingService;

@Singleton
@InterfaceAudience.Private
@InterfaceStability.Evolving
final class LoggingServiceImpl implements LoggingService {
  private final Logger logger;

  @Inject
  LoggingServiceImpl() {
    this.logger = LoggerFactory.getLogger(LoggingService.class);
  }

  @VisibleForTesting
  LoggingServiceImpl(final Logger logger) {
    Preconditions.checkNotNull("logger", logger);
    this.logger = logger;
  }

  @Override
  public void log(final LogLevel logLevel, final String message, final String... arguments) {
    if (logLevel == LogLevel.Debug) {
      this.logDebug(message, arguments);
    } else if (logLevel == LogLevel.Info) {
      this.logInfo(message, arguments);
    } else if (logLevel == LogLevel.Trace) {
      this.logTrace(message, arguments);
    } else if (logLevel == LogLevel.Warning) {
      this.logWarning(message, arguments);
    } else if (logLevel == LogLevel.Error) {
      this.logError(message, arguments);
    }
  }

  @Override
  public boolean logLevelEnabled(LogLevel logLevel) {
    if (logLevel == LogLevel.Debug) {
      return this.logger.isDebugEnabled();
    } else if (logLevel == LogLevel.Info) {
      return this.logger.isInfoEnabled();
    } else if (logLevel == LogLevel.Trace) {
      return this.logger.isTraceEnabled();
    } else if (logLevel == LogLevel.Warning) {
      return this.logger.isWarnEnabled();
    } else if (logLevel == LogLevel.Error) {
      return this.logger.isErrorEnabled();
    }

    return false;
  }

  private void logDebug(final String message, final String... arguments) {
    if (logger.isDebugEnabled()) {
      final String formattedMessage = ensureMessageFormatted(message, arguments);
      this.logger.debug(formattedMessage);
    }
  }

  private void logInfo(final String message, final String... arguments) {
    if (logger.isInfoEnabled()) {
      final String formattedMessage = ensureMessageFormatted(message, arguments);
      this.logger.info(formattedMessage);
    }
  }

  private void logWarning(final String message, final String... arguments) {
    if (logger.isWarnEnabled()) {
      final String formattedMessage = ensureMessageFormatted(message, arguments);
      this.logger.warn(formattedMessage);
    }
  }

  private void logError(final String message, final String... arguments) {
    if (logger.isErrorEnabled()) {
      final String formattedMessage = ensureMessageFormatted(message, arguments);
      this.logger.error(formattedMessage);
    }
  }

  private void logTrace(final String message, final String... arguments) {
    if (logger.isTraceEnabled()) {
      final String formattedMessage = ensureMessageFormatted(message, arguments);
      this.logger.trace(formattedMessage);
    }
  }

  private String ensureMessageFormatted(final String message, final String... arguments) {
    String formatted = message;
    if (arguments != null && arguments.length > 0) {
      formatted = String.format(message, arguments);
    }

    return formatted;
  }
}