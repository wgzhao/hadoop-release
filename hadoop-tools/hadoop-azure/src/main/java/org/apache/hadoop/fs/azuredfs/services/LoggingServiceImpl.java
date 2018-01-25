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

import java.text.MessageFormat;

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
import org.apache.htrace.fasterxml.jackson.core.JsonProcessingException;
import org.apache.htrace.fasterxml.jackson.databind.ObjectMapper;
import org.apache.htrace.fasterxml.jackson.databind.ObjectWriter;
import org.apache.htrace.fasterxml.jackson.databind.SerializationFeature;

@Singleton
@InterfaceAudience.Private
@InterfaceStability.Evolving
final class LoggingServiceImpl implements LoggingService {
  private final Logger logger;
  private boolean traceEnabled;
  private boolean debugEnabled;
  private boolean warningEnabled;
  private boolean errorEnabled;
  private boolean infoEnabled;

  private static final ObjectWriter JSON_WRITER =
      new ObjectMapper()
          .configure(SerializationFeature.WRITE_BIGDECIMAL_AS_PLAIN, true)
          .configure(SerializationFeature.WRITE_EMPTY_JSON_ARRAYS, false)
          .configure(SerializationFeature.USE_EQUALITY_FOR_OBJECT_ID, false)
          .writer();

  @Inject
  LoggingServiceImpl() {
    this(LoggerFactory.getLogger(LoggingService.class));
  }

  @VisibleForTesting
  LoggingServiceImpl(final Logger logger) {
    Preconditions.checkNotNull(logger, "logger");
    this.logger = logger;
    this.cacheState();
  }

  @Override
  public LoggingService get(final Class loggingClass) {
    Preconditions.checkNotNull(loggingClass, "loggingClass");
    Logger logger = LoggerFactory.getLogger(loggingClass);
    return new LoggingServiceImpl(logger);
  }

  @Override
  public void log(final LogLevel logLevel, final String message, final Object... arguments) {
    switch (logLevel) {
      case Trace:
        this.trace(message, arguments);
        break;
      case Debug:
        this.debug(message, arguments);
        break;
      case Warning:
        this.warning(message, arguments);
        break;
      case Error:
        this.error(message, arguments);
        break;
      case Info:
        this.info(message, arguments);
        break;
      default:
        throw new AssertionError("Can't get here.");
    }
  }

  @Override
  public boolean logLevelEnabled(final LogLevel logLevel) {
    switch (logLevel) {
      case Trace:
        return this.traceEnabled;
      case Debug:
        return this.debugEnabled;
      case Warning:
        return this.warningEnabled;
      case Error:
        return this.errorEnabled;
      case Info:
        return this.infoEnabled;
      default:
        throw new AssertionError("Can't get here.");
    }
  }

  public void debug(final String message, final Object... arguments) {
    if (debugEnabled) {
      final String formattedMessage = ensureMessageFormatted(message, arguments);
      this.logger.debug(formattedMessage);
    }
  }

  public void info(final String message, final Object... arguments) {
    if (infoEnabled) {
      final String formattedMessage = ensureMessageFormatted(message, arguments);
      this.logger.info(formattedMessage);
    }
  }

  public void warning(final String message, final Object... arguments) {
    if (warningEnabled) {
      final String formattedMessage = ensureMessageFormatted(message, arguments);
      this.logger.warn(formattedMessage);
    }
  }

  public void error(final String message, final Object... arguments) {
    if (errorEnabled) {
      final String formattedMessage = ensureMessageFormatted(message, arguments);
      this.logger.error(formattedMessage);
    }
  }

  public void trace(final String message, final Object... arguments) {
    if (traceEnabled) {
      final String formattedMessage = ensureMessageFormatted(message, arguments);
      this.logger.trace(formattedMessage);
    }
  }

  private void cacheState() {
    this.debugEnabled = this.logger.isDebugEnabled();
    this.traceEnabled = this.logger.isTraceEnabled();
    this.warningEnabled = this.logger.isWarnEnabled();
    this.errorEnabled = this.logger.isErrorEnabled();
    this.infoEnabled = this.logger.isInfoEnabled();
  }

  private String ensureMessageFormatted(final String message, final Object... arguments) {
    String formatted = message;
    if (arguments != null && arguments.length > 0) {
      String[] convertedArguments = new String[arguments.length];
      for (int i = 0; i < arguments.length; i++) {
        try {
          String convertedArgument = JSON_WRITER.writeValueAsString(arguments[i]);
          convertedArguments[i] = convertedArgument;
        }
        catch (JsonProcessingException e) {
          String errorMessage = "Json processing error: " + e.getMessage();
          this.error(errorMessage);

          throw new IllegalStateException(errorMessage);
        }
      }

      formatted = MessageFormat.format(message, convertedArguments);
    }

    return formatted;
  }
}