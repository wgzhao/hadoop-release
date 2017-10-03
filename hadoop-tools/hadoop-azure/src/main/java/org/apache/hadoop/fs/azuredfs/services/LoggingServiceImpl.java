

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
    Preconditions.checkNotNull(logger, "logger");
    this.logger = logger;
  }

  @Override
  public void log(final LogLevel logLevel, final String message, final String... arguments) {
    switch (logLevel) {
      case Trace:
        this.logTrace(message, arguments);
        break;
      case Debug:
        this.logDebug(message, arguments);
        break;
      case Warning:
        this.logWarning(message, arguments);
        break;
      case Error:
        this.logError(message, arguments);
        break;
      case Info:
        this.logInfo(message, arguments);
        break;
      default:
        throw new AssertionError("Can't get here.");
    }
  }

  @Override
  public boolean logLevelEnabled(LogLevel logLevel) {
    switch (logLevel) {
      case Trace:
        return this.logger.isTraceEnabled();
      case Debug:
        return this.logger.isDebugEnabled();
      case Warning:
        return this.logger.isWarnEnabled();
      case Error:
        return this.logger.isErrorEnabled();
      case Info:
        return this.logger.isInfoEnabled();
      default:
        throw new AssertionError("Can't get here.");
    }
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