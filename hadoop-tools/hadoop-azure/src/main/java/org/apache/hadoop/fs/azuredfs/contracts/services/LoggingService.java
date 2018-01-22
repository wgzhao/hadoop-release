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

package org.apache.hadoop.fs.azuredfs.contracts.services;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.azuredfs.contracts.log.LogLevel;

/**
 * Azure Distributed File System logging service.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public interface LoggingService extends InjectableService {
  /**
   * Returns the logger with proper name.
   * @param loggingClass logging class.
   * @return proper logger
   */
  LoggingService get(Class loggingClass);

  /**
   * Logs the provided message with the provided logLevel to Hadoop logging system.
   * @param logLevel the log level {@link LogLevel}
   * @param message the message to be logged.
   * @param arguments if message is formatted, the values must be provided as arguments.
   */
  void log(LogLevel logLevel, String message, Object... arguments);

  /**
   * Returns true whether a certain logLevel is enabled.
   * @param logLevel
   * @return true if provided log level is enabled.
   */
  boolean logLevelEnabled(LogLevel logLevel);

  /**
   * Logs the provided debug message to Hadoop logging system.
   * @param message the message to be logged.
   * @param arguments if message is formatted, the values must be provided as arguments.
   */
  void debug(String message, Object... arguments);

  /**
   * Logs the provided info message to Hadoop logging system.
   * @param message the message to be logged.
   * @param arguments if message is formatted, the values must be provided as arguments.
   */
  void info(String message, Object... arguments);

  /**
   * Logs the provided warning message to Hadoop logging system.
   * @param message the message to be logged.
   * @param arguments if message is formatted, the values must be provided as arguments.
   */
  void warning(String message, Object... arguments);

  /**
   * Logs the provided error message to Hadoop logging system.
   * @param message the message to be logged.
   * @param arguments if message is formatted, the values must be provided as arguments.
   */
  void error(String message, Object... arguments);

  /**
   * Logs the provided trace message to Hadoop logging system.
   * @param message the message to be logged.
   * @param arguments if message is formatted, the values must be provided as arguments.
   */
  void trace(String message, Object... arguments);
}