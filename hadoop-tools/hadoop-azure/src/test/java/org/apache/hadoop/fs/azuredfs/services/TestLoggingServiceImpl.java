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

import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;
import org.slf4j.Logger;

import org.apache.hadoop.fs.azuredfs.contracts.log.LogLevel;

import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.*;

public class TestLoggingServiceImpl {
  @Test
  public void ensureLogsAreNotLoggedWhenDisabled() throws Exception {
    Logger logger = AdfsLoggingTestUtils.createMockLogger(false);
    LoggingServiceImpl azureLoggingService = new LoggingServiceImpl(logger);

    verifyAllLogLevelsDisabled(logger);

    for (LogLevel logLevel : LogLevel.values()) {
      azureLoggingService.log(logLevel, "message");

      verify(logger, never()).info(anyString());
      verify(logger, never()).debug(anyString());
      verify(logger, never()).warn(anyString());
      verify(logger, never()).trace(anyString());
      verify(logger, never()).error(anyString());

      Mockito.reset(logger);
    }
  }

  @Test
  public void ensureLogsAreLoggedWhenEnabled() throws Exception {
    Logger logger = AdfsLoggingTestUtils.createMockLogger(true);
    LoggingServiceImpl azureLoggingService = new LoggingServiceImpl(logger);

    verifyAllLogLevelsEnabled(logger);

    for (LogLevel logLevel : LogLevel.values()) {
      azureLoggingService.log(logLevel, logLevel.toString());

      if (logLevel == LogLevel.Info) {
        Mockito.verify(logger).info(logLevel.toString());
      }
      else if (logLevel == LogLevel.Debug) {
        Mockito.verify(logger).debug(logLevel.toString());
      }
      else if (logLevel == LogLevel.Warning) {
        Mockito.verify(logger).warn(logLevel.toString());
      }
      else if (logLevel == LogLevel.Trace) {
        Mockito.verify(logger).trace(logLevel.toString());
      }
      else if (logLevel == LogLevel.Error) {
        Mockito.verify(logger).error(logLevel.toString());
      }

      Mockito.reset(logger);
    }
  }

  private void verifyAllLogLevelsEnabled(Logger logger) {
    assertLogLevelsToBe(logger, true);
  }

  private void verifyAllLogLevelsDisabled(Logger logger) {
    assertLogLevelsToBe(logger, false);
  }

  private void assertLogLevelsToBe(Logger logger, boolean enabled) {
    Assert.assertTrue(logger.isInfoEnabled() == enabled);
    Assert.assertTrue(logger.isDebugEnabled() == enabled);
    Assert.assertTrue(logger.isErrorEnabled() == enabled);
    Assert.assertTrue(logger.isTraceEnabled() == enabled);
    Assert.assertTrue(logger.isWarnEnabled() == enabled);
    Assert.assertTrue(logger.isErrorEnabled() == enabled);
  }
}
