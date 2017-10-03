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

import org.apache.hadoop.fs.azuredfs.contracts.log.LogLevel;
import org.apache.hadoop.fs.azuredfs.services.Mocks.MockLogger;

public class LoggingServiceImplTests {
  @Test
  public void ensureLogsAreNotLoggedWhenDisabled() throws Exception {
    MockLogger logger = new MockLogger(false);
    LoggingServiceImpl azureLoggingService = new LoggingServiceImpl(logger);

    verifyAllLogLevelsDisabled(logger);

    for (LogLevel logLevel : LogLevel.values()) {
      azureLoggingService.log(logLevel, "");
      Assert.assertEquals(logger.lastMessage, null);
      Assert.assertEquals(logger.lastLogLevel, null);
    }
  }

  @Test
  public void ensureLogsAreLoggedWhenEnabled() throws Exception {
    MockLogger logger = new MockLogger(true);
    LoggingServiceImpl azureLoggingService = new LoggingServiceImpl(logger);

    verifyAllLogLevelsEnabled(logger);

    for (LogLevel logLevel : LogLevel.values()) {
      azureLoggingService.log(logLevel, logLevel.toString());
      Assert.assertEquals(logger.lastMessage, logLevel.toString());
      Assert.assertEquals(logger.lastLogLevel, logLevel);
    }
  }

  private void verifyAllLogLevelsEnabled(MockLogger logger) {
    assertLogLevelsToBe(logger, true);
  }

  private void verifyAllLogLevelsDisabled(MockLogger logger) {
    assertLogLevelsToBe(logger, false);
  }

  private void assertLogLevelsToBe(MockLogger logger, boolean enabled) {
    Assert.assertTrue(logger.isInfoEnabled() == enabled);
    Assert.assertTrue(logger.isDebugEnabled() == enabled);
    Assert.assertTrue(logger.isErrorEnabled() == enabled);
    Assert.assertTrue(logger.isTraceEnabled() == enabled);
    Assert.assertTrue(logger.isWarnEnabled() == enabled);
    Assert.assertTrue(logger.isErrorEnabled() == enabled);
  }
}
