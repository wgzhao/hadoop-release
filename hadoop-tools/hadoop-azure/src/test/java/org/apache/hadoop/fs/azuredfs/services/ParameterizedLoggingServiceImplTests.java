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

import java.util.Arrays;
import java.util.Collection;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import org.apache.hadoop.fs.azuredfs.contracts.log.LogLevel;

@RunWith(Parameterized.class)
public class ParameterizedLoggingServiceImplTests {
  private LogLevel logLevel;
  private String logMessage;

  @Parameterized.Parameters
  public static Collection<Object[]> params() {
    return Arrays.asList(new Object[][] {
        { LogLevel.Info, "Test string" },
        { LogLevel.Debug, "Test string" },
        { LogLevel.Trace, "Test string" },
        { LogLevel.Warning, "Test string" },
        { LogLevel.Error, "Test string" },
    });
  }

  public ParameterizedLoggingServiceImplTests(LogLevel logLevel, String logMessage) {
    this.logLevel = logLevel;
    this.logMessage = logMessage;
  }

  @Test
  public void verifyLoggingTests() throws Exception {
    MockLogger logger = new MockLogger(true);
    LoggingServiceImpl azureLoggingService = new LoggingServiceImpl(logger);
    azureLoggingService.log(this.logLevel, this.logMessage);
    Assert.assertTrue(logger.lastMessage.contains(this.logMessage));
    Assert.assertEquals(this.logLevel, logger.lastLogLevel);
  }

  @Test
  public void verifyOnlyOneLoggerTests() throws Exception {
    MockLogger logger = new MockLogger(this.logLevel);
    LoggingServiceImpl azureLoggingService = new LoggingServiceImpl(logger);

    for (LogLevel logLevel : LogLevel.values()) {
      logger.lastMessage = null;
      logger.lastLogLevel = null;

      if (logLevel != this.logLevel) {
        azureLoggingService.log(logLevel, this.logMessage);
        Assert.assertNull(logger.lastMessage);
        Assert.assertNull(logger.lastLogLevel);
      }
      else {
        azureLoggingService.log(logLevel, this.logMessage);
        Assert.assertTrue(logger.lastMessage.contains(this.logMessage));
        Assert.assertEquals(this.logLevel, logger.lastLogLevel);
      }
    }
  }
}
