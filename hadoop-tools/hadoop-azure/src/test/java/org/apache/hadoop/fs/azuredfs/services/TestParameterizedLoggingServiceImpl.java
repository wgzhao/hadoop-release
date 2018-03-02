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

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.mockito.Mockito;
import org.slf4j.Logger;

import org.apache.hadoop.fs.azuredfs.contracts.log.LogLevel;

@RunWith(Parameterized.class)
public class TestParameterizedLoggingServiceImpl {
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

  public TestParameterizedLoggingServiceImpl(LogLevel logLevel, String logMessage) {
    this.logLevel = logLevel;
    this.logMessage = logMessage;
  }

  @Test
  public void verifyLoggingTests() throws Exception {
    Logger logger = AdfsLoggingTestUtils.createMockLogger(true);
    LoggingServiceImpl azureLoggingService = new LoggingServiceImpl(logger);
    azureLoggingService.log(this.logLevel, this.logMessage);
    AdfsLoggingTestUtils.ensureCalledWithValue(logger, logLevel, this.logMessage);
    Mockito.reset(logger);
  }

  @Test
  public void verifyOnlyOneLoggerTests() throws Exception {
    Logger logger = AdfsLoggingTestUtils.createMockLogger(this.logLevel);
    LoggingServiceImpl azureLoggingService = new LoggingServiceImpl(logger);

    for (LogLevel logLevel : LogLevel.values()) {
      if (logLevel != this.logLevel) {
        azureLoggingService.log(logLevel, this.logMessage);
        AdfsLoggingTestUtils.ensureOnlyLogLevelCall(logger, logLevel);
      }
      else {
        azureLoggingService.log(logLevel, this.logMessage);
        AdfsLoggingTestUtils.ensureCalledWithValue(logger, logLevel, this.logMessage);
      }

      Mockito.reset(logger);
    }
  }
}
