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

import java.util.ArrayList;
import java.util.List;

import org.junit.Assert;
import org.mockito.Matchers;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.slf4j.Logger;

import org.apache.commons.lang.ArrayUtils;
import org.apache.hadoop.fs.azuredfs.contracts.log.LogLevel;
import org.apache.hadoop.fs.azuredfs.contracts.services.LoggingService;

import static org.mockito.Matchers.anyObject;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.*;
import static org.mockito.Mockito.doAnswer;

public final class AdfsLoggingTestUtils {
  public static Logger createMockLogger(final boolean enableLogging) {
    Logger mockLogger = Mockito.mock(Logger.class);

    when(mockLogger.isDebugEnabled()).thenReturn(enableLogging);
    when(mockLogger.isTraceEnabled()).thenReturn(enableLogging);
    when(mockLogger.isInfoEnabled()).thenReturn(enableLogging);
    when(mockLogger.isWarnEnabled()).thenReturn(enableLogging);
    when(mockLogger.isErrorEnabled()).thenReturn(enableLogging);

    return mockLogger;
  }

  public static Logger createMockLogger(final LogLevel logLevel) {
    Logger mockLogger = Mockito.mock(Logger.class);

    when(mockLogger.isDebugEnabled()).thenReturn(logLevel == LogLevel.Debug);
    when(mockLogger.isTraceEnabled()).thenReturn(logLevel == LogLevel.Trace);
    when(mockLogger.isInfoEnabled()).thenReturn(logLevel == LogLevel.Info);
    when(mockLogger.isWarnEnabled()).thenReturn(logLevel == LogLevel.Warning);
    when(mockLogger.isErrorEnabled()).thenReturn(logLevel == LogLevel.Error);

    return mockLogger;
  }

  public static void ensureCalledWithValue(Logger logger, LogLevel logLevel, String logMessage) {
    if (logLevel == LogLevel.Info) {
      Mockito.verify(logger).info(logMessage);
    }
    else if (logLevel == LogLevel.Debug) {
      Mockito.verify(logger).debug(logMessage);
    }
    else if (logLevel == LogLevel.Warning) {
      Mockito.verify(logger).warn(logMessage);
    }
    else if (logLevel == LogLevel.Trace) {
      Mockito.verify(logger).trace(logMessage);
    }
    else if (logLevel == LogLevel.Error) {
      Mockito.verify(logger).error(logMessage);
    }
    else {
      Assert.fail();
    }
  }

  public static void ensureOnlyLogLevelCall(Logger logger, LogLevel excludedLogLevel) {
    LogLevel[] logLevels = LogLevel.values();
    logLevels = (LogLevel[]) ArrayUtils.removeElement(logLevels, excludedLogLevel);

    for (LogLevel logLevel : logLevels) {
      if (logLevel == LogLevel.Info) {
        verify(logger, never()).info(anyString());
      }
      else if (logLevel == LogLevel.Debug) {
        verify(logger, never()).debug(anyString());
      }
      else if (logLevel == LogLevel.Warning) {
        verify(logger, never()).warn(anyString());
      }
      else if (logLevel == LogLevel.Trace) {
        verify(logger, never()).trace(anyString());
      }
      else if (logLevel == LogLevel.Error) {
        verify(logger, never()).error(anyString());
      } else {
        Assert.fail();
      }
    }
  }

  public static LoggingService createMockLoggingService(final List<String> messageStorage) {
    LoggingService mockLoggingService = Mockito.mock(LoggingService.class);

    when(mockLoggingService.get((Class) anyObject())).thenReturn(mockLoggingService);
    when(mockLoggingService.logLevelEnabled((LogLevel) anyObject())).thenReturn(true);

    Answer answer = new Answer() {
      @Override
      public Object answer(InvocationOnMock invocationOnMock) throws Throwable {
        Object message = invocationOnMock.getArguments()[0];
        return messageStorage.add((String) message);
      }
    };

    doAnswer(answer).when(mockLoggingService).log((LogLevel) anyObject(), anyString(), Matchers.anyVararg());
    doAnswer(answer).when(mockLoggingService).debug(anyString(), Matchers.anyVararg());
    doAnswer(answer).when(mockLoggingService).info(anyString(), Matchers.anyVararg());
    doAnswer(answer).when(mockLoggingService).warning(anyString(), Matchers.anyVararg());
    doAnswer(answer).when(mockLoggingService).error(anyString(), Matchers.anyVararg());
    doAnswer(answer).when(mockLoggingService).trace(anyString(), Matchers.anyVararg());

    return mockLoggingService;
  }

  public static LoggingService createMockLoggingService() {
    return createMockLoggingService(new ArrayList<String>());
  }
}
