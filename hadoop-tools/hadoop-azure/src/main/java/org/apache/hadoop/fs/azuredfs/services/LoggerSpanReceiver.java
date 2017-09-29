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

import java.io.IOException;

import com.google.common.base.Preconditions;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.azuredfs.contracts.exceptions.ServiceResolutionException;
import org.apache.hadoop.fs.azuredfs.contracts.log.LogLevel;
import org.apache.hadoop.fs.azuredfs.contracts.services.LoggingService;
import org.apache.htrace.core.HTraceConfiguration;
import org.apache.htrace.core.Span;
import org.apache.htrace.core.SpanReceiver;
import org.apache.htrace.fasterxml.jackson.core.JsonProcessingException;
import org.apache.htrace.fasterxml.jackson.databind.ObjectMapper;
import org.apache.htrace.fasterxml.jackson.databind.ObjectWriter;

/**
 * LoggerSpanReceiver is a layer between HTrace and log4j only used for {@link org.apache.hadoop.fs.azuredfs.contracts.services.TracingService}
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public final class LoggerSpanReceiver extends SpanReceiver {
  private static final ObjectWriter JSON_WRITER = new ObjectMapper().writer();
  private final LoggingService loggingService;

  public LoggerSpanReceiver(HTraceConfiguration hTraceConfiguration) throws ServiceResolutionException {
    Preconditions.checkNotNull(hTraceConfiguration, "hTraceConfiguration");
    this.loggingService = ServiceProviderImpl.instance().get(LoggingService.class);
  }

  @Override
  public void receiveSpan(Span span) {
    String jsonValue = null;
    try {
      jsonValue = JSON_WRITER.writeValueAsString(span);
      loggingService.log(LogLevel.Trace, jsonValue);
    } catch (JsonProcessingException e) {
      loggingService.log(LogLevel.Error, "Json processing error: " + e.getMessage());
    }
  }

  @Override
  public void close() throws IOException {
    // No-Op
  }
}