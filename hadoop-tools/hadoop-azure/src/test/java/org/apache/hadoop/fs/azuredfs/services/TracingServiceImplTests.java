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

import org.junit.Test;

import org.apache.hadoop.fs.azuredfs.DependencyInjectedTest;
import org.apache.hadoop.fs.azuredfs.contracts.services.LoggingService;
import org.apache.hadoop.fs.azuredfs.contracts.services.TracingService;
import org.apache.htrace.core.MilliSpan;
import org.apache.htrace.core.TraceScope;

public class TracingServiceImplTests extends DependencyInjectedTest {
  public TracingServiceImplTests() throws Exception {
    super();

    this.mockServiceInjector.replaceProvider(LoggingService.class, MockLoggingServiceImpl.class);
    this.mockServiceInjector.replaceProvider(TracingService.class, TracingServiceImpl.class);
  }

  @Test
  public void traceSerializationTest() throws Exception {
    MockLoggingServiceImpl mockLoggingService = (MockLoggingServiceImpl) ServiceProviderImpl.instance().get(LoggingService.class);

    TracingService tracingService = ServiceProviderImpl.instance().get(TracingService.class);
    TraceScope traceScope = tracingService.traceBegin("Test Scope");
    traceScope.addTimelineAnnotation("Timeline Annotations");
    traceScope.addKVAnnotation("key", "value");
    traceScope.close();

    // Should not throw exception.
    MilliSpan.fromJson(mockLoggingService.messages.get(0));
  }
}
