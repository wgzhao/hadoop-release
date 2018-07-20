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

package org.apache.hadoop.fs.azurebfs.services;

import org.apache.hadoop.fs.azurebfs.contracts.exceptions.AzureServiceErrorResponseException;
import org.apache.hadoop.fs.azurebfs.contracts.services.AzureServiceErrorCode;
import org.junit.Assert;
import org.junit.Test;

import org.apache.hadoop.fs.azurebfs.AzureBlobFileSystem;
import org.apache.hadoop.fs.azurebfs.DependencyInjectedTest;
import org.apache.hadoop.fs.azurebfs.contracts.services.AbfsHttpClientFactory;

public final class ITestAbfsClient extends DependencyInjectedTest {
  @Test
  public void testContinuationTokenHavingEqualSign() throws Exception {
    final AzureBlobFileSystem fs = this.getFileSystem();
    final AbfsHttpClientFactory abfsHttpClientFactory = new AbfsHttpClientFactoryImpl(new LoggingServiceImpl());
    final AbfsClient abfsClient = abfsHttpClientFactory.create(fs);

    try {
      AbfsRestOperation op = abfsClient.listPath("/", true, 5000, "===========");
      Assert.assertTrue(op.getUrl().toString().contains("continuation=%3D%3D%3D%3D%3D%3D%3D%3D%3D%3D%3D"));
    } catch (AzureServiceErrorResponseException ex) {
      if (ex.getErrorCode() == AzureServiceErrorCode.INVALID_QUERY_PARAMETER_VALUE) {
        // Ignore
      } else {
        throw ex;
      }
    }
  }
}
