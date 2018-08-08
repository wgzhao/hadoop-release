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

import java.net.HttpURLConnection;

import org.junit.Assert;
import org.junit.Assume;
import org.junit.Test;
import org.junit.Ignore;

import org.apache.hadoop.fs.azurebfs.constants.HttpHeaderConfigurations;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.AzureServiceErrorResponseException;
import org.apache.hadoop.fs.azurebfs.contracts.services.AzureServiceErrorCode;
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
      Assert.fail("The request should fail due to bad continuation token");
    } catch (AzureServiceErrorResponseException ex) {
      if (ex.getErrorCode() == AzureServiceErrorCode.INVALID_QUERY_PARAMETER_VALUE) {
        // Ignore
      } else {
        throw ex;
      }
    }
  }

  @Test
  @Ignore ("enable next week when fix is deployed")
  public void testConditionalForAccessControl() throws Exception {
    Assume.assumeTrue(isNamespaceEnabled());

    final AzureBlobFileSystem fs = this.getFileSystem();
    final AbfsHttpClientFactory abfsHttpClientFactory = new AbfsHttpClientFactoryImpl(new LoggingServiceImpl());
    final AbfsClient abfsClient = abfsHttpClientFactory.create(fs);

    try {
      abfsClient.setAcl("//", "user::rwx,group::rwx,other::rwx", "0x0D0FD0070AD00B0");
      Assert.fail("The request should fail due to unmatched eTag");
    } catch (AzureServiceErrorResponseException ex) {
      if (ex.getStatusCode() == HttpURLConnection.HTTP_PRECON_FAILED) {
        // Ignore
      } else {
        throw ex;
      }
    }

    AbfsRestOperation op1 = abfsClient.setAcl("//", "user::rwx,group::rwx,other::rwx");
    Assert.assertFalse(op1.getResult().getResponseHeader(HttpHeaderConfigurations.ETAG) == null);

    AbfsRestOperation op2 = abfsClient.getAclStatus("//");
    Assert.assertFalse(op2.getResult().getResponseHeader(HttpHeaderConfigurations.ETAG) == null);
  }
}
