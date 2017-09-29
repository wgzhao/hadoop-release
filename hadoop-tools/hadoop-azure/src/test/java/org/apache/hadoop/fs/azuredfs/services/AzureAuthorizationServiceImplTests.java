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

import okhttp3.OkHttpClient;
import okhttp3.Request;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.azuredfs.constants.ConfigurationKeys;

public class AzureAuthorizationServiceImplTests {
  @Test
  public void ensureUserIsAbleToAcquireProperAuthorization() throws Exception {
    Configuration conf = new Configuration();
    conf.addResource("azure-adfs-test.xml");

    String accountName = conf.get(ConfigurationKeys.FS_AZURE_ACCOUNT_NAME);
    String accountKey = conf.get(ConfigurationKeys.FS_AZURE_ACCOUNT_KEY);

    Assume.assumeNotNull(accountName);
    Assume.assumeNotNull(accountKey);

    AzureAuthorizationServiceImpl azureAuthorizationService = new AzureAuthorizationServiceImpl(new ConfigurationServiceImpl(conf));
    Request.Builder requestBuilder = new Request.Builder();

    // Update with HDFS url once it is ready.
    requestBuilder.url(String.format("https://%s.blob.core.windows.net/?comp=list", accountName));
    Request request = requestBuilder.build();
    request = azureAuthorizationService.updateRequestWithAuthorizationHeader(request);

    OkHttpClient okHttpClient = new OkHttpClient();
    int statusCode = okHttpClient.newCall(request).execute().code();
    Assert.assertEquals(statusCode, 200);
  }
}