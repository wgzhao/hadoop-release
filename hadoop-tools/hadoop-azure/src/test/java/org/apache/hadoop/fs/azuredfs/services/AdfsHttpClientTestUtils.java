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

import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import org.apache.hadoop.fs.azuredfs.AzureDistributedFileSystem;
import org.apache.hadoop.fs.azuredfs.contracts.exceptions.AzureDistributedFileSystemException;
import org.apache.hadoop.fs.azuredfs.contracts.services.AdfsHttpClientSession;
import org.apache.hadoop.fs.azuredfs.contracts.services.AdfsHttpClientSessionFactory;

import static org.mockito.Matchers.anyObject;
import static org.mockito.Mockito.when;

public final class AdfsHttpClientTestUtils {
  public static AdfsHttpClientSessionFactory createAdfsHttpClientSessionFactory(final String accountName, final String accountKey, final String filesystem)
      throws AzureDistributedFileSystemException {
    AdfsHttpClientSessionFactory adfsHttpClientSessionFactory = Mockito.mock(AdfsHttpClientSessionFactory.class);

    when(adfsHttpClientSessionFactory.create((AzureDistributedFileSystem) anyObject())).thenAnswer(new Answer<AdfsHttpClientSession>() {
      @Override
      public AdfsHttpClientSession answer(InvocationOnMock invocationOnMock) throws Throwable {
        return new AdfsHttpClientSessionImpl(
            accountName,
            accountKey,
            filesystem
        );
      }
    });

    return adfsHttpClientSessionFactory;
  }
}
