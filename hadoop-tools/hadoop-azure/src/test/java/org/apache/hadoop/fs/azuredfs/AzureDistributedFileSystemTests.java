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

package org.apache.hadoop.fs.azuredfs;

import java.util.concurrent.Callable;

import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.azuredfs.contracts.exceptions.AzureServiceErrorResponseException;
import org.apache.hadoop.fs.azuredfs.contracts.services.AdfsHttpService;
import org.apache.hadoop.fs.azuredfs.contracts.services.ConfigurationService;
import org.apache.hadoop.fs.azuredfs.services.ServiceProviderImpl;

import static org.apache.hadoop.fs.azuredfs.contracts.services.AzureServiceErrorCode.FILE_SYSTEM_NOT_FOUND;
import static org.apache.hadoop.test.LambdaTestUtils.intercept;

public class AzureDistributedFileSystemTests extends DependencyInjectedTest {
  public AzureDistributedFileSystemTests() throws Exception {
    super();
  }

  @Test
  public void ensureFileCreated() throws Exception {
    final AzureDistributedFileSystem fs = (AzureDistributedFileSystem) FileSystem.get(this.getConfiguration());
    fs.create(new Path("testfile"));

    FileStatus[] fileStatuses = fs.listStatus(new Path("testfile"));
    Assert.assertEquals(1, fileStatuses.length);
  }

  @Test
  public void ensureFileIsRenamed() throws Exception {
    final AzureDistributedFileSystem fs = (AzureDistributedFileSystem) FileSystem.get(this.getConfiguration());
    fs.create(new Path("testfile"));
    fs.rename(new Path("testfile"), new Path("testfile2"));

    FileStatus[] fileStatuses = fs.listStatus(new Path("testfile2"));
    Assert.assertEquals(1, fileStatuses.length);
  }

  @Test
  public void ensureFileIsDeleted() throws Exception {
    Configuration configuration = ServiceProviderImpl.instance().get(ConfigurationService.class).getConfiguration();
    final AzureDistributedFileSystem fs = (AzureDistributedFileSystem) FileSystem.get(configuration);
    fs.create(new Path("testfile"));
    fs.delete(new Path("testfile"), false);

    FileStatus[] fileStatuses = fs.listStatus(new Path("testfile"));
    Assert.assertEquals(0, fileStatuses.length);
  }

  @After
  public void TestCleanup() throws Exception {
    FileSystem.closeAll();

    Configuration configuration = ServiceProviderImpl.instance().get(ConfigurationService.class).getConfiguration();
    final AzureDistributedFileSystem fs = (AzureDistributedFileSystem) FileSystem.get(configuration);

    final AdfsHttpService adfsHttpService = ServiceProviderImpl.instance().get(AdfsHttpService.class);
    adfsHttpService.deleteFilesystem(fs);

    AzureServiceErrorResponseException ex = intercept(
        AzureServiceErrorResponseException.class,
        new Callable<Void>() {
          @Override
          public Void call() throws Exception {
            adfsHttpService.getFilesystemProperties(fs);
            return null;
          }
        });

    Assert.assertEquals(FILE_SYSTEM_NOT_FOUND.getStatusCode(), ex.getStatusCode());
  }
}