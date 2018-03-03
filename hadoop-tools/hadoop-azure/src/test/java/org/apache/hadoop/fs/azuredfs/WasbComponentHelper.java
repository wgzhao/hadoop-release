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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.azure.AbstractWasbTestBase;
import org.apache.hadoop.fs.azure.AzureBlobStorageTestAccount;
import org.apache.hadoop.fs.azure.NativeAzureFileSystem;

import java.net.URI;

public class WasbComponentHelper extends AbstractWasbTestBase {
  public AzureBlobStorageTestAccount wasbAccount;
  public NativeAzureFileSystem nativeFs;

  public WasbComponentHelper() throws Exception {
    super.setUp();
    Configuration conf = fs.getConf();
    conf.setBoolean(NativeAzureFileSystem.APPEND_SUPPORT_ENABLE_PROPERTY_NAME, true);
    URI uri = fs.getUri();
    fs.initialize(uri, conf);
    wasbAccount = getTestAccount();
    nativeFs = wasbAccount.getFileSystem();
  }

  @Override
  protected AzureBlobStorageTestAccount createTestAccount() throws Exception {
    return AzureBlobStorageTestAccount.create();
  }
}