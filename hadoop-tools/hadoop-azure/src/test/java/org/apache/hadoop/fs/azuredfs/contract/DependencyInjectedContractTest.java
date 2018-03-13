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

package org.apache.hadoop.fs.azuredfs.contract;

import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.fs.azuredfs.DependencyInjectedTest;
import org.apache.hadoop.fs.azuredfs.constants.TestConfigurationKeys;

public class DependencyInjectedContractTest extends DependencyInjectedTest {
  private final URI testUri;
  private final String fileSystemName;
  private final String accountName;

  public DependencyInjectedContractTest() throws Exception {
    super();

    Configuration configuration = getConfiguration();
    this.testUri = new URI(configuration.get(TestConfigurationKeys.FS_AZURE_CONTRACT_TEST_URI));
    configuration.set(CommonConfigurationKeysPublic.FS_DEFAULT_NAME_KEY, this.testUri.toString());

    String[] splitAuthority = this.testUri.getAuthority().split("\\@");
    this.fileSystemName = splitAuthority[0];
    this.accountName = splitAuthority[1];
  }

  @Override
  protected String getTestUrl() {
    return this.testUri.toString();
  }

  @Override
  protected String getFileSystemName() {
    return this.fileSystemName;
  }

  @Override
  protected String getAccountName() {
    return this.accountName;
  }
}