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

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.UUID;

import org.junit.After;
import org.junit.Before;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.azuredfs.constants.FileSystemUriSchemes;
import org.apache.hadoop.fs.azuredfs.constants.TestConfigurationKeys;
import org.apache.hadoop.fs.azuredfs.contracts.exceptions.ServiceResolutionException;
import org.apache.hadoop.fs.azuredfs.contracts.services.AdfsBlobHandler;
import org.apache.hadoop.fs.azuredfs.contracts.services.AdfsHttpClientFactory;
import org.apache.hadoop.fs.azuredfs.contracts.services.AdfsHttpClientSession;
import org.apache.hadoop.fs.azuredfs.contracts.services.AdfsHttpClientSessionFactory;
import org.apache.hadoop.fs.azuredfs.contracts.services.ConfigurationService;
import org.apache.hadoop.fs.azuredfs.contracts.services.LoggingService;
import org.apache.hadoop.fs.azuredfs.services.MockAdfsBlobHandlerImpl;
import org.apache.hadoop.fs.azuredfs.services.MockAdfsHttpClientFactoryImpl;
import org.apache.hadoop.fs.azuredfs.services.MockAdfsHttpClientSessionFactoryImpl;
import org.apache.hadoop.fs.azuredfs.services.MockConfigurationServiceImpl;
import org.apache.hadoop.fs.azuredfs.services.MockLoggingServiceImpl;
import org.apache.hadoop.fs.azuredfs.services.MockServiceInjectorImpl;
import org.apache.hadoop.fs.azuredfs.services.MockServiceProviderImpl;
import org.apache.hadoop.fs.azuredfs.services.ServiceProviderImpl;
import org.apache.hadoop.fs.contract.AbstractBondedFSContract;

public class AdfsFileSystemContract extends AbstractBondedFSContract {

  public static final String CONTRACT_XML = "adfs.xml";

  protected AdfsFileSystemContract(Configuration conf) {
    super(conf);
    //insert the base features
    addConfResource(CONTRACT_XML);
  }

  @Override
  public String getScheme() {
    return "adfs";
  }
}