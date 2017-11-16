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
import org.apache.hadoop.fs.azuredfs.contracts.services.AdfsStreamFactory;
import static org.apache.hadoop.fs.azuredfs.constants.ConfigurationKeys.*;
import static org.apache.hadoop.fs.azuredfs.constants.FileSystemConfigurations.*;
import static org.junit.Assert.*;
public class TestUserDefinedBufferSize extends DependencyInjectedTest {
  private AdfsStreamFactoryImpl adfsStreamFactory;

  public TestUserDefinedBufferSize() throws Exception {
    super();
  }

  @Test
  public void testGetBufferSize() throws Exception {
    adfsStreamFactory = (AdfsStreamFactoryImpl)ServiceProviderImpl.instance().get(AdfsStreamFactory.class);

    // testing: user haven't overwritten the default size for a write buffer
    final int bufferSize = adfsStreamFactory.getBufferSize(KEY_WRITE_BLOCK_SIZE, DEFAULT_WRITE_BUFFER_SIZE);
    assertEquals(bufferSize, DEFAULT_WRITE_BUFFER_SIZE);

    // testing: user overwritten the default size for a write buffer with a valid value
    testUserDefinedBufferSize(KEY_WRITE_BLOCK_SIZE, "10485760", DEFAULT_WRITE_BUFFER_SIZE, 10485760);
    // testing: user overwritten the default size for a read buffer with a value smaller than the minimum required
    testUserDefinedBufferSize(KEY_READ_BLOCK_SIZE, "3072", DEFAULT_READ_BUFFER_SIZE, MIN_BUFFER_SIZE);
    // testing: user overwritten the default size for a read buffer with a value larger than the maximum allowed
    testUserDefinedBufferSize(KEY_READ_BLOCK_SIZE, "106000000", DEFAULT_READ_BUFFER_SIZE, MAX_BUFFER_SIZE);
  }

  private void testUserDefinedBufferSize(final String configKey, String userDefinedVal, final int defaultVal, final int actualVal) {
    this.getConfiguration().set(configKey, userDefinedVal);
    final int bufferSize = adfsStreamFactory.getBufferSize(configKey, defaultVal);
    assertEquals(bufferSize, actualVal);
  }
}
