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

package org.apache.hadoop.fs.azuredfs.utils;

import org.junit.Assert;
import org.junit.Test;

public final class TestUriUtils {
  @Test
  public void testIfUriContainsAdfs() throws Exception {
    Assert.assertTrue(UriUtils.containsAdfsUrl("adfs.dfs.core.windows.net"));
    Assert.assertTrue(UriUtils.containsAdfsUrl("adfs.dfs.preprod.core.windows.net"));
    Assert.assertFalse(UriUtils.containsAdfsUrl("adfs.dfs.cores.windows.net"));
    Assert.assertFalse(UriUtils.containsAdfsUrl(""));
    Assert.assertFalse(UriUtils.containsAdfsUrl(null));
    Assert.assertFalse(UriUtils.containsAdfsUrl("adfs.dfs.cores.windows.net"));
    Assert.assertFalse(UriUtils.containsAdfsUrl("xhdfs.blob.core.windows.net"));
  }

  @Test
  public void testExtractRawAccountName() throws Exception {
    Assert.assertEquals("adfs", UriUtils.extractRawAccountFromAccountName("adfs.dfs.core.windows.net"));
    Assert.assertEquals("adfs", UriUtils.extractRawAccountFromAccountName("adfs.dfs.preprod.core.windows.net"));
    Assert.assertEquals(null, UriUtils.extractRawAccountFromAccountName("adfs.dfs.cores.windows.net"));
    Assert.assertEquals(null, UriUtils.extractRawAccountFromAccountName(""));
    Assert.assertEquals(null, UriUtils.extractRawAccountFromAccountName(null));
    Assert.assertEquals(null, UriUtils.extractRawAccountFromAccountName("adfs.dfs.cores.windows.net"));
  }
}
