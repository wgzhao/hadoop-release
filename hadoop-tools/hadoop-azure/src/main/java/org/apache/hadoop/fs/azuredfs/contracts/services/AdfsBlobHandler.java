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

package org.apache.hadoop.fs.azuredfs.contracts.services;

import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

/**
 * AdfsBlobHandler. This class is temporarily used for making the NativeAzureFileSystem injectable.
 * Currently for rename/delete directory we redirect the requests to WASB.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public interface AdfsBlobHandler extends InjectableService {
  /**
   * Converts Adfs Uri to Blob Uri.
   * @return URI blob URI.
   */
  URI convertAdfsUriToBlobUri(URI adfsUri) throws URISyntaxException;

  /**
   * Creates blob endpoint uri from blob uri and account name.
   * @return blob endpoint URI.
   */
  URI getBlobEndpointUriFromBlobUriAndAccountName(URI blobUri, String accountName) throws URISyntaxException;
}