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

import java.net.URI;
import java.net.URISyntaxException;

import com.google.inject.Singleton;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.azuredfs.constants.FileSystemUriSchemes;
import org.apache.hadoop.fs.azuredfs.contracts.services.AdfsBlobHandler;
import org.apache.hadoop.fs.azuredfs.utils.UriUtils;

@Singleton
@InterfaceAudience.Private
@InterfaceStability.Evolving
class AdfsBlobHandlerImpl implements AdfsBlobHandler {
  @Override
  public URI convertAdfsUriToBlobUri(final URI adfsUri) throws URISyntaxException {
    String adfsUrl = adfsUri.toString();
    if (UriUtils.containsAdfsUrl(adfsUrl)) {
      return new URI(replaceAdfsWithBlob(adfsUrl));
    }

    return adfsUri;
  }

  @Override
  public URI getBlobEndpointUriFromBlobUriAndAccountName(final URI blobUri, final String accountName) throws URISyntaxException {
    String scheme;
    String sessionScheme = blobUri.getScheme();
    // Check if we're on a secure URI scheme: wasbs or the legacy asvs scheme.
    if (sessionScheme != null
        && (sessionScheme.equalsIgnoreCase("asvs")
        || sessionScheme.equalsIgnoreCase("wasbs"))) {
      scheme = FileSystemUriSchemes.HTTPS_SCHEME;
    } else {
      // At this point the scheme should be either null or asv or wasb.
      // Intentionally I'm not going to validate it though since I don't feel
      // it's this method's job to ensure a valid URI scheme for this file
      // system.
      scheme = FileSystemUriSchemes.HTTP_SCHEME;
    }

    String modifedAccountName = accountName;
    if (UriUtils.containsAdfsUrl(accountName)) {
      modifedAccountName = replaceAdfsWithBlob(accountName);
    }

    return new URI(scheme + "://" + modifedAccountName);
  }

  private String replaceAdfsWithBlob(String adfsString) {
    return adfsString.replace(".dfs.", ".blob.");
  }
}