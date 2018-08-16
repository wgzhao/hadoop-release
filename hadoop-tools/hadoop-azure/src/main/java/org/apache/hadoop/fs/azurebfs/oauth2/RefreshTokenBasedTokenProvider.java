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

package org.apache.hadoop.fs.azurebfs.oauth2;

import java.io.IOException;

import com.google.common.base.Preconditions;

import org.apache.hadoop.fs.azurebfs.contracts.services.LoggingService;


/**
 * Provides tokens based on refresh token.
 */
public class RefreshTokenBasedTokenProvider extends AccessTokenProvider {
  private final String clientId;

  private final String refreshToken;

  private final LoggingService loggingService;

  /**
   * Constructs a token provider based on the refresh token provided.
   *
   * @param loggingService the logging service
   * @param clientId the client ID (GUID) of the client web app obtained from Azure Active Directory configuration
   * @param refreshToken the refresh token
   */
  public RefreshTokenBasedTokenProvider(final LoggingService loggingService, String clientId, String refreshToken) {
    super(loggingService);

    Preconditions.checkNotNull(loggingService, "loggingService");
    Preconditions.checkNotNull(refreshToken, "refreshToken");

    this.loggingService = loggingService.get(ClientCredsTokenProvider.class);
    this.clientId = clientId;
    this.refreshToken = refreshToken;
  }


  @Override
  protected AzureADToken refreshToken() throws IOException {
    this.loggingService.debug("AADToken: refreshing refresh-token based token");
    return AzureADAuthenticator.getTokenUsingRefreshToken(clientId, refreshToken);
  }
}
