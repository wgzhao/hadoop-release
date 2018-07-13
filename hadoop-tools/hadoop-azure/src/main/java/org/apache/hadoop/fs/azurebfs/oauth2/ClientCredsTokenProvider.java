/*
 * Copyright (c) Microsoft Corporation. All rights reserved.
 * Licensed under the MIT License.
 * See License.txt in the project root for license information.
 */

package org.apache.hadoop.fs.azurebfs.oauth2;

import java.io.IOException;

import com.google.common.base.Preconditions;

import org.apache.hadoop.fs.azurebfs.contracts.services.LoggingService;

/**
 * Provides tokens based on client credentials
 */
public class ClientCredsTokenProvider extends AccessTokenProvider {

  private final String authEndpoint;

  private final String clientId;

  private final String clientSecret;

  private final LoggingService loggingService;


  public ClientCredsTokenProvider(final LoggingService loggingService, final String authEndpoint, final String clientId, final String clientSecret) {
    super(loggingService);

    Preconditions.checkNotNull(loggingService, "loggingService");
    Preconditions.checkNotNull(authEndpoint, "authEndpoint");
    Preconditions.checkNotNull(clientId, "clientId");
    Preconditions.checkNotNull(clientSecret, "clientSecret");

    this.loggingService = loggingService.get(ClientCredsTokenProvider.class);
    this.authEndpoint = authEndpoint;
    this.clientId = clientId;
    this.clientSecret = clientSecret;
  }


  @Override
  protected AzureADToken refreshToken() throws IOException {
    this.loggingService.debug("AADToken: refreshing client-credential based token");
    return AzureADAuthenticator.getTokenUsingClientCreds(authEndpoint, clientId, clientSecret);
  }


}
