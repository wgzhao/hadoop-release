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
 * Provides tokens based on Azure VM's Managed Service Identity
 */
public class MsiTokenProvider extends AccessTokenProvider {

  private final String tenantGuid;

  private final String clientId;

  private final LoggingService loggingService;

  public MsiTokenProvider(final LoggingService loggingService, final String tenantGuid, final String clientId) {
    super(loggingService);

    Preconditions.checkNotNull(loggingService, "loggingService");

    this.loggingService = loggingService.get(ClientCredsTokenProvider.class);
    this.tenantGuid = tenantGuid;
    this.clientId = clientId;
  }

  @Override
  protected AzureADToken refreshToken() throws IOException {
    this.loggingService.debug("AADToken: refreshing token from MSI");
    AzureADToken token = AzureADAuthenticator.getTokenFromMsi(tenantGuid, clientId, false);
    return token;
  }
}