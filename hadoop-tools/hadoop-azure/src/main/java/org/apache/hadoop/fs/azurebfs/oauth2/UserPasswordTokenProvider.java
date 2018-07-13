/*
 * Copyright (c) Microsoft Corporation. All rights reserved.
 * Licensed under the MIT License.
 * See License.txt in the project root for license information.
 */

package org.apache.hadoop.fs.azurebfs.oauth2;

import java.io.IOException;

import com.google.common.base.Preconditions;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.azurebfs.contracts.services.LoggingService;

/**
 * Provides tokens based on username and password
 */
public class UserPasswordTokenProvider extends AccessTokenProvider {

  private final String authEndpoint;

  private final String username;

  private final String password;

  private final LoggingService loggingService;

  public UserPasswordTokenProvider(final LoggingService loggingService, final String authEndpoint, final String username, final String password) {
    super(loggingService);

    Preconditions.checkNotNull(loggingService, "loggingService");
    Preconditions.checkNotNull(authEndpoint, "authEndpoint");
    Preconditions.checkNotNull(username, "username");
    Preconditions.checkNotNull(password, "password");

    this.loggingService = loggingService.get(UserPasswordTokenProvider.class);
    this.authEndpoint = authEndpoint;
    this.username = username;
    this.password = password;
  }

  @Override
  protected AzureADToken refreshToken() throws IOException {
    this.loggingService.debug("AADToken: refreshing user-password based token");
    return AzureADAuthenticator.getTokenUsingClientCreds(authEndpoint, username, password);
  }

  private static String getPasswordString(Configuration conf, String key)
          throws IOException {
    char[] passchars = conf.getPassword(key);
    if (passchars == null) {
      throw new IOException("Password " + key + " not found");
    }
    return new String(passchars);
  }
}
