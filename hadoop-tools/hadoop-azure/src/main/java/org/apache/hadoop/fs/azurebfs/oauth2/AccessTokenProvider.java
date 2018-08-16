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
import java.util.Date;

import com.google.common.base.Preconditions;

import org.apache.hadoop.fs.azurebfs.contracts.services.LoggingService;

/**
 * Returns an Azure Active Directory token when requested. The provider can cache the token if it has already
 * retrieved one. If it does, then the provider is responsible for checking expiry and refreshing as needed.
 * <p>
 * In other words, this is is a token cache that fetches tokens when requested, if the cached token has expired.
 * </P>
 */
public abstract class AccessTokenProvider {

  private AzureADToken token;
  private final LoggingService loggingService;


  protected AccessTokenProvider(final LoggingService loggingService) {
    Preconditions.checkNotNull(loggingService, "loggingService");

    this.loggingService = loggingService.get(ClientCredsTokenProvider.class);
  }

  /**
   * returns the {@link AzureADToken} cached (or retrieved) by this instance.
   *
   * @return {@link AzureADToken} containing the access token
   * @throws IOException if there is an error fetching the token
   */
  public synchronized AzureADToken getToken() throws IOException {
    if (isTokenAboutToExpire()) {
      this.loggingService.debug("AAD Token is missing or expired: Calling refresh-token from abstract base class");
      token = refreshToken();
    }
    return token;
  }

  /**
   * the method to fetch the access token. Derived classes should override this method to
   * actually get the token from Azure Active Directory.
   * <p>
   * This method will be called initially, and then once when the token is about to expire.
   * </P>
   *
   * @return {@link AzureADToken} containing the access token
   * @throws IOException if there is an error fetching the token
   */
  protected abstract AzureADToken refreshToken() throws IOException;

  /**
   * Checks if the token is about to expire in the next 5 minutes. The 5 minute allowance is to
   * allow for clock skew and also to allow for token to be refreshed in that much time.
   *
   * @return true if the token is expiring in next 5 minutes
   */
  private boolean isTokenAboutToExpire() {
    if (token == null) {
      this.loggingService.debug("AADToken: no token. Returning expiring=true");
      return true;   // no token should have same response as expired token
    }
    if (token.getExpiry() == null) {
      this.loggingService.debug("AADToken: no token expiry set. Returning expiring=true");
      return true; // if don't know expiry then assume expired (should not happen with a correctly implemented token)
    }
    boolean expiring = false;
    long approximatelyNow = System.currentTimeMillis() + FIVE_MINUTES;   // allow 5 minutes for clock skew
    if (token.getExpiry().getTime() < approximatelyNow) {
      expiring = true;
    }
    if (expiring) {
      this.loggingService.debug("AADToken: token expiring: "
              + token.getExpiry().toString() + " : Five-minute window: " + new Date(approximatelyNow).toString());
    }

    return expiring;
  }

  private static final long FIVE_MINUTES = 300 * 1000; // 5 minutes in milliseconds
}
