/*
 * Copyright (c) Microsoft Corporation. All rights reserved.
 * Licensed under the MIT License.
 * See License.txt in the project root for license information.
 */

package org.apache.hadoop.fs.azurebfs.oauth2;

import java.util.Date;


/**
 * Object representing the AAD access token to use when making HTTP requests to Azure Data Lake Storage.
 */
public class AzureADToken {
  private String accessToken;
  private Date expiry;

  public String getAccessToken() {
    return this.accessToken;
  }

  public void setAccessToken(String accessToken) {
    this.accessToken = accessToken;
  }

  public Date getExpiry() {
    return this.expiry;
  }

  public void setExpiry(Date expiry) {
    this.expiry = expiry;
  }

}