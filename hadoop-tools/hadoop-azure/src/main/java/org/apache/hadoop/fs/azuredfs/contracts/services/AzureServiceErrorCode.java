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

import java.net.HttpURLConnection;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

/**
 * Azure service error codes.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public enum AzureServiceErrorCode {
  FILE_SYSTEM_ALREADY_EXISTS("FilesystemAlreadyExists", HttpURLConnection.HTTP_CONFLICT),
  PATH_ALREADY_EXISTS("PathAlreadyExists", HttpURLConnection.HTTP_CONFLICT),
  PATH_CONFLICT("PathConflict", HttpURLConnection.HTTP_CONFLICT),
  FILE_SYSTEM_NOT_FOUND("FilesystemNotFound", HttpURLConnection.HTTP_NOT_FOUND),
  PATH_NOT_FOUND("PathNotFound", HttpURLConnection.HTTP_NOT_FOUND),
  PRE_CONDITION_FAILED("PreconditionFailed", HttpURLConnection.HTTP_PRECON_FAILED),
  SOURCE_PATH_NOT_FOUND("SourcePathNotFound", HttpURLConnection.HTTP_NOT_FOUND),
  INVALID_SOURCE_OR_DESTINATION_RESOURCE_TYPE("InvalidSourceOrDestinationResourceType", HttpURLConnection.HTTP_CONFLICT),
  RENAME_DESTINATION_PARENT_PATH_NOT_FOUND("RenameDestinationParentPathNotFound", HttpURLConnection.HTTP_NOT_FOUND),
  UNKNOWN(null, -1);

  private final String errorCode;
  private final int httpStatusCode;
  AzureServiceErrorCode(String errorCode, int httpStatusCodes) {
    this.errorCode = errorCode;
    this.httpStatusCode = httpStatusCodes;
  }

  public int getStatusCode() {
    return this.httpStatusCode;
  }

  public String getErrorCode() {
    return this.errorCode;
  }

  public static AzureServiceErrorCode getAzureServiceCode(int httpStatusCode, String errorCode) {
    if (errorCode == null || errorCode.isEmpty() || httpStatusCode == UNKNOWN.httpStatusCode) {
      return UNKNOWN;
    }

    for (AzureServiceErrorCode azureServiceErrorCode : AzureServiceErrorCode.values()) {
      if (errorCode.equalsIgnoreCase(azureServiceErrorCode.errorCode) && azureServiceErrorCode.httpStatusCode == httpStatusCode) {
        return azureServiceErrorCode;
      }
    }

    return UNKNOWN;
  }
}
