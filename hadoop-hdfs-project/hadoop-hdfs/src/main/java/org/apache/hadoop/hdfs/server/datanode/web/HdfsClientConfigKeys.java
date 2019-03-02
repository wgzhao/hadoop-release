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
package org.apache.hadoop.hdfs.server.datanode.web;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;

import java.util.concurrent.TimeUnit;

/** Client configuration properties */
@InterfaceAudience.Private
public class HdfsClientConfigKeys {
  public static String DFS_WEBHDFS_REST_CSRF_ENABLED_KEY = "dfs.webhdfs.rest-csrf.enabled";
  public static boolean DFS_WEBHDFS_REST_CSRF_ENABLED_DEFAULT = false;
  public static String DFS_WEBHDFS_REST_CSRF_CUSTOM_HEADER_KEY =
      "dfs.webhdfs.rest-csrf.custom-header";
  public static String DFS_WEBHDFS_REST_CSRF_CUSTOM_HEADER_DEFAULT = "X-XSRF-HEADER";
  public static String DFS_WEBHDFS_REST_CSRF_METHODS_TO_IGNORE_KEY =
      "dfs.webhdfs.rest-csrf.methods-to-ignore";
  public static String DFS_WEBHDFS_REST_CSRF_METHODS_TO_IGNORE_DEFAULT =
      "GET,OPTIONS,HEAD,TRACE";
  public static String DFS_WEBHDFS_REST_CSRF_BROWSER_USERAGENTS_REGEX_KEY =
      "dfs.webhdfs.rest-csrf.browser-useragents-regex";

  public static String OAUTH_CLIENT_ID_KEY = "dfs.webhdfs.oauth2.client.id";
  public static String OAUTH_REFRESH_URL_KEY = "dfs.webhdfs.oauth2.refresh.url";

  public static String ACCESS_TOKEN_PROVIDER_KEY = "dfs.webhdfs.oauth2.access.token.provider";
}
