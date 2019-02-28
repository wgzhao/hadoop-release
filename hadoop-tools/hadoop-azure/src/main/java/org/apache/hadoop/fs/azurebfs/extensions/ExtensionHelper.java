/*
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

package org.apache.hadoop.fs.azurebfs.extensions;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IOUtils;

/**
 * Classes to help with use of extensions, expecially those
 * implementing @{@link BoundDTExtension}.
 */
@InterfaceAudience.LimitedPrivate("authorization-subsystems")
@InterfaceStability.Unstable
public final class ExtensionHelper {

  private ExtensionHelper() {
  }

  /**
   * If the passed in extension class implements {@link BoundDTExtension}
   * then it will have its {@link BoundDTExtension#bind(URI, Configuration)}
   * method called.
   * @param extension extension to examine and maybe invoke
   * @param uri URI of the filesystem.
   * @param conf configuration of this extension.
   * @throws IOException failure during binding.
   */
  public static void bind(Object extension, URI uri, Configuration conf)
      throws IOException {
    if (extension instanceof BoundDTExtension) {
      ((BoundDTExtension) extension).bind(uri, conf);
    }
  }

  /**
   * Close an extension if it is closeable.
   * Any error raised is caught and logged.
   * @param extension extension instance.
   */
  public static void close(Object extension) {
    if (ifBoundDTExtension(extension)) {
      IOUtils.cleanupWithLogger(null, (BoundDTExtension) extension);
    }
  }

  /**
   * Invoke {@link BoundDTExtension#getUserAgentSuffix()} or
   * return the default value.
   * @param extension extension to invoke
   * @param def default if the class is of the wrong type.
   * @return a user agent suffix
   */
  public static String getUserAgentSuffix(Object extension, String def) {
    if (ifBoundDTExtension(extension)){
      return ((BoundDTExtension) extension).getUserAgentSuffix();
    } else {
      return def;
    }
  }

  /**
   * Invoke {@link BoundDTExtension#getCanonicalServiceName()} or
   * return the default value.
   * @param extension extension to invoke
   * @param def default if the class is of the wrong type.
   * @return a canonical service name.
   */
  public static String getCanonicalServiceName(Object extension, String def) {
    if (ifBoundDTExtension(extension)){
      return ((BoundDTExtension) extension).getCanonicalServiceName();
    } else {
      return def;
    }
  }

  /**
   * Invoke an operation on an object if it implements the BoundDTExtension
   * interface; returns an optional value.
   * @param extension the extension to invoke.
   * @return true if extension is instance of BoundDTExtension
   */
  private static boolean ifBoundDTExtension(Object extension) {
    return extension instanceof BoundDTExtension;
  }

}
