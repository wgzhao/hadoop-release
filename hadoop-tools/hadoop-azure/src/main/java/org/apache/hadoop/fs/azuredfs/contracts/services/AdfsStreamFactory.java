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

import java.io.InputStream;
import java.io.OutputStream;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.azuredfs.AzureDistributedFileSystem;

/**
 * AdfsStream factory to create read/write streams.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public interface AdfsStreamFactory extends InjectableService {
  /**
   * Creates a read stream to the provided path on the service.
   * @param azureDistributedFileSystem filesystem to create a read stream to a file path.
   * @param path file path to be read.
   * @param fileSize size of the file to create the read stream to.
   * @param version version of the file.
   * @return InputStream a stream to the provided path.
   */
  InputStream createReadStream(AzureDistributedFileSystem azureDistributedFileSystem, Path path, long fileSize, String version);

  /**
   * Creates a write stream to the provided path on the service.
   * @param azureDistributedFileSystem filesystem to create a write stream to a file path.
   * @param path file path to be written.
   * @param offset offset to start writing.
   * @return OutputStream a stream to the provided path.
   */
  OutputStream createWriteStream(AzureDistributedFileSystem azureDistributedFileSystem, Path path, long offset);
}