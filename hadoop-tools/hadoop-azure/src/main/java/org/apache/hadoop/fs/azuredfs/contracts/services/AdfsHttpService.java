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
import java.util.Hashtable;

import rx.Observable;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.azuredfs.AzureDistributedFileSystem;
import org.apache.hadoop.fs.azuredfs.contracts.exceptions.AzureDistributedFileSystemException;

/**
 * File System http service to provide network calls for file system operations.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public interface AdfsHttpService extends InjectableService {
  /**
   * Gets filesystem properties on the Azure service.
   * @param azureDistributedFileSystem filesystem to get the properties.
   * @return Hashtable<String, String> hash table containing all the filesystem properties.
   */
  Hashtable<String, String> getFilesystemProperties(AzureDistributedFileSystem azureDistributedFileSystem) throws AzureDistributedFileSystemException;

  /**
   * Gets filesystem properties on the Azure service asynchronously.
   * @param azureDistributedFileSystem filesystem to get the properties.
   * @return Observable<Hashtable<String, String>> an observable hash table containing the properties.
   */
  Observable<Hashtable<String, String>> getFilesystemPropertiesAsync(AzureDistributedFileSystem azureDistributedFileSystem) throws
      AzureDistributedFileSystemException;

  /**
   * Sets filesystem properties on the Azure service.
   * @param azureDistributedFileSystem filesystem to get the properties.
   * @param properties file system properties to set.
   */
  void setFilesystemProperties(AzureDistributedFileSystem azureDistributedFileSystem, Hashtable<String, String> properties) throws
      AzureDistributedFileSystemException;

  /**
   * Sets filesystem properties on the Azure service asynchronously.
   * @param azureDistributedFileSystem filesystem to get the properties.
   * @param properties file system properties to set.
   * @return Observable<void> an observable Void object.
   */
  Observable<Void> setFilesystemPropertiesAsync(AzureDistributedFileSystem azureDistributedFileSystem, Hashtable<String, String> properties) throws
      AzureDistributedFileSystemException;

  /**
   * Creates filesystem on the Azure service.
   * @param azureDistributedFileSystem filesystem to be created.
   */
  void createFilesystem(AzureDistributedFileSystem azureDistributedFileSystem) throws AzureDistributedFileSystemException;

  /**
   * Creates filesystem on the Azure service asynchronously.
   * @param azureDistributedFileSystem filesystem to be created.
   * @return Observable<void> an observable Void object.
   */
  Observable<Void> createFilesystemAsync(AzureDistributedFileSystem azureDistributedFileSystem) throws AzureDistributedFileSystemException;

  /**
   * Deletes filesystem on the Azure service.
   * @param azureDistributedFileSystem filesystem to be deleted.
   */
  void deleteFilesystem(AzureDistributedFileSystem azureDistributedFileSystem) throws AzureDistributedFileSystemException;

  /**
   * Deletes filesystem on the Azure service asynchronously.
   * @param azureDistributedFileSystem filesystem to be deleted.
   * @return Observable<void> an observable Void object.
   */
  Observable<Void> deleteFilesystemAsync(AzureDistributedFileSystem azureDistributedFileSystem) throws AzureDistributedFileSystemException;

  /**
   * Creates file or directory on the Azure service.
   * @param azureDistributedFileSystem filesystem to create file or directory.
   * @param path path to be created.
   * @param isDirectory if path represents directory.
   * @return OutputStream stream to the path.
   */
  OutputStream createPath(AzureDistributedFileSystem azureDistributedFileSystem, Path path, boolean isDirectory) throws AzureDistributedFileSystemException;

  /**
   * Creates file or directory on the Azure service asynchronously.
   * @param azureDistributedFileSystem filesystem to create file or directory.
   * @param path path to be created.
   * @param isDirectory if path represents directory.
   * @return Observable<OutputStream> an observable stream to the path.
   */
  Observable<OutputStream> createPathAsync(AzureDistributedFileSystem azureDistributedFileSystem, Path path, boolean isDirectory) throws
      AzureDistributedFileSystemException;

  /**
   * Reads path and returns the stream if it is file.
   * @param azureDistributedFileSystem filesystem to read a file from.
   * @param path path to be created.
   * @return InputStream the stream to the file.
   */
  InputStream readPath(AzureDistributedFileSystem azureDistributedFileSystem, Path path) throws AzureDistributedFileSystemException;

  /**
   * Reads path and returns the stream if it is file asynchronously.
   * @param azureDistributedFileSystem filesystem to read a file from.
   * @param path path to be created.
   * @return Observable<InputStream> an observable stream to the file.
   */
  Observable<InputStream> readPathAsync(AzureDistributedFileSystem azureDistributedFileSystem, Path path) throws AzureDistributedFileSystemException;

  /**
   * Updates a path with provided arguments.
   * @param azureDistributedFileSystem filesystem to read a file from.
   * @param isDirectory flag to determine the path type.
   * @param path path to be updated.
   */
  void updatePath(AzureDistributedFileSystem azureDistributedFileSystem, Path path, boolean isDirectory) throws AzureDistributedFileSystemException;

  /**
   * Updates a path with provided arguments asynchronously.
   * @param azureDistributedFileSystem filesystem to read a file from.
   * @param path path to be updated.
   * @param isDirectory flag to determine the path type.
   * @return Observable<void> an observable Void object.
   */
  Observable<Void> updatePathAsync(AzureDistributedFileSystem azureDistributedFileSystem, Path path, boolean isDirectory)
      throws AzureDistributedFileSystemException;

  /**
   * Renames path from source to destination.
   * @param azureDistributedFileSystem filesystem to rename a file.
   * @param source source path.
   * @param destination destination path.
   * @param isDirectory flag to determine the path type.
   */
  void renamePath(AzureDistributedFileSystem azureDistributedFileSystem, Path source, Path destination, boolean isDirectory)
      throws AzureDistributedFileSystemException;

  /**
   * Renames path from source to destination asynchronously.
   * @param azureDistributedFileSystem filesystem to rename a file.
   * @param source source path.
   * @param destination destination path.
   * @param isDirectory flag to determine the path type.
   * @return Observable<void> an observable Void object.
   */
  Observable<Void> renamePathAsync(AzureDistributedFileSystem azureDistributedFileSystem, Path source, Path destination, boolean isDirectory) throws
      AzureDistributedFileSystemException;

  /**
   * Deletes a path.
   * @param azureDistributedFileSystem filesystem to delete the path.
   * @param path path to be deleted.
   * @param isDirectory flag to determine the path type.
   */
  void deletePath(AzureDistributedFileSystem azureDistributedFileSystem, Path path, boolean isDirectory) throws AzureDistributedFileSystemException;

  /**
   * Deletes a path asynchronously.
   * @param azureDistributedFileSystem filesystem to delete the path.
   * @param path path to be deleted.
   * @param isDirectory flag to determine the path type.
   * @return Observable<void> an observable Void object.
   */
  Observable<Void> deletePathAsync(AzureDistributedFileSystem azureDistributedFileSystem, Path path, boolean isDirectory)
      throws AzureDistributedFileSystemException;

  /**
   * Lists all the paths under the provided path on the Azure service.
   * @param azureDistributedFileSystem filesystem to perform the list operation.
   * @param path path delimiter.
   * @return FileStatus[] list of all paths in the file system.
   */
  FileStatus[] listStatus(AzureDistributedFileSystem azureDistributedFileSystem, Path path) throws AzureDistributedFileSystemException;

  /**
   * Lists all the paths under the provided path on the Azure service asynchronously.
   * @param azureDistributedFileSystem filesystem to perform the list operation.
   * @param path path delimiter.
   * @return Observable<FileStatus[]> an observable list of all paths in the file system.
   */
  Observable<FileStatus[]> listStatusAsync(AzureDistributedFileSystem azureDistributedFileSystem, Path path) throws AzureDistributedFileSystemException;

  /**
   * Closes the client to filesystem to Azure service.
   * @param azureDistributedFileSystem filesystem to perform the list operation.
   */
  void closeFileSystem(AzureDistributedFileSystem azureDistributedFileSystem) throws AzureDistributedFileSystemException;
}