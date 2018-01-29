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
import java.util.concurrent.Future;

import io.netty.buffer.ByteBuf;

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
   * @return Future<Hashtable<String, String>> Future of a hash table containing the properties.
   */
  Future<Hashtable<String, String>> getFilesystemPropertiesAsync(AzureDistributedFileSystem azureDistributedFileSystem) throws
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
   * @return Future<void> Future of a Void object.
   */
  Future<Void> setFilesystemPropertiesAsync(AzureDistributedFileSystem azureDistributedFileSystem, Hashtable<String, String> properties) throws
      AzureDistributedFileSystemException;

  /**
   * Gets path properties on the Azure service.
   * @param azureDistributedFileSystem filesystem to get the properties of the path.
   * @param path path to get properties.
   * @return Hashtable<String, String> hash table containing all the path properties.
   */
  Hashtable<String, String> getPathProperties(AzureDistributedFileSystem azureDistributedFileSystem, Path path) throws AzureDistributedFileSystemException;

  /**
   * Gets path properties on the Azure service asynchronously.
   * @param azureDistributedFileSystem filesystem to get the properties of the path.
   * @param path path to get properties.
   * @return Future<Hashtable<String, String>> Future of a hash table containing all the path properties.
   */
  Future<Hashtable<String, String>> getPathPropertiesAsync(AzureDistributedFileSystem azureDistributedFileSystem, Path path) throws
      AzureDistributedFileSystemException;

  /**
   * Sets path properties on the Azure service.
   * @param azureDistributedFileSystem filesystem to get the properties of the path.
   * @param path path to set properties.
   * @param properties hash table containing all the path properties.
   */
  void setPathProperties(AzureDistributedFileSystem azureDistributedFileSystem, Path path, Hashtable<String, String> properties) throws
      AzureDistributedFileSystemException;

  /**
   * Sets path properties on the Azure service asynchronously.
   * @param azureDistributedFileSystem filesystem to get the properties of the path.
   * @param path path to get properties.
   * @param properties hash table containing all the path properties.
   * @return Future<Void> Future of a Void object.
   */
  Future<Void> setPathPropertiesAsync(AzureDistributedFileSystem azureDistributedFileSystem, Path path, Hashtable<String, String> properties) throws
      AzureDistributedFileSystemException;

  /**
   * Creates filesystem on the Azure service.
   * @param azureDistributedFileSystem filesystem to be created.
   */
  void createFilesystem(AzureDistributedFileSystem azureDistributedFileSystem) throws AzureDistributedFileSystemException;

  /**
   * Creates filesystem on the Azure service asynchronously.
   * @param azureDistributedFileSystem filesystem to be created.
   * @return Future<void> Future of a Void object.
   */
  Future<Void> createFilesystemAsync(AzureDistributedFileSystem azureDistributedFileSystem) throws AzureDistributedFileSystemException;

  /**
   * Deletes filesystem on the Azure service.
   * @param azureDistributedFileSystem filesystem to be deleted.
   */
  void deleteFilesystem(AzureDistributedFileSystem azureDistributedFileSystem) throws AzureDistributedFileSystemException;

  /**
   * Deletes filesystem on the Azure service asynchronously.
   * @param azureDistributedFileSystem filesystem to be deleted.
   * @return Future<void> Future of a Void object.
   */
  Future<Void> deleteFilesystemAsync(AzureDistributedFileSystem azureDistributedFileSystem) throws AzureDistributedFileSystemException;

  /**
   * Creates a file on the Azure service.
   * @param azureDistributedFileSystem filesystem to create file or directory.
   * @param path path of the file to be created.
   * @param overwrite should overwrite.
   * @return OutputStream stream to the file.
   */
  OutputStream createFile(AzureDistributedFileSystem azureDistributedFileSystem, Path path, boolean overwrite) throws AzureDistributedFileSystemException;

  /**
   * Creates a file on the Azure service asynchronously.
   * @param azureDistributedFileSystem filesystem to create file or directory.
   * @param path path of the file to be created.
   * @param overwrite should overwrite.
   * @return Future<OutputStream> Future of a stream to the file.
   */
  Future<OutputStream> createFileAsync(AzureDistributedFileSystem azureDistributedFileSystem, Path path, boolean overwrite) throws
      AzureDistributedFileSystemException;

  /**
   * Creates a directory on the Azure service.
   * @param azureDistributedFileSystem filesystem to create file or directory.
   * @param path path of the directory to be created.
   * @return OutputStream stream to the file.
   */
  Void createDirectory(AzureDistributedFileSystem azureDistributedFileSystem, Path path) throws AzureDistributedFileSystemException;

  /**
   * Creates a directory on the Azure service asynchronously.
   * @param azureDistributedFileSystem filesystem to create file or directory.
   * @param path path of the file to be created.
   * @return Future<Void> Future of a Void object.
   */
  Future<Void> createDirectoryAsync(AzureDistributedFileSystem azureDistributedFileSystem, Path path) throws
      AzureDistributedFileSystemException;

  /**
   * Opens a file to read and returns the stream.
   * @param azureDistributedFileSystem filesystem to read a file from.
   * @param path file path to read.
   * @return InputStream a stream to the file to read.
   */
  InputStream openFileForRead(AzureDistributedFileSystem azureDistributedFileSystem, Path path) throws AzureDistributedFileSystemException;

  /**
   * Opens a file to read and returns the stream asynchronously.
   * @param azureDistributedFileSystem filesystem to read a file from.
   * @param path file path to read.
   * @return Future<InputStream> Future of a stream to the file to read.
   */
  Future<InputStream> openFileForReadAsync(AzureDistributedFileSystem azureDistributedFileSystem, Path path) throws AzureDistributedFileSystemException;

  /**
   * Opens a file to write and returns the stream.
   * @param azureDistributedFileSystem filesystem to write a file to.
   * @param path file path to write.
   * @param overwrite should overwrite.
   * @return OutputStream a stream to the file to write.
   */
  OutputStream openFileForWrite(AzureDistributedFileSystem azureDistributedFileSystem, Path path, boolean overwrite) throws AzureDistributedFileSystemException;

  /**
   * Opens a file to write and returns the stream asynchronously.
   * @param azureDistributedFileSystem filesystem to write a file to.
   * @param path file path to write.
   * @param overwrite should overwrite.
   * @return Future<OutputStream> Future of a stream to the file to write.
   */
  Future<OutputStream> openFileForWriteAsync(AzureDistributedFileSystem azureDistributedFileSystem, Path path, boolean overwrite) throws
      AzureDistributedFileSystemException;

  /**
   * Reads a file and returns the stream.
   * @param azureDistributedFileSystem filesystem to read a file from.
   * @param path file path to be read.
   * @param offset offset of the file to read
   * @param length the length of read operation
   * @param readBuffer buffer to read the file content.
   * @param readBufferOffset offset of the read buffer.
   * @return Integer total bytes read.
   */
  Integer readFile(
      AzureDistributedFileSystem azureDistributedFileSystem,
      Path path,
      String version,
      long offset,
      int length,
      ByteBuf readBuffer,
      int readBufferOffset) throws
      AzureDistributedFileSystemException;

  /**
   * Reads a file and returns the stream.
   * @param azureDistributedFileSystem filesystem to read a file from asynchronously.
   * @param path file path to be read.
   * @param offset offset of the file to read
   * @param length the length of read operation
   * @param readBuffer buffer to read the file content.
   * @param readBufferOffset offset of the read buffer.
   * @param version of the file.
   * @return Future<Integer> Future of total bytes read.
   */
  Future<Integer> readFileAsync(
      AzureDistributedFileSystem azureDistributedFileSystem,
      Path path,
      String version,
      long offset,
      int length,
      ByteBuf readBuffer,
      int readBufferOffset) throws
      AzureDistributedFileSystemException;

  /**
   * Writes a byte array to a file.
   * @param azureDistributedFileSystem filesystem to append data to file.
   * @param path path to append.
   * @param body the content to append to file.
   * @param offset offset to append.
   */
  void writeFile(AzureDistributedFileSystem azureDistributedFileSystem, Path path, ByteBuf body, long offset) throws AzureDistributedFileSystemException;

  /**
   * Writes a byte array to a file asynchronously.
   * @param azureDistributedFileSystem filesystem to append data to file.
   * @param path path to append.
   * @param body the content to append to file.
   * @param offset offset to append.
   * @return Future<void> Future of a Void object.
   */
  Future<Void> writeFileAsync(AzureDistributedFileSystem azureDistributedFileSystem, Path path, ByteBuf body, long offset) throws
      AzureDistributedFileSystemException;

  /**
   * Flushes the current pending appends to a file on the service.
   * @param azureDistributedFileSystem filesystem to flush.
   * @param path path of the file to be flushed.
   * @param offset offset to apply flush.
   * @param retainUncommitedData when flush is called out of order this parameter must be true.
   */
  void flushFile(AzureDistributedFileSystem azureDistributedFileSystem, Path path, final long offset, final boolean retainUncommitedData) throws
      AzureDistributedFileSystemException;

  /**
   * Flushes the current pending appends to a file on the service asynchronously.
   * @param azureDistributedFileSystem filesystem to flush.
   * @param path path of the file to be flushed.
   * @param offset offset to apply flush.
   * @param retainUncommitedData when flush is called out of order this parameter must be true.
   * @return Future<Void> Future of a Void object.
   */
  Future<Void> flushFileAsync(AzureDistributedFileSystem azureDistributedFileSystem, Path path, final long offset, final boolean retainUncommitedData) throws
      AzureDistributedFileSystemException;

  /**
   * Renames a file from source to destination.
   * @param azureDistributedFileSystem filesystem to rename a file.
   * @param source source path.
   * @param destination destination path.
   */
  void renameFile(AzureDistributedFileSystem azureDistributedFileSystem, Path source, Path destination)
      throws AzureDistributedFileSystemException;

  /**
   * Renames a file from source to destination asynchronously.
   * @param azureDistributedFileSystem filesystem to rename a file.
   * @param source source path.
   * @param destination destination path.
   * @return Future<Void> Future of a Void object.
   */
  Future<Void> renameFileAsync(AzureDistributedFileSystem azureDistributedFileSystem, Path source, Path destination) throws
      AzureDistributedFileSystemException;

  /**
   * Renames a directory from source to destination.
   * @param azureDistributedFileSystem filesystem to rename a directory.
   * @param source source path.
   * @param destination destination path.
   */
  void renameDirectory(AzureDistributedFileSystem azureDistributedFileSystem, Path source, Path destination)
      throws AzureDistributedFileSystemException;

  /**
   * Renames a directory from source to destination asynchronously.
   * @param azureDistributedFileSystem filesystem to rename a directory.
   * @param source source path.
   * @param destination destination path.
   */
  Future<Void> renameDirectoryAsync(AzureDistributedFileSystem azureDistributedFileSystem, Path source, Path destination) throws
      AzureDistributedFileSystemException;

  /**
   * Deletes a file.
   * @param azureDistributedFileSystem filesystem to delete the path.
   * @param path file path to be deleted.
   */
  void deleteFile(AzureDistributedFileSystem azureDistributedFileSystem, Path path) throws AzureDistributedFileSystemException;

  /**
   * Deletes a file asynchronously.
   * @param azureDistributedFileSystem filesystem to delete the path.
   * @param path file path to be deleted.
   * @return Future<Void> Future of a Void object.
   */
  Future<Void> deleteFileAsync(AzureDistributedFileSystem azureDistributedFileSystem, Path path)
      throws AzureDistributedFileSystemException;

  /**
   * Deletes a directory.
   * @param azureDistributedFileSystem filesystem to delete the path.
   * @param path directory path to be deleted.
   * @param recursive true if directory is being deleted recursively.
   */
  void deleteDirectory(AzureDistributedFileSystem azureDistributedFileSystem, Path path, boolean recursive) throws AzureDistributedFileSystemException;

  /**
   * Deletes a directory asynchronously.
   * @param azureDistributedFileSystem filesystem to delete the path.
   * @param path directory path to be deleted.
   * @param recursive true if directory is being deleted recursively.
   * @return Future<Void> Future of a Void object.
   */
  Future<Void> deleteDirectoryAsync(AzureDistributedFileSystem azureDistributedFileSystem, Path path, boolean recursive)
      throws AzureDistributedFileSystemException;

  /**
   * Gets path's status under the provided path on the Azure service.
   * @param azureDistributedFileSystem filesystem to perform the get file status operation.
   * @param path path delimiter.
   * @return FileStatus FileStatus of the path in the file system.
   */
  FileStatus getFileStatus(AzureDistributedFileSystem azureDistributedFileSystem, Path path) throws AzureDistributedFileSystemException;

  /**
   * Gets path's status under the provided path on the Azure service asynchronously.
   * @param azureDistributedFileSystem filesystem to perform the get file status operation.
   * @param path path delimiter.
   * @return Future<FileStatus> Future of FileStatus of the path in the file system.
   */
  Future<FileStatus> getFileStatusAsync(AzureDistributedFileSystem azureDistributedFileSystem, Path path) throws AzureDistributedFileSystemException;

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
   * @return Future<FileStatus[]> Future of a list of all paths in the file system.
   */
  Future<FileStatus[]> listStatusAsync(AzureDistributedFileSystem azureDistributedFileSystem, Path path) throws AzureDistributedFileSystemException;

  /**
   * Closes the client to filesystem to Azure service.
   * @param azureDistributedFileSystem filesystem to perform the list operation.
   */
  void closeFileSystem(AzureDistributedFileSystem azureDistributedFileSystem) throws AzureDistributedFileSystemException;
}