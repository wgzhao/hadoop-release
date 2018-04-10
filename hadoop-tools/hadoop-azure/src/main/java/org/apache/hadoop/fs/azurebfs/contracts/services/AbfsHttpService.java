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

package org.apache.hadoop.fs.azurebfs.contracts.services;

import java.io.InputStream;
import java.io.OutputStream;
import java.util.Hashtable;
import java.util.concurrent.Future;

import io.netty.buffer.ByteBuf;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.azurebfs.AzureBlobFileSystem;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.AzureBlobFileSystemException;

/**
 * File System http service to provide network calls for file system operations.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public interface AbfsHttpService extends InjectableService {
  /**
   * Gets filesystem properties on the Azure service.
   * @param azureBlobFileSystem filesystem to get the properties.
   * @return Hashtable<String, String> hash table containing all the filesystem properties.
   */
  Hashtable<String, String> getFilesystemProperties(AzureBlobFileSystem azureBlobFileSystem) throws AzureBlobFileSystemException;

  /**
   * Gets filesystem properties on the Azure service asynchronously.
   * @param azureBlobFileSystem filesystem to get the properties.
   * @return Future<Hashtable<String, String>> Future of a hash table containing the properties.
   */
  Future<Hashtable<String, String>> getFilesystemPropertiesAsync(AzureBlobFileSystem azureBlobFileSystem) throws
      AzureBlobFileSystemException;

  /**
   * Sets filesystem properties on the Azure service.
   * @param azureBlobFileSystem filesystem to get the properties.
   * @param properties file system properties to set.
   */
  void setFilesystemProperties(AzureBlobFileSystem azureBlobFileSystem, Hashtable<String, String> properties) throws
      AzureBlobFileSystemException;

  /**
   * Sets filesystem properties on the Azure service asynchronously.
   * @param azureBlobFileSystem filesystem to get the properties.
   * @param properties file system properties to set.
   * @return Future<void> Future of a Void object.
   */
  Future<Void> setFilesystemPropertiesAsync(AzureBlobFileSystem azureBlobFileSystem, Hashtable<String, String> properties) throws
      AzureBlobFileSystemException;

  /**
   * Gets path properties on the Azure service.
   * @param azureBlobFileSystem filesystem to get the properties of the path.
   * @param path path to get properties.
   * @return Hashtable<String, String> hash table containing all the path properties.
   */
  Hashtable<String, String> getPathProperties(AzureBlobFileSystem azureBlobFileSystem, Path path) throws AzureBlobFileSystemException;

  /**
   * Gets path properties on the Azure service asynchronously.
   * @param azureBlobFileSystem filesystem to get the properties of the path.
   * @param path path to get properties.
   * @return Future<Hashtable<String, String>> Future of a hash table containing all the path properties.
   */
  Future<Hashtable<String, String>> getPathPropertiesAsync(AzureBlobFileSystem azureBlobFileSystem, Path path) throws
      AzureBlobFileSystemException;

  /**
   * Sets path properties on the Azure service.
   * @param azureBlobFileSystem filesystem to get the properties of the path.
   * @param path path to set properties.
   * @param properties hash table containing all the path properties.
   */
  void setPathProperties(AzureBlobFileSystem azureBlobFileSystem, Path path, Hashtable<String, String> properties) throws
      AzureBlobFileSystemException;

  /**
   * Sets path properties on the Azure service asynchronously.
   * @param azureBlobFileSystem filesystem to get the properties of the path.
   * @param path path to get properties.
   * @param properties hash table containing all the path properties.
   * @return Future<Void> Future of a Void object.
   */
  Future<Void> setPathPropertiesAsync(AzureBlobFileSystem azureBlobFileSystem, Path path, Hashtable<String, String> properties) throws
      AzureBlobFileSystemException;

  /**
   * Creates filesystem on the Azure service.
   * @param azureBlobFileSystem filesystem to be created.
   */
  void createFilesystem(AzureBlobFileSystem azureBlobFileSystem) throws AzureBlobFileSystemException;

  /**
   * Creates filesystem on the Azure service asynchronously.
   * @param azureBlobFileSystem filesystem to be created.
   * @return Future<void> Future of a Void object.
   */
  Future<Void> createFilesystemAsync(AzureBlobFileSystem azureBlobFileSystem) throws AzureBlobFileSystemException;

  /**
   * Deletes filesystem on the Azure service.
   * @param azureBlobFileSystem filesystem to be deleted.
   */
  void deleteFilesystem(AzureBlobFileSystem azureBlobFileSystem) throws AzureBlobFileSystemException;

  /**
   * Deletes filesystem on the Azure service asynchronously.
   * @param azureBlobFileSystem filesystem to be deleted.
   * @return Future<void> Future of a Void object.
   */
  Future<Void> deleteFilesystemAsync(AzureBlobFileSystem azureBlobFileSystem) throws AzureBlobFileSystemException;

  /**
   * Creates a file on the Azure service.
   * @param azureBlobFileSystem filesystem to create file or directory.
   * @param path path of the file to be created.
   * @param overwrite should overwrite.
   * @return OutputStream stream to the file.
   */
  OutputStream createFile(AzureBlobFileSystem azureBlobFileSystem, Path path, boolean overwrite) throws AzureBlobFileSystemException;

  /**
   * Creates a file on the Azure service asynchronously.
   * @param azureBlobFileSystem filesystem to create file or directory.
   * @param path path of the file to be created.
   * @param overwrite should overwrite.
   * @return Future<OutputStream> Future of a stream to the file.
   */
  Future<OutputStream> createFileAsync(AzureBlobFileSystem azureBlobFileSystem, Path path, boolean overwrite) throws
      AzureBlobFileSystemException;

  /**
   * Creates a directory on the Azure service.
   * @param azureBlobFileSystem filesystem to create file or directory.
   * @param path path of the directory to be created.
   * @return OutputStream stream to the file.
   */
  Void createDirectory(AzureBlobFileSystem azureBlobFileSystem, Path path) throws AzureBlobFileSystemException;

  /**
   * Creates a directory on the Azure service asynchronously.
   * @param azureBlobFileSystem filesystem to create file or directory.
   * @param path path of the file to be created.
   * @return Future<Void> Future of a Void object.
   */
  Future<Void> createDirectoryAsync(AzureBlobFileSystem azureBlobFileSystem, Path path) throws
      AzureBlobFileSystemException;

  /**
   * Opens a file to read and returns the stream.
   * @param azureBlobFileSystem filesystem to read a file from.
   * @param path file path to read.
   * @return InputStream a stream to the file to read.
   */
  InputStream openFileForRead(AzureBlobFileSystem azureBlobFileSystem, Path path) throws AzureBlobFileSystemException;

  /**
   * Opens a file to read and returns the stream asynchronously.
   * @param azureBlobFileSystem filesystem to read a file from.
   * @param path file path to read.
   * @return Future<InputStream> Future of a stream to the file to read.
   */
  Future<InputStream> openFileForReadAsync(AzureBlobFileSystem azureBlobFileSystem, Path path) throws AzureBlobFileSystemException;

  /**
   * Opens a file to write and returns the stream.
   * @param azureBlobFileSystem filesystem to write a file to.
   * @param path file path to write.
   * @param overwrite should overwrite.
   * @return OutputStream a stream to the file to write.
   */
  OutputStream openFileForWrite(AzureBlobFileSystem azureBlobFileSystem, Path path, boolean overwrite) throws AzureBlobFileSystemException;

  /**
   * Opens a file to write and returns the stream asynchronously.
   * @param azureBlobFileSystem filesystem to write a file to.
   * @param path file path to write.
   * @param overwrite should overwrite.
   * @return Future<OutputStream> Future of a stream to the file to write.
   */
  Future<OutputStream> openFileForWriteAsync(AzureBlobFileSystem azureBlobFileSystem, Path path, boolean overwrite) throws
      AzureBlobFileSystemException;

  /**
   * Reads a file and returns the stream.
   * @param azureBlobFileSystem filesystem to read a file from.
   * @param path file path to be read.
   * @param offset offset of the file to read
   * @param length the length of read operation
   * @param readBuffer buffer to read the file content.
   * @param readBufferOffset offset of the read buffer.
   * @return Integer total bytes read.
   */
  Integer readFile(
      AzureBlobFileSystem azureBlobFileSystem,
      Path path,
      String version,
      long offset,
      int length,
      ByteBuf readBuffer,
      int readBufferOffset) throws
      AzureBlobFileSystemException;

  /**
   * Reads a file and returns the stream.
   * @param azureBlobFileSystem filesystem to read a file from asynchronously.
   * @param path file path to be read.
   * @param offset offset of the file to read
   * @param length the length of read operation
   * @param readBuffer buffer to read the file content.
   * @param readBufferOffset offset of the read buffer.
   * @param version of the file.
   * @return Future<Integer> Future of total bytes read.
   */
  Future<Integer> readFileAsync(
      AzureBlobFileSystem azureBlobFileSystem,
      Path path,
      String version,
      long offset,
      int length,
      ByteBuf readBuffer,
      int readBufferOffset) throws
      AzureBlobFileSystemException;

  /**
   * Writes a byte array to a file.
   * @param azureBlobFileSystem filesystem to append data to file.
   * @param path path to append.
   * @param body the content to append to file.
   * @param offset offset to append.
   */
  void writeFile(AzureBlobFileSystem azureBlobFileSystem, Path path, ByteBuf body, long offset) throws AzureBlobFileSystemException;

  /**
   * Writes a byte array to a file asynchronously.
   * @param azureBlobFileSystem filesystem to append data to file.
   * @param path path to append.
   * @param body the content to append to file.
   * @param offset offset to append.
   * @return Future<void> Future of a Void object.
   */
  Future<Void> writeFileAsync(AzureBlobFileSystem azureBlobFileSystem, Path path, ByteBuf body, long offset) throws
      AzureBlobFileSystemException;

  /**
   * Flushes the current pending appends to a file on the service.
   * @param azureBlobFileSystem filesystem to flush.
   * @param path path of the file to be flushed.
   * @param offset offset to apply flush.
   * @param retainUncommitedData when flush is called out of order this parameter must be true.
   */
  void flushFile(AzureBlobFileSystem azureBlobFileSystem, Path path, final long offset, final boolean retainUncommitedData) throws
      AzureBlobFileSystemException;

  /**
   * Flushes the current pending appends to a file on the service asynchronously.
   * @param azureBlobFileSystem filesystem to flush.
   * @param path path of the file to be flushed.
   * @param offset offset to apply flush.
   * @param retainUncommitedData when flush is called out of order this parameter must be true.
   * @return Future<Void> Future of a Void object.
   */
  Future<Void> flushFileAsync(AzureBlobFileSystem azureBlobFileSystem, Path path, final long offset, final boolean retainUncommitedData) throws
      AzureBlobFileSystemException;

  /**
   * Renames a file or directory from source to destination.
   * @param azureBlobFileSystem filesystem to rename a path.
   * @param source source path.
   * @param destination destination path.
   */
  void rename(AzureBlobFileSystem azureBlobFileSystem, Path source, Path destination)
      throws AzureBlobFileSystemException;

  /**
   * Renames a file or directory from source to destination asynchronously.
   * @param azureBlobFileSystem filesystem to rename a path.
   * @param source source path.
   * @param destination destination path.
   * @return Future<Void> Future of a Void object.
   */
  Future<Void> renameAsync(AzureBlobFileSystem azureBlobFileSystem, Path source, Path destination) throws
      AzureBlobFileSystemException;

  /**
   * Deletes a file or directory.
   * @param azureBlobFileSystem filesystem to delete the path.
   * @param path file path to be deleted.
   * @param recursive true if path is a directory and recursive deletion is desired.
   */
  void delete(AzureBlobFileSystem azureBlobFileSystem, Path path, boolean recursive) throws AzureBlobFileSystemException;

  /**
   * Deletes a file or directory asynchronously.
   * @param azureBlobFileSystem filesystem to delete the path.
   * @param path file path to be deleted.
   * @param recursive true if path is a directory and recursive deletion is desired.
   * @return Future<Void> Future of a Void object.
   */
  Future<Void> deleteAsync(AzureBlobFileSystem azureBlobFileSystem, Path path, boolean recursive)
      throws AzureBlobFileSystemException;

  /**
   * Gets path's status under the provided path on the Azure service.
   * @param azureBlobFileSystem filesystem to perform the get file status operation.
   * @param path path delimiter.
   * @return FileStatus FileStatus of the path in the file system.
   */
  FileStatus getFileStatus(AzureBlobFileSystem azureBlobFileSystem, Path path) throws AzureBlobFileSystemException;

  /**
   * Gets path's status under the provided path on the Azure service asynchronously.
   * @param azureBlobFileSystem filesystem to perform the get file status operation.
   * @param path path delimiter.
   * @return Future<FileStatus> Future of FileStatus of the path in the file system.
   */
  Future<FileStatus> getFileStatusAsync(AzureBlobFileSystem azureBlobFileSystem, Path path) throws AzureBlobFileSystemException;

  /**
   * Lists all the paths under the provided path on the Azure service.
   * @param azureBlobFileSystem filesystem to perform the list operation.
   * @param path path delimiter.
   * @return FileStatus[] list of all paths in the file system.
   */
  FileStatus[] listStatus(AzureBlobFileSystem azureBlobFileSystem, Path path) throws AzureBlobFileSystemException;

  /**
   * Lists all the paths under the provided path on the Azure service asynchronously.
   * @param azureBlobFileSystem filesystem to perform the list operation.
   * @param path path delimiter.
   * @return Future<FileStatus[]> Future of a list of all paths in the file system.
   */
  Future<FileStatus[]> listStatusAsync(AzureBlobFileSystem azureBlobFileSystem, Path path) throws AzureBlobFileSystemException;

  /**
   * Closes the client to filesystem to Azure service.
   * @param azureBlobFileSystem filesystem to perform the list operation.
   */
  void closeFileSystem(AzureBlobFileSystem azureBlobFileSystem) throws AzureBlobFileSystemException;

  /**
   * Checks for the given path if it is marked as atomic rename directory or not.
   * @param key
   * @return True if the given path is listed under atomic rename property otherwise False.
   */
  boolean isAtomicRenameKey(String key);
}