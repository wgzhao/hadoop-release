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

package org.apache.hadoop.fs.azuredfs.services;

import java.io.InputStream;
import java.io.OutputStream;
import java.util.Hashtable;
import java.util.concurrent.Future;

import com.google.inject.Singleton;
import io.netty.buffer.ByteBuf;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.azuredfs.AzureDistributedFileSystem;
import org.apache.hadoop.fs.azuredfs.contracts.exceptions.AzureDistributedFileSystemException;
import org.apache.hadoop.fs.azuredfs.contracts.services.AdfsHttpService;

@Singleton
public class MockAdfsHttpImpl implements AdfsHttpService {
  public FileStatus fileStatus;

  @Override
  public Hashtable<String, String> getFilesystemProperties(AzureDistributedFileSystem azureDistributedFileSystem) throws AzureDistributedFileSystemException {
    return null;
  }

  @Override
  public Future<Hashtable<String, String>> getFilesystemPropertiesAsync(AzureDistributedFileSystem azureDistributedFileSystem) throws AzureDistributedFileSystemException {
    return null;
  }

  @Override
  public void setFilesystemProperties(AzureDistributedFileSystem azureDistributedFileSystem, Hashtable<String, String> properties) throws AzureDistributedFileSystemException {

  }

  @Override
  public Future<Void> setFilesystemPropertiesAsync(AzureDistributedFileSystem azureDistributedFileSystem, Hashtable<String, String> properties) throws AzureDistributedFileSystemException {
    return null;
  }

  @Override
  public Hashtable<String, String> getPathProperties(AzureDistributedFileSystem azureDistributedFileSystem, Path path) throws AzureDistributedFileSystemException {
    return null;
  }

  @Override
  public Future<Hashtable<String, String>> getPathPropertiesAsync(AzureDistributedFileSystem azureDistributedFileSystem, Path path) throws AzureDistributedFileSystemException {
    return null;
  }

  @Override
  public void setPathProperties(AzureDistributedFileSystem azureDistributedFileSystem, Path path, Hashtable<String, String> properties) throws AzureDistributedFileSystemException {

  }

  @Override
  public Future<Void> setPathPropertiesAsync(AzureDistributedFileSystem azureDistributedFileSystem, Path path, Hashtable<String, String> properties) throws AzureDistributedFileSystemException {
    return null;
  }

  @Override
  public void createFilesystem(AzureDistributedFileSystem azureDistributedFileSystem) throws AzureDistributedFileSystemException {

  }

  @Override
  public Future<Void> createFilesystemAsync(AzureDistributedFileSystem azureDistributedFileSystem) throws AzureDistributedFileSystemException {
    return null;
  }

  @Override
  public void deleteFilesystem(AzureDistributedFileSystem azureDistributedFileSystem) throws AzureDistributedFileSystemException {

  }

  @Override
  public Future<Void> deleteFilesystemAsync(AzureDistributedFileSystem azureDistributedFileSystem) throws AzureDistributedFileSystemException {
    return null;
  }

  @Override
  public OutputStream createFile(AzureDistributedFileSystem azureDistributedFileSystem, Path path, boolean overwrite) throws AzureDistributedFileSystemException {
    return null;
  }

  @Override
  public Future<OutputStream> createFileAsync(AzureDistributedFileSystem azureDistributedFileSystem, Path path, boolean overwrite) throws AzureDistributedFileSystemException {
    return null;
  }

  @Override
  public Void createDirectory(AzureDistributedFileSystem azureDistributedFileSystem, Path path) throws AzureDistributedFileSystemException {
    return null;
  }

  @Override
  public Future<Void> createDirectoryAsync(AzureDistributedFileSystem azureDistributedFileSystem, Path path) throws AzureDistributedFileSystemException {
    return null;
  }

  @Override
  public InputStream openFileForRead(AzureDistributedFileSystem azureDistributedFileSystem, Path path) throws AzureDistributedFileSystemException {
    return null;
  }

  @Override
  public Future<InputStream> openFileForReadAsync(AzureDistributedFileSystem azureDistributedFileSystem, Path path) throws AzureDistributedFileSystemException {
    return null;
  }

  @Override
  public OutputStream openFileForWrite(AzureDistributedFileSystem azureDistributedFileSystem, Path path, boolean overwrite) throws AzureDistributedFileSystemException {
    return null;
  }

  @Override
  public Future<OutputStream> openFileForWriteAsync(AzureDistributedFileSystem azureDistributedFileSystem, Path path, boolean overwrite) throws
      AzureDistributedFileSystemException {
    return null;
  }

  @Override
  public Integer readFile(AzureDistributedFileSystem azureDistributedFileSystem, Path path, String version, long offset, int length, ByteBuf readBuffer, int
      readBufferOffset) throws AzureDistributedFileSystemException {
    return null;
  }

  @Override
  public Future<Integer> readFileAsync(AzureDistributedFileSystem azureDistributedFileSystem, Path path, String version, long offset, int length, ByteBuf
      readBuffer, int readBufferOffset) throws AzureDistributedFileSystemException {
    return null;
  }

  @Override
  public void writeFile(AzureDistributedFileSystem azureDistributedFileSystem, Path path, ByteBuf body, long offset) throws AzureDistributedFileSystemException {

  }

  @Override
  public Future<Void> writeFileAsync(AzureDistributedFileSystem azureDistributedFileSystem, Path path, ByteBuf body, long offset) throws AzureDistributedFileSystemException {
    return null;
  }

  @Override
  public void flushFile(AzureDistributedFileSystem azureDistributedFileSystem, Path path, long offset, boolean retainUncommitedData) throws AzureDistributedFileSystemException {

  }

  @Override
  public Future<Void> flushFileAsync(AzureDistributedFileSystem azureDistributedFileSystem, Path path, long offset, boolean retainUncommitedData) throws AzureDistributedFileSystemException {
    return null;
  }

  @Override
  public void renameFile(AzureDistributedFileSystem azureDistributedFileSystem, Path source, Path destination) throws AzureDistributedFileSystemException {

  }

  @Override
  public Future<Void> renameFileAsync(AzureDistributedFileSystem azureDistributedFileSystem, Path source, Path destination) throws AzureDistributedFileSystemException {
    return null;
  }

  @Override
  public void renameDirectory(AzureDistributedFileSystem azureDistributedFileSystem, Path source, Path destination) throws AzureDistributedFileSystemException {

  }

  @Override
  public Future<Void> renameDirectoryAsync(AzureDistributedFileSystem azureDistributedFileSystem, Path source, Path destination) throws AzureDistributedFileSystemException {
    return null;
  }

  @Override
  public void deleteFile(AzureDistributedFileSystem azureDistributedFileSystem, Path path) throws AzureDistributedFileSystemException {

  }

  @Override
  public Future<Void> deleteFileAsync(AzureDistributedFileSystem azureDistributedFileSystem, Path path) throws AzureDistributedFileSystemException {
    return null;
  }

  @Override
  public void deleteDirectory(AzureDistributedFileSystem azureDistributedFileSystem, Path path, boolean recursive) throws AzureDistributedFileSystemException {

  }

  @Override
  public Future<Void> deleteDirectoryAsync(AzureDistributedFileSystem azureDistributedFileSystem, Path path, boolean recursive) throws AzureDistributedFileSystemException {
    return null;
  }

  @Override
  public FileStatus getFileStatus(AzureDistributedFileSystem azureDistributedFileSystem, Path path) throws AzureDistributedFileSystemException {
    return fileStatus;
  }

  @Override
  public Future<FileStatus> getFileStatusAsync(AzureDistributedFileSystem azureDistributedFileSystem, Path path) throws AzureDistributedFileSystemException {
    return null;
  }

  @Override
  public FileStatus[] listStatus(AzureDistributedFileSystem azureDistributedFileSystem, Path path) throws AzureDistributedFileSystemException {
    return new FileStatus[0];
  }

  @Override
  public Future<FileStatus[]> listStatusAsync(AzureDistributedFileSystem azureDistributedFileSystem, Path path) throws AzureDistributedFileSystemException {
    return null;
  }

  @Override
  public void closeFileSystem(AzureDistributedFileSystem azureDistributedFileSystem) throws AzureDistributedFileSystemException {

  }
}
