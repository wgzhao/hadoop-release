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

import com.google.inject.Singleton;
import rx.Observable;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.azuredfs.AzureDistributedFileSystem;
import org.apache.hadoop.fs.azuredfs.contracts.exceptions.AzureDistributedFileSystemException;
import org.apache.hadoop.fs.azuredfs.contracts.services.AdfsHttpService;

@Singleton
public class MockAdfsHttpImpl implements AdfsHttpService {
  @Override
  public Hashtable<String, String> getFilesystemProperties(AzureDistributedFileSystem azureDistributedFileSystem) throws AzureDistributedFileSystemException {
    return null;
  }

  @Override
  public Observable<Hashtable<String, String>> getFilesystemPropertiesAsync(AzureDistributedFileSystem azureDistributedFileSystem) throws AzureDistributedFileSystemException {
    return null;
  }

  @Override
  public void setFilesystemProperties(AzureDistributedFileSystem azureDistributedFileSystem, Hashtable<String, String> properties) throws AzureDistributedFileSystemException {

  }

  @Override
  public Observable<Void> setFilesystemPropertiesAsync(AzureDistributedFileSystem azureDistributedFileSystem, Hashtable<String, String> properties) throws AzureDistributedFileSystemException {
    return null;
  }

  @Override
  public void createFilesystem(AzureDistributedFileSystem azureDistributedFileSystem) throws AzureDistributedFileSystemException {

  }

  @Override
  public Observable<Void> createFilesystemAsync(AzureDistributedFileSystem azureDistributedFileSystem) throws AzureDistributedFileSystemException {
    return null;
  }

  @Override
  public void deleteFilesystem(AzureDistributedFileSystem azureDistributedFileSystem) throws AzureDistributedFileSystemException {

  }

  @Override
  public Observable<Void> deleteFilesystemAsync(AzureDistributedFileSystem azureDistributedFileSystem) throws AzureDistributedFileSystemException {
    return null;
  }

  @Override
  public OutputStream createPath(AzureDistributedFileSystem azureDistributedFileSystem, Path path, boolean isDirectory) throws AzureDistributedFileSystemException {
    return null;
  }

  @Override
  public Observable<OutputStream> createPathAsync(AzureDistributedFileSystem azureDistributedFileSystem, Path path, boolean isDirectory) throws
      AzureDistributedFileSystemException {
    return null;
  }

  @Override
  public InputStream readPath(AzureDistributedFileSystem azureDistributedFileSystem, Path path) throws AzureDistributedFileSystemException {
    return null;
  }

  @Override
  public Observable<InputStream> readPathAsync(AzureDistributedFileSystem azureDistributedFileSystem, Path path) throws AzureDistributedFileSystemException {
    return null;
  }

  @Override
  public void updatePath(AzureDistributedFileSystem azureDistributedFileSystem, Path path, boolean isDirectory) throws AzureDistributedFileSystemException {

  }

  @Override
  public Observable<Void> updatePathAsync(AzureDistributedFileSystem azureDistributedFileSystem, Path path, boolean isDirectory) throws
      AzureDistributedFileSystemException {
    return null;
  }

  @Override
  public void renamePath(AzureDistributedFileSystem azureDistributedFileSystem, Path source, Path destination, boolean isDirectory) throws AzureDistributedFileSystemException {

  }

  @Override
  public Observable<Void> renamePathAsync(AzureDistributedFileSystem azureDistributedFileSystem, Path source, Path destination, boolean isDirectory) throws
      AzureDistributedFileSystemException {
    return null;
  }

  @Override
  public void deletePath(AzureDistributedFileSystem azureDistributedFileSystem, Path path, boolean isDirectory) throws AzureDistributedFileSystemException {

  }

  @Override
  public Observable<Void> deletePathAsync(AzureDistributedFileSystem azureDistributedFileSystem, Path path, boolean isDirectory) throws AzureDistributedFileSystemException {
    return null;
  }

  @Override
  public FileStatus[] listStatus(AzureDistributedFileSystem azureDistributedFileSystem, Path path) throws AzureDistributedFileSystemException {
    return new FileStatus[0];
  }

  @Override
  public Observable<FileStatus[]> listStatusAsync(AzureDistributedFileSystem azureDistributedFileSystem, Path path) throws AzureDistributedFileSystemException {
    return null;
  }

  @Override
  public void closeFileSystem(AzureDistributedFileSystem azureDistributedFileSystem) throws AzureDistributedFileSystemException {

  }
}
