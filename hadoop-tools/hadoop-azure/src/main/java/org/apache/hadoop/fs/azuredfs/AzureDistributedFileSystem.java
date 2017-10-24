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


package org.apache.hadoop.fs.azuredfs;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Paths;
import java.util.concurrent.Callable;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;

import org.apache.commons.lang.ArrayUtils;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.azuredfs.constants.FileSystemConfigurations;
import org.apache.hadoop.fs.azuredfs.constants.FileSystemUriSchemes;
import org.apache.hadoop.fs.azuredfs.contracts.exceptions.AzureDistributedFileSystemException;
import org.apache.hadoop.fs.azuredfs.contracts.exceptions.AzureServiceErrorResponseException;
import org.apache.hadoop.fs.azuredfs.contracts.exceptions.FileSystemOperationUnhandledException;
import org.apache.hadoop.fs.azuredfs.contracts.exceptions.InvalidUriAuthorityException;
import org.apache.hadoop.fs.azuredfs.contracts.exceptions.InvalidUriException;
import org.apache.hadoop.fs.azuredfs.contracts.services.AdfsHttpService;
import org.apache.hadoop.fs.azuredfs.contracts.services.AzureServiceErrorCode;
import org.apache.hadoop.fs.azuredfs.contracts.services.ServiceProvider;
import org.apache.hadoop.fs.azuredfs.contracts.services.TracingService;
import org.apache.hadoop.fs.azuredfs.services.ServiceProviderImpl;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.Progressable;
import org.apache.htrace.core.TraceScope;

/**
 * A {@link org.apache.hadoop.fs.FileSystem} for reading and writing files stored on <a
 * href="http://store.azure.com/">Windows Azure</a>
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class AzureDistributedFileSystem extends FileSystem {
  private URI uri;
  private Path workingDir;
  private UserGroupInformation userGroupInformation;
  private ServiceProvider serviceProvider;
  private TracingService tracingService;
  private AdfsHttpService adfsHttpService;

  @Override
  public void initialize(URI uri, Configuration configuration)
      throws IOException {
    uri = ensureAuthority(uri, configuration);
    super.initialize(uri, configuration);

    setConf(configuration);
    this.uri = URI.create(uri.getScheme() + "://" + uri.getAuthority());
    this.userGroupInformation = UserGroupInformation.getCurrentUser();
    this.setWorkingDirectory(this.getHomeDirectory());

    this.serviceProvider = ServiceProviderImpl.create(configuration);

    try {
      this.tracingService = serviceProvider.get(TracingService.class);
      this.adfsHttpService = serviceProvider.get(AdfsHttpService.class);
    } catch (AzureDistributedFileSystemException exception) {
      throw new IOException(exception);
    }

    this.createFileSystem();
    this.mkdirs(this.workingDir);
  }

  @Override
  public URI getUri() {
    return this.uri;
  }

  @Override
  public FSDataInputStream open(final Path path, final int bufferSize) throws IOException {
    final AzureDistributedFileSystem azureDistributedFileSystem = this;
    FileSystemOperation<FSDataInputStream> open = this.execute("AzureDistributedFileSystem.open",
        new Callable<FSDataInputStream>() {
          @Override
          public FSDataInputStream call() throws Exception {
            return new FSDataInputStream(adfsHttpService.readPath(
                azureDistributedFileSystem,
                makeQualified(path)));
          }
        });

    evaluateFileSystemOperation(open);
    return open.result;
  }

  @Override
  public FSDataOutputStream create(final Path f, final FsPermission permission, boolean overwrite, final int bufferSize,
      final short replication, final long blockSize, final Progressable progress) throws IOException {
    final AzureDistributedFileSystem azureDistributedFileSystem = this;
    FileSystemOperation<FSDataOutputStream> create = this.execute("AzureDistributedFileSystem.create",
        new Callable<FSDataOutputStream>() {
          @Override
          public FSDataOutputStream call() throws Exception {
            adfsHttpService.createPath(azureDistributedFileSystem, makeQualified(f), false);
            return null;
          }
        });

    evaluateFileSystemOperation(create);
    return create.result;
  }

  @Override
  public FSDataOutputStream append(final Path f, final int bufferSize, final Progressable progress) throws IOException {
    throw new UnsupportedOperationException();
  }

  public boolean rename(final Path src, final Path dst) throws IOException {
    final AzureDistributedFileSystem azureDistributedFileSystem = this;
    FileSystemOperation<Void> rename = this.execute(
        "AzureDistributedFileSystem.rename",
        new Callable<Void>() {
          @Override
          public Void call() throws Exception {
            // handle directory rename later.
            adfsHttpService.renamePath(azureDistributedFileSystem, makeQualified(src), makeQualified(dst), false);
            return null;
          }
        });

    evaluateFileSystemOperation(rename);
    return !rename.failed();
  }

  @Override
  public boolean delete(final Path f, final boolean recursive) throws IOException {
    final AzureDistributedFileSystem azureDistributedFileSystem = this;
    FileSystemOperation<Void> delete = this.execute("AzureDistributedFileSystem.delete",
        new Callable<Void>() {
          @Override
          public Void call() throws Exception {
            // handle directory rename later.
            adfsHttpService.deletePath(azureDistributedFileSystem, makeQualified(f), false);
            return null;
          }
        });

    evaluateFileSystemOperation(delete);
    return !delete.failed();
  }

  @Override
  public FileStatus[] listStatus(final Path f) throws IOException {
    final AzureDistributedFileSystem azureDistributedFileSystem = this;
    FileSystemOperation<FileStatus[]> listStatus = this.execute(
        "AzureDistributedFileSystem.listStatus",
        new Callable<FileStatus[]>() {
          @Override
          public FileStatus[] call() throws Exception {
            return adfsHttpService.listStatus(azureDistributedFileSystem, makeQualified(f));
          }
        });

    evaluateFileSystemOperation(listStatus);
    return listStatus.result;
  }

  @Override
  public boolean mkdirs(final Path f, final FsPermission permission) throws IOException {
    final AzureDistributedFileSystem azureDistributedFileSystem = this;
    FileSystemOperation<Void> mkdirs = this.execute(
        "AzureDistributedFileSystem.create",
        new Callable<Void>() {
          @Override
          public Void call() throws Exception {
            adfsHttpService.createPath(azureDistributedFileSystem, makeQualified(f), true);
            return null;
          }
        });

    evaluateFileSystemOperation(mkdirs, AzureServiceErrorCode.PATH_CONFLICT);
    return !mkdirs.failed();
  }

  @Override
  public void close() throws IOException {
    final AzureDistributedFileSystem azureDistributedFileSystem = this;
    FileSystemOperation<Void> close = this.execute("AzureDistributedFileSystem.create",
        new Callable<Void>() {
          @Override
          public Void call() throws Exception {
            adfsHttpService.closeFileSystem(azureDistributedFileSystem);
            return null;
          }
        });

    evaluateFileSystemOperation(close);
  }

  @Override
  public FileStatus getFileStatus(final Path f) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public Path getWorkingDirectory() {
    return this.workingDir;
  }

  @Override
  public void setWorkingDirectory(final Path newDir) {
    this.workingDir = newDir;
  }

  @Override
  public String getScheme() {
    return FileSystemUriSchemes.ADFS_SCHEME;
  }

  @Override
  public Path getHomeDirectory() {
    return makeQualified(
        new Path(Paths.get(
            FileSystemConfigurations.USER_HOME_DIRECTORY_PREFIX,
            this.userGroupInformation.getShortUserName()).toString()));
  }

  private void createFileSystem() throws IOException {
    final AzureDistributedFileSystem azureDistributedFileSystem = this;
    FileSystemOperation<Void> createFileSystem = this.execute("AzureDistributedFileSystem.listStatus",
        new Callable<Void>() {
          @Override
          public Void call() throws Exception {
            adfsHttpService.createFilesystem(azureDistributedFileSystem);
            return null;
          }
        });

    evaluateFileSystemOperation(createFileSystem, AzureServiceErrorCode.FILE_SYSTEM_ALREADY_EXISTS);
  }

  private URI ensureAuthority(URI uri, final Configuration conf) {

    Preconditions.checkNotNull(uri, "uri");

    if (uri.getAuthority() == null) {
      final URI defaultUri = FileSystem.getDefaultUri(conf);

      if (defaultUri != null && isAdfsScheme(defaultUri.getScheme())) {
        try {
          // Reconstruct the URI with the authority from the default URI.
          uri = new URI(
              uri.getScheme(),
              defaultUri.getAuthority(),
              uri.getPath(),
              uri.getQuery(),
              uri.getFragment());
        } catch (URISyntaxException e) {
          // This should never happen.
          throw new IllegalArgumentException(new InvalidUriException(uri.toString()));
        }
      }
    }

    if (uri.getAuthority() == null) {
      throw new IllegalArgumentException(new InvalidUriAuthorityException(uri.toString()));
    }

    return uri;
  }

  private boolean isAdfsScheme(final String scheme) {
    if (scheme == null) {
      return false;
    }

    for (String adfsScheme : FileSystemUriSchemes.ADFS_SCHEMES) {
      if (scheme.equalsIgnoreCase(adfsScheme)) {
        return true;
      }
    }

    return false;
  }

  @VisibleForTesting
  <T> FileSystemOperation execute(
      final String scopeDescription,
      final Callable<T> callableFileOperation) throws IOException {

    final TraceScope traceScope = tracingService.traceBegin(scopeDescription);
    try {
      final T executionResult = callableFileOperation.call();
      return new FileSystemOperation(executionResult, null);
    } catch (AzureServiceErrorResponseException azureServiceErrorResponseException) {
      return new FileSystemOperation(null, azureServiceErrorResponseException);
    } catch (AzureDistributedFileSystemException azureDistributedFileSystemException) {
      tracingService.traceException(traceScope, azureDistributedFileSystemException);
      throw new IOException(azureDistributedFileSystemException);
    } catch (Exception exception) {
      final FileSystemOperationUnhandledException fileSystemOperationUnhandledException = new FileSystemOperationUnhandledException(exception);
      tracingService.traceException(traceScope, fileSystemOperationUnhandledException);
      throw new IOException(fileSystemOperationUnhandledException);
    } finally {
      tracingService.traceEnd(traceScope);
    }
  }

  private <T> void evaluateFileSystemOperation(
      final FileSystemOperation<T> fileSystemOperation,
      final AzureServiceErrorCode... whitelistedErrorCodes) throws IOException {
    if (fileSystemOperation.failed()) {
      if (!ArrayUtils.contains(whitelistedErrorCodes, fileSystemOperation.exception.getErrorCode())) {
        throw new IOException(fileSystemOperation.exception);
      }
    }
  }

  @VisibleForTesting
  class FileSystemOperation<T> {
    private final T result;
    private final AzureServiceErrorResponseException exception;

    public FileSystemOperation(final T result, final AzureServiceErrorResponseException exception) {
      this.result = result;
      this.exception = exception;
    }

    public boolean failed() {
      return this.exception != null;
    }
  }
}