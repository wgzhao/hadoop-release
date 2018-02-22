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

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
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
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.ParentNotDirectoryException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.azuredfs.constants.FileSystemConfigurations;
import org.apache.hadoop.fs.azuredfs.constants.FileSystemUriSchemes;
import org.apache.hadoop.fs.azuredfs.contracts.exceptions.AzureDistributedFileSystemException;
import org.apache.hadoop.fs.azuredfs.contracts.exceptions.AzureServiceErrorResponseException;
import org.apache.hadoop.fs.azuredfs.contracts.exceptions.FileSystemOperationUnhandledException;
import org.apache.hadoop.fs.azuredfs.contracts.exceptions.InvalidUriAuthorityException;
import org.apache.hadoop.fs.azuredfs.contracts.exceptions.InvalidUriException;
import org.apache.hadoop.fs.azuredfs.contracts.services.AdfsHttpService;
import org.apache.hadoop.fs.azuredfs.contracts.services.AdfsStatisticsService;
import org.apache.hadoop.fs.azuredfs.contracts.services.AzureServiceErrorCode;
import org.apache.hadoop.fs.azuredfs.contracts.services.ConfigurationService;
import org.apache.hadoop.fs.azuredfs.contracts.services.LoggingService;
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
  private LoggingService loggingService;
  private AdfsHttpService adfsHttpService;
  private ConfigurationService configurationService;
  private AdfsStatisticsService adfsStatisticsService;

  @Override
  public void initialize(URI uri, Configuration configuration)
      throws IOException {
    uri = ensureAuthority(uri, configuration);
    super.initialize(uri, configuration);

    setConf(configuration);

    try {
      this.serviceProvider = ServiceProviderImpl.create(configuration);
      this.tracingService = serviceProvider.get(TracingService.class);
      this.adfsHttpService = serviceProvider.get(AdfsHttpService.class);
      this.loggingService = serviceProvider.get(LoggingService.class).get(AzureDistributedFileSystem.class);
      this.configurationService = serviceProvider.get(ConfigurationService.class);
      this.adfsStatisticsService = serviceProvider.get(AdfsStatisticsService.class);
    } catch (AzureDistributedFileSystemException exception) {
      throw new IOException(exception);
    }

    this.loggingService.debug(
        "Initializing AzureDistributedFileSystem for {0}", uri);

    this.uri = URI.create(uri.getScheme() + "://" + uri.getAuthority());
    this.userGroupInformation = UserGroupInformation.getCurrentUser();

    this.loggingService.debug(
        "Initializing NativeAzureFileSystem for {0}", uri);

    this.setWorkingDirectory(this.getHomeDirectory());
    this.createFileSystem();
    this.mkdirs(this.workingDir);

    if (statistics != null) {
      this.adfsStatisticsService.subscribe(this, statistics);
    }
  }

  @Override
  public URI getUri() {
    return this.uri;
  }

  @Override
  public FSDataInputStream open(final Path path, final int bufferSize) throws IOException {
    final AzureDistributedFileSystem azureDistributedFileSystem = this;

    this.loggingService.debug(
        "AzureDistributedFileSystem.open path: {0} bufferSize: {1}", path.toString(), bufferSize);

    final FileSystemOperation<InputStream> open = execute(
        "AzureDistributedFileSystem.open",
        new Callable<InputStream>() {
          @Override
          public InputStream call() throws Exception {
            return adfsHttpService.openFileForRead(
                azureDistributedFileSystem,
                makeQualified(path));
          }
        });

    evaluateFileSystemPathOperation(path, open);
    return new FSDataInputStream(open.result);
  }

  @Override
  public FSDataOutputStream create(final Path f, final FsPermission permission, final boolean overwrite, final int bufferSize,
      final short replication, final long blockSize, final Progressable progress) throws IOException {
    this.loggingService.debug(
        "AzureDistributedFileSystem.create path: {0} permission: {1} overwrite: {2} bufferSize: {3}",
        f.toString(),
        permission,
        overwrite,
        blockSize);

    final AzureDistributedFileSystem azureDistributedFileSystem = this;
    final FileSystemOperation<OutputStream> create = execute(
        "AzureDistributedFileSystem.create",
        new Callable<OutputStream>() {
          @Override
          public OutputStream call() throws Exception {
            return adfsHttpService.createFile(azureDistributedFileSystem, makeQualified(f), overwrite);
          }
        });

    evaluateFileSystemPathOperation(f, create);
    // Keep the second argument null. Statistics are already traced by adfsStatisticService.
    return new FSDataOutputStream(create.result, null);
  }

  @Override
  public FSDataOutputStream append(final Path f, final int bufferSize, final Progressable progress) throws IOException {
    final AzureDistributedFileSystem azureDistributedFileSystem = this;

    this.loggingService.debug(
        "AzureDistributedFileSystem.append path: {0} bufferSize: {1}",
        f.toString(),
        bufferSize);

    final FileSystemOperation<OutputStream> append = execute(
        "AzureDistributedFileSystem.append",
        new Callable<OutputStream>() {
          @Override
          public OutputStream call() throws Exception {
            return adfsHttpService.openFileForWrite(azureDistributedFileSystem, makeQualified(f), false);
          }
        });

    evaluateFileSystemPathOperation(f, append);
    return new FSDataOutputStream(append.result, null);
  }

  public boolean rename(final Path src, final Path dst) throws IOException {
    this.loggingService.debug(
        "AzureDistributedFileSystem.rename src: {0} dst: {1}", src.toString(), dst.toString());

    Path parentFolder = src.getParent();
    if (parentFolder == null) {
      return false;
    }

    final AzureDistributedFileSystem azureDistributedFileSystem = this;
    final FileStatus srcFileStatus = tryGetFileStatus(src);

    if (srcFileStatus == null) {
      return false;
    }

    final FileStatus dstFileStatus = tryGetFileStatus(dst);
    final FileSystemOperation<Boolean> rename = execute(
        "AzureDistributedFileSystem.rename",
        new Callable<Boolean>() {
          @Override
          public Boolean call() throws Exception {
            if (srcFileStatus.isDirectory()) {
              if (dstFileStatus == null) {
                adfsHttpService.renameDirectory(azureDistributedFileSystem, makeQualified(src), makeQualified(dst));
                return true;
              }

              if (!dstFileStatus.isDirectory()) {
                return false;
              }

              final String sourceDirectoryName = src.getName();
              final Path adjustedDst = new Path(dst, sourceDirectoryName);
              adfsHttpService.renameDirectory(azureDistributedFileSystem, makeQualified(src), makeQualified(adjustedDst));
              return true;
            }

            if (dstFileStatus == null) {
              adfsHttpService.renameFile(azureDistributedFileSystem, makeQualified(src), makeQualified(dst));
              return true;
            }

            if (dstFileStatus.isDirectory()) {
              String sourceFileName = src.getName();
              Path adjustedDst = new Path(dst, sourceFileName);
              adfsHttpService.renameFile(azureDistributedFileSystem, makeQualified(src), makeQualified(adjustedDst));
              return true;
            }

            return false;
          }
        }, false);

    evaluateFileSystemPathOperation(src, rename);
    return rename.result;
  }

  @Override
  public boolean delete(final Path f, final boolean recursive) throws IOException {
    this.loggingService.debug(
        "AzureDistributedFileSystem.delete path: {0} recursive: {1}", f.toString(), recursive);

    final AzureDistributedFileSystem azureDistributedFileSystem = this;
    final FileStatus fileStatus = tryGetFileStatus(f);

    if (fileStatus == null) {
      return false;
    }

    final FileSystemOperation<Boolean> delete = execute(
        "AzureDistributedFileSystem.delete",
        new Callable<Boolean>() {
          @Override
          public Boolean call() throws Exception {
            if (fileStatus.isDirectory()) {
              adfsHttpService.deleteDirectory(azureDistributedFileSystem, makeQualified(f), recursive);
            }
            else {
              adfsHttpService.deleteFile(azureDistributedFileSystem, makeQualified(f));
            }
            return true;
          }
        }, false);

    evaluateFileSystemPathOperation(f, delete);
    return delete.result;
  }

  @Override
  public FileStatus[] listStatus(final Path f) throws IOException {
    this.loggingService.debug(
        "AzureDistributedFileSystem.listStatus path: {0}", f.toString());

    final AzureDistributedFileSystem azureDistributedFileSystem = this;
    final FileSystemOperation<FileStatus[]> listStatus = execute(
        "AzureDistributedFileSystem.listStatus",
        new Callable<FileStatus[]>() {
          @Override
          public FileStatus[] call() throws Exception {
            return adfsHttpService.listStatus(azureDistributedFileSystem, makeQualified(f));
          }
        });

    evaluateFileSystemPathOperation(f, listStatus);
    return listStatus.result;
  }

  @Override
  public boolean mkdirs(final Path f, final FsPermission permission) throws IOException {
    this.loggingService.debug(
        "AzureDistributedFileSystem.mkdirs path: {0} permissions: {1}", f.toString(), permission);

    final AzureDistributedFileSystem azureDistributedFileSystem = this;
    final Path parentFolder = f.getParent();
    if (parentFolder != null && parentFolder.getParent() != null) { // skip root
      FileStatus fileStatus = tryGetFileStatus(f.getParent());
      if (fileStatus != null && !fileStatus.isDirectory()) {
        throw new ParentNotDirectoryException();
      }

      fileStatus = tryGetFileStatus(f);
      if (fileStatus != null && !fileStatus.isDirectory()) {
        throw new FileAlreadyExistsException();
      }
    }

    final FileSystemOperation<Boolean> mkdirs = execute(
        "AzureDistributedFileSystem.mkdirs",
        new Callable<Boolean>() {
          @Override
          public Boolean call() throws Exception {
            adfsHttpService.createDirectory(azureDistributedFileSystem, makeQualified(f));
            return true;
          }
        }, false);

    evaluateFileSystemPathOperation(f, mkdirs, AzureServiceErrorCode.PATH_CONFLICT);
    return mkdirs.result;
  }

  @Override
  public void close() throws IOException {
    this.loggingService.debug("AzureDistributedFileSystem.close");

    final AzureDistributedFileSystem azureDistributedFileSystem = this;
    final FileSystemOperation<Void> close = execute(
        "AzureDistributedFileSystem.close",
        new Callable<Void>() {
          @Override
          public Void call() throws Exception {
            adfsHttpService.closeFileSystem(azureDistributedFileSystem);
            return null;
          }
        });

    this.adfsStatisticsService.unsubscribe(this);
    evaluateFileSystemOperation(close);
  }

  @Override
  public FileStatus getFileStatus(final Path f) throws IOException {
    this.loggingService.debug("AzureDistributedFileSystem.getFileStatus path: {0}", f.toString());

    final AzureDistributedFileSystem azureDistributedFileSystem = this;
    final FileSystemOperation<FileStatus> getFileStatus = execute(
        "AzureDistributedFileSystem.getFileStatus",
        new Callable<FileStatus>() {
          @Override
          public FileStatus call() throws Exception {
            return adfsHttpService.getFileStatus(azureDistributedFileSystem, makeQualified(f));
          }
        });

    evaluateFileSystemPathOperation(f, getFileStatus);
    return getFileStatus.result;
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

  /**
   * Return an array containing hostnames, offset and size of
   * portions of the given file. For ADFS we'll just lie and give
   * fake hosts to make sure we get many splits in MR jobs.
   */
  @Override
  public BlockLocation[] getFileBlockLocations(FileStatus file,
      long start, long len) throws IOException {
    if (file == null) {
      return null;
    }

    if ((start < 0) || (len < 0)) {
      throw new IllegalArgumentException("Invalid start or len parameter");
    }

    if (file.getLen() < start) {
      return new BlockLocation[0];
    }
    final String blobLocationHost = this.configurationService.getAzureBlockLocationHost();

    final String[] name = { blobLocationHost };
    final String[] host = { blobLocationHost };
    long blockSize = file.getBlockSize();
    if (blockSize <= 0) {
      throw new IllegalArgumentException(
          "The block size for the given file is not a positive number: "
              + blockSize);
    }
    int numberOfLocations = (int) (len / blockSize)
        + ((len % blockSize == 0) ? 0 : 1);
    BlockLocation[] locations = new BlockLocation[numberOfLocations];
    for (int i = 0; i < locations.length; i++) {
      long currentOffset = start + (i * blockSize);
      long currentLength = Math.min(blockSize, start + len - currentOffset);
      locations[i] = new BlockLocation(name, host, currentOffset, currentLength);
    }

    return locations;
  }

  private FileStatus tryGetFileStatus(final Path f) {
    try {
      return getFileStatus(f);
    }
    catch (IOException ex) {
      this.loggingService.debug("File not found {0}", f.toString());
      return null;
    }
  }

  private void createFileSystem() throws IOException {
    this.loggingService.debug(
        "AzureDistributedFileSystem.createFileSystem uri: {0}", uri);

    final AzureDistributedFileSystem azureDistributedFileSystem = this;
    final FileSystemOperation<Void> createFileSystem = execute("AzureDistributedFileSystem.createFileSystem",
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
    return execute(scopeDescription, callableFileOperation, null);
  }

  @VisibleForTesting
  <T> FileSystemOperation execute(
      final String scopeDescription,
      final Callable<T> callableFileOperation,
      T defaultResultValue) throws IOException {

    final TraceScope traceScope = tracingService.traceBegin(scopeDescription);
    try {
      final T executionResult = callableFileOperation.call();
      return new FileSystemOperation(executionResult, null);
    } catch (AzureServiceErrorResponseException azureServiceErrorResponseException) {
      return new FileSystemOperation(defaultResultValue, azureServiceErrorResponseException);
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
    evaluateFileSystemPathOperation(null, fileSystemOperation, whitelistedErrorCodes);
  }

  private <T> void evaluateFileSystemPathOperation(
      final Path path,
      final FileSystemOperation<T> fileSystemOperation,
      final AzureServiceErrorCode... whitelistedErrorCodes) throws IOException {
    if (fileSystemOperation.failed()) {
      if (!ArrayUtils.contains(whitelistedErrorCodes, fileSystemOperation.exception.getErrorCode())) {
        if (fileSystemOperation.exception.getStatusCode() == HttpURLConnection.HTTP_NOT_FOUND) {
          throw new FileNotFoundException(path == null ? null : path.toString());
        }

        throw new IOException(fileSystemOperation.exception);
      }
    }
  }

  @VisibleForTesting
  FileSystem.Statistics getFsStatistics() { return this.statistics; }

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