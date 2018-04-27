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

package org.apache.hadoop.fs.azurebfs;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import org.threadly.util.ExceptionUtils;

import org.apache.commons.lang.ArrayUtils;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.CreateFlag;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.azurebfs.constants.FileSystemConfigurations;
import org.apache.hadoop.fs.azurebfs.constants.FileSystemUriSchemes;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.AzureBlobFileSystemException;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.AzureServiceErrorResponseException;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.FileSystemOperationUnhandledException;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.InvalidUriAuthorityException;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.InvalidUriException;
import org.apache.hadoop.fs.azurebfs.contracts.services.AbfsHttpService;
import org.apache.hadoop.fs.azurebfs.contracts.services.AbfsStatisticsService;
import org.apache.hadoop.fs.azurebfs.contracts.services.AzureServiceErrorCode;
import org.apache.hadoop.fs.azurebfs.contracts.services.ConfigurationService;
import org.apache.hadoop.fs.azurebfs.contracts.services.LoggingService;
import org.apache.hadoop.fs.azurebfs.contracts.services.ServiceProvider;
import org.apache.hadoop.fs.azurebfs.contracts.services.TracingService;
import org.apache.hadoop.fs.azurebfs.services.ServiceProviderImpl;
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
public class AzureBlobFileSystem extends FileSystem {
  private URI uri;
  private Path workingDir;
  private UserGroupInformation userGroupInformation;
  private String user;
  private String primaryUserGroup;
  private ServiceProvider serviceProvider;
  private TracingService tracingService;
  private LoggingService loggingService;
  private AbfsHttpService abfsHttpService;
  private ConfigurationService configurationService;
  private AbfsStatisticsService abfsStatisticsService;
  private boolean isClosed;

  @Override
  public void initialize(URI uri, Configuration configuration)
      throws IOException {
    uri = ensureAuthority(uri, configuration);
    super.initialize(uri, configuration);

    setConf(configuration);

    try {
      this.serviceProvider = ServiceProviderImpl.create(configuration);
      this.tracingService = serviceProvider.get(TracingService.class);
      this.abfsHttpService = serviceProvider.get(AbfsHttpService.class);
      this.loggingService = serviceProvider.get(LoggingService.class).get(AzureBlobFileSystem.class);
      this.configurationService = serviceProvider.get(ConfigurationService.class);
      this.abfsStatisticsService = serviceProvider.get(AbfsStatisticsService.class);
    } catch (AzureBlobFileSystemException exception) {
      throw new IOException(exception);
    }

    this.loggingService.debug(
        "Initializing AzureBlobFileSystem for {0}", uri);

    this.uri = URI.create(uri.getScheme() + "://" + uri.getAuthority());
    this.userGroupInformation = UserGroupInformation.getCurrentUser();
    this.user = userGroupInformation.getUserName();
    this.primaryUserGroup = userGroupInformation.getPrimaryGroupName();

    this.loggingService.debug(
        "Initializing NativeAzureFileSystem for {0}", uri);

    this.setWorkingDirectory(this.getHomeDirectory());

    if (this.configurationService.getCreateRemoteFileSystemDuringInitialization()) {
      this.createFileSystem();
    }

    this.mkdirs(this.workingDir);

    if (statistics != null) {
      this.abfsStatisticsService.subscribe(this, statistics);
    }
  }

  public boolean isSecure() {
    return false;
  }

  @Override
  public URI getUri() {
    return this.uri;
  }

  @Override
  public FSDataInputStream open(final Path path, final int bufferSize) throws IOException {
    final AzureBlobFileSystem azureBlobFileSystem = this;

    this.loggingService.debug(
        "AzureBlobFileSystem.open path: {0} bufferSize: {1}", path.toString(), bufferSize);

    final FileSystemOperation<InputStream> open = execute(
        "AzureBlobFileSystem.open",
        new Callable<InputStream>() {
          @Override
          public InputStream call() throws Exception {
            return abfsHttpService.openFileForRead(
                azureBlobFileSystem,
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
        "AzureBlobFileSystem.create path: {0} permission: {1} overwrite: {2} bufferSize: {3}",
        f.toString(),
        permission,
        overwrite,
        blockSize);

    final AzureBlobFileSystem azureBlobFileSystem = this;
    final FileSystemOperation<OutputStream> create = execute(
        "AzureBlobFileSystem.create",
        new Callable<OutputStream>() {
          @Override
          public OutputStream call() throws Exception {
            return abfsHttpService.createFile(azureBlobFileSystem, makeQualified(f), overwrite);
          }
        });

    evaluateFileSystemPathOperation(f, create);
    // Keep the second argument null. Statistics are already traced by abfsStatisticService.
    return new FSDataOutputStream(create.result, null);
  }

  @Override
  @SuppressWarnings("deprecation")
  public FSDataOutputStream createNonRecursive(final Path f, final FsPermission permission,
      final boolean overwrite, final int bufferSize, final short replication, final long blockSize,
      final Progressable progress) throws IOException {

    final Path parent = f.getParent();
    final FileStatus parentFileStatus = tryGetFileStatus(parent);

    if (parentFileStatus == null) {
      throw new FileNotFoundException("Cannot create file "
          + f.getName() + " because parent folder does not exist.");
    }

    return create(f, permission, overwrite, bufferSize, replication, blockSize, progress);
  }

  @Override
  @SuppressWarnings("deprecation")
  public FSDataOutputStream createNonRecursive(final Path f, final FsPermission permission,
      final EnumSet<CreateFlag> flags, final int bufferSize, final short replication, final long blockSize,
      final Progressable progress) throws IOException {

    // Check if file should be appended or overwritten. Assume that the file
    // is overwritten on if the CREATE and OVERWRITE create flags are set.
    final EnumSet<CreateFlag> createflags =
        EnumSet.of(CreateFlag.CREATE, CreateFlag.OVERWRITE);
    final boolean overwrite = flags.containsAll(createflags);

    // Delegate the create non-recursive call.
    return this.createNonRecursive(f, permission, overwrite,
        bufferSize, replication, blockSize, progress);
  }

  @Override
  @SuppressWarnings("deprecation")
  public FSDataOutputStream createNonRecursive(final Path f,
      final boolean overwrite, final int bufferSize, final short replication, final long blockSize,
      final Progressable progress) throws IOException {
    return this.createNonRecursive(f, FsPermission.getFileDefault(),
        overwrite, bufferSize, replication, blockSize, progress);
  }

  @Override
  public FSDataOutputStream append(final Path f, final int bufferSize, final Progressable progress) throws IOException {
    final AzureBlobFileSystem azureBlobFileSystem = this;

    this.loggingService.debug(
        "AzureBlobFileSystem.append path: {0} bufferSize: {1}",
        f.toString(),
        bufferSize);

    final FileSystemOperation<OutputStream> append = execute(
        "AzureBlobFileSystem.append",
        new Callable<OutputStream>() {
          @Override
          public OutputStream call() throws Exception {
            return abfsHttpService.openFileForWrite(azureBlobFileSystem, makeQualified(f), false);
          }
        });

    evaluateFileSystemPathOperation(f, append);
    return new FSDataOutputStream(append.result, null);
  }

  public boolean rename(final Path src, final Path dst) throws IOException {
    this.loggingService.debug(
        "AzureBlobFileSystem.rename src: {0} dst: {1}", src.toString(), dst.toString());

    Path parentFolder = src.getParent();
    if (parentFolder == null) {
      return false;
    }

    final AzureBlobFileSystem azureBlobFileSystem = this;
    final FileStatus dstFileStatus = tryGetFileStatus(dst);
    final FileSystemOperation<Boolean> rename = execute(
        "AzureBlobFileSystem.rename",
        new Callable<Boolean>() {
          @Override
          public Boolean call() throws Exception {
            String sourceFileName = src.getName();
            Path adjustedDst = dst;

            if (dstFileStatus != null) {
              if (!dstFileStatus.isDirectory()) {
                return src.equals(dst);
              }

              adjustedDst = new Path(dst, sourceFileName);
            }

            abfsHttpService.rename(azureBlobFileSystem, makeQualified(src), makeQualified(adjustedDst));
            return true;
          }
        }, false);

    evaluateFileSystemPathOperation(
        src,
        rename,
        AzureServiceErrorCode.PATH_ALREADY_EXISTS,
        AzureServiceErrorCode.INVALID_RENAME_SOURCE_PATH,
        AzureServiceErrorCode.SOURCE_PATH_NOT_FOUND,
        AzureServiceErrorCode.INVALID_SOURCE_OR_DESTINATION_RESOURCE_TYPE,
        AzureServiceErrorCode.RENAME_DESTINATION_PARENT_PATH_NOT_FOUND);

    return rename.result;
  }

  @Override
  public boolean delete(final Path f, final boolean recursive) throws IOException {
    this.loggingService.debug(
        "AzureBlobFileSystem.delete path: {0} recursive: {1}", f.toString(), recursive);

    if (f.isRoot()) {
      if (!recursive) {
        return false;
      }

      return deleteRoot();
    }

    final AzureBlobFileSystem azureBlobFileSystem = this;
    final FileSystemOperation<Boolean> delete = execute(
        "AzureBlobFileSystem.delete",
        new Callable<Boolean>() {
          @Override
          public Boolean call() throws Exception {
            abfsHttpService.delete(azureBlobFileSystem, makeQualified(f), recursive);
            return true;
          }
        }, false);

    evaluateFileSystemPathOperation(
        f,
        delete,
        AzureServiceErrorCode.PATH_NOT_FOUND);
    return delete.result;
  }

  @Override
  public FileStatus[] listStatus(final Path f) throws IOException {
    this.loggingService.debug(
        "AzureBlobFileSystem.listStatus path: {0}", f.toString());

    final AzureBlobFileSystem azureBlobFileSystem = this;
    final FileSystemOperation<FileStatus[]> listStatus = execute(
        "AzureBlobFileSystem.listStatus",
        new Callable<FileStatus[]>() {
          @Override
          public FileStatus[] call() throws Exception {
            return abfsHttpService.listStatus(azureBlobFileSystem, makeQualified(f));
          }
        });

    evaluateFileSystemPathOperation(f, listStatus);
    return listStatus.result;
  }

  @Override
  public boolean mkdirs(final Path f, final FsPermission permission) throws IOException {
    this.loggingService.debug(
        "AzureBlobFileSystem.mkdirs path: {0} permissions: {1}", f.toString(), permission);

    final AzureBlobFileSystem azureBlobFileSystem = this;
    final Path parentFolder = f.getParent();
    if (parentFolder == null) {
      // Cannot create root
      return true;
    }

    final FileSystemOperation<Boolean> mkdirs = execute(
        "AzureBlobFileSystem.mkdirs",
        new Callable<Void>() {
          @Override
          public Void call() throws Exception {
            abfsHttpService.createDirectory(azureBlobFileSystem, makeQualified(f));
            return null;
          }
        });

    evaluateFileSystemPathOperation(f, mkdirs, AzureServiceErrorCode.PATH_ALREADY_EXISTS);
    return true;
  }

  @Override
  public synchronized void close() throws IOException {
    if (isClosed) {
      return;
    }

    super.close();
    this.loggingService.debug("AzureBlobFileSystem.close");

    final AzureBlobFileSystem azureBlobFileSystem = this;
    final FileSystemOperation<Void> close = execute(
        "AzureBlobFileSystem.close",
        new Callable<Void>() {
          @Override
          public Void call() throws Exception {
            abfsHttpService.closeFileSystem(azureBlobFileSystem);
            return null;
          }
        });

    this.abfsStatisticsService.unsubscribe(this);
    evaluateFileSystemOperation(close);
    this.isClosed = true;
  }

  @Override
  public FileStatus getFileStatus(final Path f) throws IOException {
    this.loggingService.debug("AzureBlobFileSystem.getFileStatus path: {0}", f.toString());

    final AzureBlobFileSystem azureBlobFileSystem = this;
    final FileSystemOperation<FileStatus> getFileStatus = execute(
        "AzureBlobFileSystem.getFileStatus",
        new Callable<FileStatus>() {
          @Override
          public FileStatus call() throws Exception {
            return abfsHttpService.getFileStatus(azureBlobFileSystem, makeQualified(f));
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
    if (newDir.isAbsolute()) {
      this.workingDir = newDir;
    } else {
      this.workingDir = new Path(workingDir, newDir);
    }
  }

  @Override
  public String getScheme() {
    return FileSystemUriSchemes.ABFS_SCHEME;
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
   * portions of the given file. For ABFS we'll just lie and give
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

  public String getOwnerUser() {
    return user;
  }

  public String getOwnerUserPrimaryGroup() {
    return primaryUserGroup;
  }

  private boolean deleteRoot() throws IOException {
    this.loggingService.debug("Deleting root content");

    final ExecutorService executorService = Executors.newFixedThreadPool(10);

    try {
      final FileStatus[] ls = listStatus(makeQualified(new Path(File.separator)));
      final ArrayList<Future> deleteTasks = new ArrayList<>();
      for (final FileStatus fs : ls) {
        final Future deleteTask = executorService.submit(new Callable<Void>() {
          @Override
          public Void call() throws Exception {
            delete(fs.getPath(), fs.isDirectory());
            return null;
          }
        });
        deleteTasks.add(deleteTask);
      }

      for (final Future deleteTask : deleteTasks) {
        execute("deleteRoot", new Callable<Void>() {
          @Override
          public Void call() throws Exception {
            deleteTask.get();
            return null;
          }
        });
      }
    }
    finally {
      executorService.shutdownNow();
    }

    return true;
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
        "AzureBlobFileSystem.createFileSystem uri: {0}", uri);

    final AzureBlobFileSystem azureBlobFileSystem = this;
    final FileSystemOperation<Void> createFileSystem = execute("AzureBlobFileSystem.createFileSystem",
        new Callable<Void>() {
          @Override
          public Void call() throws Exception {
            abfsHttpService.createFilesystem(azureBlobFileSystem);
            return null;
          }
        });

    evaluateFileSystemOperation(createFileSystem, AzureServiceErrorCode.FILE_SYSTEM_ALREADY_EXISTS);
  }

  private URI ensureAuthority(URI uri, final Configuration conf) {

    Preconditions.checkNotNull(uri, "uri");

    if (uri.getAuthority() == null) {
      final URI defaultUri = FileSystem.getDefaultUri(conf);

      if (defaultUri != null && isAbfsScheme(defaultUri.getScheme())) {
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

  private boolean isAbfsScheme(final String scheme) {
    if (scheme == null) {
      return false;
    }

    for (String abfsScheme : FileSystemUriSchemes.ABFS_SCHEMES) {
      if (scheme.equalsIgnoreCase(abfsScheme)) {
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
    } catch (AzureBlobFileSystemException azureBlobFileSystemException) {
      tracingService.traceException(traceScope, azureBlobFileSystemException);
      throw new IOException(azureBlobFileSystemException);
    } catch (Exception exception) {
      if (exception instanceof ExecutionException) {
        exception = (Exception) ExceptionUtils.getRootCause(exception);
      }

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
        if (fileSystemOperation.exception.getStatusCode() == HttpURLConnection.HTTP_CONFLICT) {
          throw new FileAlreadyExistsException(path == null ? null : path.toString());
        }

        throw new IOException(fileSystemOperation.exception);
      }
    }
  }

  @VisibleForTesting
  FileSystem.Statistics getFsStatistics() {
    return this.statistics;
  }

  @VisibleForTesting
  class FileSystemOperation<T> {
    private final T result;
    private final AzureServiceErrorResponseException exception;

    FileSystemOperation(final T result, final AzureServiceErrorResponseException exception) {
      this.result = result;
      this.exception = exception;
    }

    public boolean failed() {
      return this.exception != null;
    }
  }
}