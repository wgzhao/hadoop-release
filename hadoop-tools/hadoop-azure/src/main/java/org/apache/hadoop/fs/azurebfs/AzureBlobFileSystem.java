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
import java.util.Arrays;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;

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
import org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants;
import org.apache.hadoop.fs.azurebfs.constants.FileSystemConfigurations;
import org.apache.hadoop.fs.azurebfs.constants.FileSystemUriSchemes;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.AzureBlobFileSystemException;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.AzureServiceErrorResponseException;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.FileSystemOperationUnhandledException;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.InvalidUriAuthorityException;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.InvalidUriException;
import org.apache.hadoop.fs.azurebfs.contracts.services.AbfsHttpClientFactory;
import org.apache.hadoop.fs.azurebfs.contracts.services.AbfsHttpService;
import org.apache.hadoop.fs.azurebfs.contracts.services.AzureServiceErrorCode;
import org.apache.hadoop.fs.azurebfs.contracts.services.ConfigurationService;
import org.apache.hadoop.fs.azurebfs.contracts.services.LoggingService;
import org.apache.hadoop.fs.azurebfs.contracts.services.ServiceProvider;
import org.apache.hadoop.fs.azurebfs.security.AbfsDelegationTokenManager;
import org.apache.hadoop.fs.azurebfs.services.AbfsClient;
import org.apache.hadoop.fs.azurebfs.services.ConfigurationServiceImpl;
import org.apache.hadoop.fs.azurebfs.services.ServiceProviderImpl;
import org.apache.hadoop.fs.permission.AclEntry;
import org.apache.hadoop.fs.permission.AclStatus;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.util.Progressable;

/**
 * A {@link org.apache.hadoop.fs.FileSystem} for reading and writing files stored on <a
 * href="http://store.azure.com/">Windows Azure</a>
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class AzureBlobFileSystem extends FileSystem {
  private URI uri;
  private String fsUuid;
  private Path workingDir;
  private UserGroupInformation userGroupInformation;
  private String user;
  private String primaryUserGroup;
  private ServiceProvider serviceProvider;
  private LoggingService loggingService;
  private AbfsHttpService abfsHttpService;
  private ConfigurationService configurationService;
  private AbfsClient abfsClient;
  private Set<String> azureAtomicRenameDirSet;
  private boolean isClosed;
  private boolean enableAclBit;

  private boolean delegationTokenEnabled = false;
  private AbfsDelegationTokenManager delegationTokenManager;

  @Override
  public void initialize(URI uri, Configuration configuration)
      throws IOException {
    uri = ensureAuthority(uri, configuration);
    super.initialize(uri, configuration);

    setConf(configuration);

    this.uri = URI.create(uri.getScheme() + "://" + uri.getAuthority());
    this.userGroupInformation = UserGroupInformation.getCurrentUser();
    this.user = userGroupInformation.getUserName();
    this.setWorkingDirectory(this.getHomeDirectory());

    try {
      this.fsUuid = UUID.randomUUID().toString();
      this.serviceProvider = ServiceProviderImpl.create();
      this.abfsHttpService = serviceProvider.get(AbfsHttpService.class);
      this.loggingService = serviceProvider.get(LoggingService.class).get(AzureBlobFileSystem.class);
      this.configurationService = new ConfigurationServiceImpl(configuration, this.loggingService);
      this.abfsClient = serviceProvider.get(AbfsHttpClientFactory.class).create(this);

      this.azureAtomicRenameDirSet = new HashSet<>(Arrays.asList(configurationService.getAzureAtomicRenameDirs().split(AbfsHttpConstants.COMMA)));
    } catch (AzureBlobFileSystemException | IllegalAccessException exception) {
      throw new IOException(exception);
    }

    this.loggingService.debug(
        "Initializing AzureBlobFileSystem for {0}", uri);

    if (this.configurationService.getCreateRemoteFileSystemDuringInitialization()) {
      if (!this.fileSystemExists()) {
        this.createFileSystem();
      }
    }

    if (!this.configurationService.getSkipUserGroupMetadataDuringInitialization()) {
      this.primaryUserGroup = userGroupInformation.getPrimaryGroupName();
    } else {
      //Provide a default group name
      this.primaryUserGroup = this.user;
    }

    this.enableAclBit = this.configurationService.isAclBitEnabled();

    if (UserGroupInformation.isSecurityEnabled()) {
      this.delegationTokenEnabled = this.configurationService.isDelegationTokenManagerEnabled();

      if(this.delegationTokenEnabled) {
        this.delegationTokenManager = this.configurationService.getDelegationTokenManager();
      }
    }
  }

  public boolean isSecure() {
    return false;
  }

  public ConfigurationService getConfigurationService() {
    return configurationService;
  }

  public AbfsClient getClient() {
    return abfsClient;
  }

  public boolean isAtomicRenameKey(final String key) {
    return isKeyForDirectorySet(key, azureAtomicRenameDirSet);
  }

  public boolean isAclBitEnabled() { return enableAclBit; }

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
                makeQualified(path),
                statistics);
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
            return abfsHttpService.createFile(azureBlobFileSystem, makeQualified(f), overwrite, getValueOrDefault(permission, true),
                FsPermission.getUMask(getConf()));
          }
        });

    evaluateFileSystemPathOperation(f, create);
    return new FSDataOutputStream(create.result, statistics);
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
    return new FSDataOutputStream(append.result, statistics);
  }

  public boolean rename(final Path src, final Path dst) throws IOException {
    this.loggingService.debug(
        "AzureBlobFileSystem.rename src: {0} dst: {1}", src.toString(), dst.toString());

    Path parentFolder = src.getParent();
    if (parentFolder == null) {
      return false;
    }

    final AzureBlobFileSystem azureBlobFileSystem = this;

    final FileStatus dstFileStatus = azureBlobFileSystem.getClient().isAccountNamespaceEnabled()
        ? null : tryGetFileStatus(dst); // if namespace enabled, skip this call

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
            abfsHttpService.createDirectory(azureBlobFileSystem, makeQualified(f), getValueOrDefault(permission, false),
                FsPermission.getUMask(getConf()));
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

  @Override
  protected void finalize() throws Throwable {
    this.loggingService.debug("finalize() called.");
    close();
    super.finalize();
  }

  public String getOwnerUser() {
    return user;
  }

  public String getOwnerUserPrimaryGroup() {
    return primaryUserGroup;
  }

  public String getFsUuid() {
    return this.fsUuid;
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

  /**
   * Set owner of a path (i.e. a file or a directory).
   * The parameters owner and group cannot both be null.
   *
   * @param path  The path
   * @param owner If it is null, the original username remains unchanged.
   * @param group If it is null, the original groupname remains unchanged.
   */
  @Override
  public void setOwner(final Path path, final String owner, final String group)
      throws IOException {
    this.loggingService.debug(
        "AzureBlobFileSystem.setOwner path: {0}", path.toString());

    if((owner == null || owner.isEmpty()) && (group == null || group.isEmpty())) {
      throw new IllegalArgumentException("The parameters owner and group cannot both be blank");
    }

    final AzureBlobFileSystem azureBlobFileSystem = this;
    final FileSystemOperation<Void> setOwner = execute(
        "AzureBlobFileSystem.setOwner",
        new Callable<Void>() {
          @Override
          public Void call() throws Exception {
            abfsHttpService.setOwner(azureBlobFileSystem, makeQualified(path),
                owner,
                group);
            return null;
          }
        });

    evaluateFileSystemPathOperation(path, setOwner);
  }

  /**
   * Set permission of a path.
   *
   * @param path       The path
   * @param permission Access permission
   */
  @Override
  public void setPermission(final Path path, final FsPermission permission)
      throws IOException {
    this.loggingService.debug("AzureBlobFileSystem.setPermission path: {0}", path.toString());

    if(permission == null) {
      throw new IllegalArgumentException("The permission can't be null");
    }

    final AzureBlobFileSystem azureBlobFileSystem = this;
    final FileSystemOperation<Void> setPermission = execute(
        "AzureBlobFileSystem.setPermission",
        new Callable<Void>() {
          @Override
          public Void call() throws Exception {
            abfsHttpService.setPermission(azureBlobFileSystem, makeQualified(path), permission);
            return null;
          }
        });

    evaluateFileSystemPathOperation(path, setPermission);
  }

  /**
   * Modifies ACL entries of files and directories.  This method can add new ACL
   * entries or modify the permissions on existing ACL entries.  All existing
   * ACL entries that are not specified in this call are retained without
   * changes.  (Modifications are merged into the current ACL.)
   *
   * @param path    Path to modify
   * @param aclSpec List of AbfsAclEntry describing modifications
   * @throws IOException if an ACL could not be modified
   */
  @Override
  public void modifyAclEntries(final Path path, final List<AclEntry> aclSpec)
      throws IOException {
    this.loggingService.debug("AzureBlobFileSystem.modifyAclEntries path: {0}", path.toString());

    final AzureBlobFileSystem azureBlobFileSystem = this;

    if(aclSpec == null || aclSpec.isEmpty()) {
      throw new IllegalArgumentException("The aclSpec can't be blank");
    }

    final FileSystemOperation<Void> modifyAclEntries = execute(
        "AzureBlobFileSystem.modifyAclEntries",
        new Callable<Void>() {
          @Override
          public Void call() throws Exception {
            abfsHttpService.modifyAclEntries(azureBlobFileSystem, makeQualified(path), aclSpec);
            return null;
          }
        });

    evaluateFileSystemPathOperation(path, modifyAclEntries);
  }

  /**
   * Removes ACL entries from files and directories.  Other ACL entries are
   * retained.
   *
   * @param path    Path to modify
   * @param aclSpec List of AclEntry describing entries to remove
   * @throws IOException if an ACL could not be modified
   */
  @Override
  public void removeAclEntries(final Path path, final List<AclEntry> aclSpec)
      throws IOException {
    this.loggingService.debug("AzureBlobFileSystem.removeAclEntries path: {0}", path.toString());

    final AzureBlobFileSystem azureBlobFileSystem = this;

    if(aclSpec == null || aclSpec.isEmpty()) {
      throw new IllegalArgumentException("The aclSpec can't be blank");
    }

    final FileSystemOperation<Void> removeAclEntries = execute(
        "AzureBlobFileSystem.removeAclEntries",
        new Callable<Void>() {
          @Override
          public Void call() throws Exception {
            abfsHttpService.removeAclEntries(azureBlobFileSystem, makeQualified(path), aclSpec);
            return null;
          }
        });

    evaluateFileSystemPathOperation(path, removeAclEntries);
  }

  /**
   * Removes all default ACL entries from files and directories.
   *
   * @param path Path to modify
   * @throws IOException if an ACL could not be modified
   */
  @Override
  public void removeDefaultAcl(final Path path)
      throws IOException {
    this.loggingService.debug("AzureBlobFileSystem.removeDefaultAcl path: {0}", path.toString());

    final AzureBlobFileSystem azureBlobFileSystem = this;

    final FileSystemOperation<Void> removeDefaultAcl = execute(
        "AzureBlobFileSystem.removeDefaultAcl",
        new Callable<Void>() {
          @Override
          public Void call() throws Exception {
            abfsHttpService.removeDefaultAcl(azureBlobFileSystem, makeQualified(path));
            return null;
          }
        });

    evaluateFileSystemPathOperation(path, removeDefaultAcl);
  }

  /**
   * Removes all but the base ACL entries of files and directories.  The entries
   * for user, group, and others are retained for compatibility with permission
   * bits.
   *
   * @param path Path to modify
   * @throws IOException if an ACL could not be removed
   */
  @Override
  public void removeAcl(final Path path) throws IOException {
    this.loggingService.debug("AzureBlobFileSystem.removeAcl path: {0}", path.toString());

    final AzureBlobFileSystem azureBlobFileSystem = this;

    final FileSystemOperation<Void> removeAcl = execute(
        "AzureBlobFileSystem.removeAcl",
        new Callable<Void>() {
          @Override
          public Void call() throws Exception {
            abfsHttpService.removeAcl(azureBlobFileSystem, makeQualified(path));
            return null;
          }
        });

    evaluateFileSystemPathOperation(path, removeAcl);
  }

  /**
   * Fully replaces ACL of files and directories, discarding all existing
   * entries.
   *
   * @param path    Path to modify
   * @param aclSpec List of AclEntry describing modifications, must include
   *                entries for user, group, and others for compatibility with
   *                permission bits.
   * @throws IOException if an ACL could not be modified
   */
  @Override
  public void setAcl(final Path path, final List<AclEntry> aclSpec)
      throws IOException {
    this.loggingService.debug("AzureBlobFileSystem.setAcl path: {0}", path.toString());

    final AzureBlobFileSystem azureBlobFileSystem = this;

    if(aclSpec == null || aclSpec.size() == 0) {
      throw new IllegalArgumentException("The aclSpec can't be blank");
    }

    final FileSystemOperation<Boolean> setAcl = execute(
        "AzureBlobFileSystem.setAcl",
        new Callable<Void>() {
          @Override
          public Void call() throws Exception {
            abfsHttpService.setAcl(azureBlobFileSystem, makeQualified(path), aclSpec);
            return null;
          }
        });

    evaluateFileSystemPathOperation(path, setAcl);
  }

  /**
   * Gets the ACL of a file or directory.
   *
   * @param path Path to get
   * @return AbfsAclStatus describing the ACL of the file or directory
   * @throws IOException if an ACL could not be read
   */
  @Override
  public AclStatus getAclStatus(final Path path) throws IOException {
    this.loggingService.debug("AzureBlobFileSystem.getAclStatus path: {0}", path.toString());

    final AzureBlobFileSystem azureBlobFileSystem = this;

    final FileSystemOperation<AclStatus> getAclStatus = execute(
        "AzureBlobFileSystem.getAclStatus",
        new Callable<AclStatus>() {
          @Override
          public AclStatus call() throws Exception {
            return abfsHttpService.getAclStatus(azureBlobFileSystem, makeQualified(path));
          }
        });

    evaluateFileSystemPathOperation(path, getAclStatus);
    return getAclStatus.result;

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

  private boolean fileSystemExists() throws IOException {
    this.loggingService.debug(
        "AzureBlobFileSystem.fileSystemExists uri: {0}", uri);

    final AzureBlobFileSystem azureBlobFileSystem = this;
    final FileSystemOperation<Void> fileSystemExists = execute("AzureBlobFileSystem.fileSystemExists",
        new Callable<Void>() {
          @Override
          public Void call() throws Exception {
            abfsHttpService.getFilesystemProperties(azureBlobFileSystem);
            return null;
          }
        });

    return !fileSystemExists.failed();
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

    evaluateFileSystemOperation(createFileSystem);
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
    try {
      final T executionResult = callableFileOperation.call();
      return new FileSystemOperation(executionResult, null);
    } catch (AzureServiceErrorResponseException azureServiceErrorResponseException) {
      return new FileSystemOperation(defaultResultValue, azureServiceErrorResponseException);
    } catch (AzureBlobFileSystemException azureBlobFileSystemException) {
      throw new IOException(azureBlobFileSystemException);
    } catch (Exception exception) {
      if (exception instanceof ExecutionException) {
        exception = (Exception) getRootCause(exception);
      }
      final FileSystemOperationUnhandledException fileSystemOperationUnhandledException = new FileSystemOperationUnhandledException(exception);
      throw new IOException(fileSystemOperationUnhandledException);
    }
  }

  private <T> void evaluateFileSystemOperation(
      final FileSystemOperation<T> fileSystemOperation,
      final AzureServiceErrorCode... whitelistedErrorCodes) throws IOException {
    evaluateFileSystemPathOperation(makeQualified(new Path("/")), fileSystemOperation, whitelistedErrorCodes);
  }

  private <T> void evaluateFileSystemPathOperation(
      final Path path,
      final FileSystemOperation<T> fileSystemOperation,
      final AzureServiceErrorCode... whitelistedErrorCodes) throws IOException {
    if (fileSystemOperation.failed()) {
      if (!ArrayUtils.contains(whitelistedErrorCodes, fileSystemOperation.exception.getErrorCode())) {
        if (fileSystemOperation.exception.getStatusCode() == HttpURLConnection.HTTP_NOT_FOUND) {
          throw new FileNotFoundException(String.format("%s not found, %n %s", makeQualified(path), fileSystemOperation.exception.getMessage()));
        }
        if (fileSystemOperation.exception.getStatusCode() == HttpURLConnection.HTTP_CONFLICT) {
          throw new FileAlreadyExistsException(String.format("%s already exists, %n %s", makeQualified(path), fileSystemOperation.exception.getMessage()));
        }

        throw new IOException(fileSystemOperation.exception);
      }
    }
  }

  private boolean isKeyForDirectorySet(String key, Set<String> dirSet) {
    for (String dir : dirSet) {
      if (dir.isEmpty() || key.startsWith(dir + AbfsHttpConstants.FORWARD_SLASH)) {
        return true;
      }

      try {
        URI uri = new URI(dir);
        if (null == uri.getAuthority()) {
          if (key.startsWith(dir + "/")){
            return true;
          }
        }
      } catch (URISyntaxException e) {
        this.loggingService.info("URI syntax error creating URI for {}", dir);
      }
    }

    return false;
  }

  private FsPermission getValueOrDefault(FsPermission permission, final boolean isFile) {
    if (permission == null) {
      permission = isFile ? FsPermission.getFileDefault() : FsPermission.getDirDefault();
    }

    return permission;
  }

  /**
   * Gets the root cause of a provided {@link Throwable}.  If there is no cause for the
   * {@link Throwable} provided into this function, the original {@link Throwable} is returned.
   *
   * @param throwable starting {@link Throwable}
   * @return root cause {@link Throwable}
   */
  public static Throwable getRootCause(Throwable throwable) {
    if (throwable == null) {
      throw new IllegalArgumentException("throwable can not be null");
    }

    Throwable result = throwable;
    while (result.getCause() != null) {
      result = result.getCause();
    }

    return result;
  }

  /**
   * Get a delegation token from remote service endpoint if
   * 'fs.azure.enable.kerberos.support' is set to 'true'.
   * @param renewer the account name that is allowed to renew the token.
   * @return delegation token
   * @throws IOException thrown when getting the current user.
   */
  @Override
  public synchronized Token<?> getDelegationToken(final String renewer) throws IOException {
    if (this.delegationTokenEnabled) {
      return this.delegationTokenManager.getDelegationToken(renewer);
    } else {
      return super.getDelegationToken(renewer);
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
