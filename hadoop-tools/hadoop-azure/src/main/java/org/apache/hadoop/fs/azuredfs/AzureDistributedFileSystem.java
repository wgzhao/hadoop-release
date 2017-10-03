
package org.apache.hadoop.fs.azuredfs;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Paths;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;

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
import org.apache.hadoop.fs.azuredfs.contracts.exceptions.FileSystemOperationUnhandledException;
import org.apache.hadoop.fs.azuredfs.contracts.exceptions.InvalidUriAuthorityException;
import org.apache.hadoop.fs.azuredfs.contracts.exceptions.InvalidUriException;
import org.apache.hadoop.fs.azuredfs.contracts.exceptions.ServiceResolutionException;
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

  @Override
  public void initialize(URI uri, Configuration configuration)
      throws IOException {
    uri = ensureAuthority(uri, configuration);
    super.initialize(uri, configuration);

    setConf(configuration);
    this.uri = URI.create(uri.getScheme() + "://" + uri.getAuthority());
    this.userGroupInformation = UserGroupInformation.getCurrentUser();
    this.workingDir = new Path(
        FileSystemConfigurations.USER_HOME_DIRECTORY_PREFIX,
        UserGroupInformation.
            getCurrentUser().
            getShortUserName()).
        makeQualified(getUri(), getWorkingDirectory());

    this.serviceProvider = ServiceProviderImpl.create(configuration);

    try {
      this.tracingService = serviceProvider.get(TracingService.class);
    } catch (ServiceResolutionException serviceResolutionException) {
      throw new IOException(serviceResolutionException);
    }
  }

  @Override
  public URI getUri() {
    return this.uri;
  }

  @Override
  public FSDataInputStream open(Path path, int bufferSize) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public FSDataOutputStream create(Path f, FsPermission permission, boolean overwrite, int bufferSize,
      short replication, long blockSize, Progressable progress) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public FSDataOutputStream append(Path f, int bufferSize, Progressable progress) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean rename(Path src, Path dst) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean delete(Path f, boolean recursive) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public FileStatus[] listStatus(Path f) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public Path getWorkingDirectory() {
    return workingDir;
  }

  @Override
  public void setWorkingDirectory(Path newDir) {
    workingDir = newDir.makeQualified(this.uri, this.workingDir);
  }

  @Override
  public boolean mkdirs(Path f, FsPermission permission) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public FileStatus getFileStatus(Path f) throws IOException {
    throw new UnsupportedOperationException();
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

  private URI ensureAuthority(URI uri, Configuration conf) {

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

  private boolean isAdfsScheme(String scheme) {
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
  void execute(
      String scopeDescription,
      AzureDistributedFileSystemFileOperation fileOperation) throws IOException {

      TraceScope traceScope = tracingService.traceBegin(scopeDescription);

    try {
      fileOperation.execute();
    } catch (AzureDistributedFileSystemException azureDistributedFileSystemException) {
      tracingService.traceException(traceScope, azureDistributedFileSystemException);
      throw new IOException(azureDistributedFileSystemException);
    } catch (Exception exception) {
      FileSystemOperationUnhandledException fileSystemOperationUnhandledException = new FileSystemOperationUnhandledException(exception);
      tracingService.traceException(traceScope, fileSystemOperationUnhandledException);
      throw new IOException(fileSystemOperationUnhandledException);
    } finally {
      tracingService.traceEnd(traceScope);
    }
  }

  @VisibleForTesting
  interface AzureDistributedFileSystemFileOperation {
    void execute() throws AzureDistributedFileSystemException;
  }
}