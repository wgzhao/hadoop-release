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
import org.apache.hadoop.fs.azuredfs.contracts.exceptions.InvalidUriAuthorityException;
import org.apache.hadoop.fs.azuredfs.contracts.exceptions.InvalidUriException;
import org.apache.hadoop.fs.azuredfs.contracts.services.ServiceProvider;
import org.apache.hadoop.fs.azuredfs.services.ServiceProviderImpl;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.Progressable;

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
    workingDir = makeAbsolute(newDir);
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

  @VisibleForTesting
  Path makeAbsolute(Path path) {
    if (path.isAbsolute()) {
      return path;
    }
    return new Path(this.workingDir, path);
  }

  private URI ensureAuthority(URI uri, Configuration conf) {

    Preconditions.checkNotNull("uri", uri);

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
}