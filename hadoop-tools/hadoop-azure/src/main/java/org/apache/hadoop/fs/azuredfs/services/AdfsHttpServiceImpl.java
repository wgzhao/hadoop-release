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
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;

import com.google.common.base.Preconditions;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.microsoft.azure.dfs.rest.client.generated.models.ErrorSchemaException;
import com.microsoft.azure.dfs.rest.client.generated.models.GetFilesystemPropertiesHeaders;
import com.microsoft.azure.dfs.rest.client.generated.models.ListEntrySchema;
import com.microsoft.azure.dfs.rest.client.generated.models.ListSchema;
import com.microsoft.rest.ServiceResponseWithHeaders;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import rx.Observable;
import rx.functions.Func1;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.azuredfs.AzureDistributedFileSystem;
import org.apache.hadoop.fs.azuredfs.constants.FileSystemConfigurations;
import org.apache.hadoop.fs.azuredfs.contracts.exceptions.AzureDistributedFileSystemException;
import org.apache.hadoop.fs.azuredfs.contracts.exceptions.AzureServiceErrorResponseException;
import org.apache.hadoop.fs.azuredfs.contracts.exceptions.InvalidAzureServiceErrorResponseException;
import org.apache.hadoop.fs.azuredfs.contracts.exceptions.InvalidFileSystemPropertyException;
import org.apache.hadoop.fs.azuredfs.contracts.services.AdfsHttpClient;
import org.apache.hadoop.fs.azuredfs.contracts.services.AdfsHttpClientFactory;
import org.apache.hadoop.fs.azuredfs.contracts.services.AdfsHttpService;

@Singleton
@InterfaceAudience.Private
@InterfaceStability.Evolving
final class AdfsHttpServiceImpl implements AdfsHttpService {
  private static final String FILE_SYSTEM = "filesystem";
  private static final String FILE = "file";
  private static final String DIRECTORY = "directory";
  private static final String DATE_TIME_PATTERN = "E, dd MMM yyyy HH:mm:ss 'GMT'";
  private static final String LEASE_ACTION_AQUIRE = "acquire";
  private static final String HEADER_PROPERTIES_IS_DIR = "x-ms-meta-hdi_isfolder";
  private static final String COMP_PROPERTIES = "properties";
  private static final int LIST_MAX_RESULTS = 100;

  private final AdfsHttpClientFactory adfsHttpClientFactory;
  private final ConcurrentHashMap<AzureDistributedFileSystem, AdfsHttpClient> adfsHttpClientCache;

  @Inject
  AdfsHttpServiceImpl(
      final AdfsHttpClientFactory adfsHttpClientFactory) {
    Preconditions.checkNotNull(adfsHttpClientFactory, "adfsHttpClientFactory");

    this.adfsHttpClientFactory = adfsHttpClientFactory;
    this.adfsHttpClientCache = new ConcurrentHashMap<>();
  }

  @Override
  public Hashtable<String, String> getFilesystemProperties(final AzureDistributedFileSystem azureDistributedFileSystem)
      throws AzureDistributedFileSystemException {
    return execute(new Callable<Hashtable<String, String>>() {
      @Override
      public Hashtable<String, String> call() throws Exception {
        return getFilesystemPropertiesAsync(azureDistributedFileSystem).toBlocking().single();
      }
    });
  }

  @Override
  public Observable<Hashtable<String, String>> getFilesystemPropertiesAsync(final AzureDistributedFileSystem azureDistributedFileSystem) throws
      AzureDistributedFileSystemException {
    final AdfsHttpClient adfsHttpClient = this.getFileSystemClient(azureDistributedFileSystem);
    Observable<ServiceResponseWithHeaders<Void, GetFilesystemPropertiesHeaders>> response =
        adfsHttpClient.getFilesystemPropertiesWithServiceResponseAsync(
            adfsHttpClient.getSession().getFileSystem(),
            FILE_SYSTEM);

    final Hashtable<String, String> propertiesTable = new Hashtable<>();
    return response.flatMap(new Func1<ServiceResponseWithHeaders<Void, GetFilesystemPropertiesHeaders>, Observable<Hashtable<String, String>>>() {
      @Override
      public Observable<Hashtable<String, String>> call(ServiceResponseWithHeaders<Void, GetFilesystemPropertiesHeaders>
          voidGetFilesystemPropertiesHeadersServiceResponseWithHeaders) {
        String xMsProperties = voidGetFilesystemPropertiesHeadersServiceResponseWithHeaders.headers().xMsProperties();
        if (xMsProperties != null && !xMsProperties.isEmpty()) {
          try {
            String[] properties =
                voidGetFilesystemPropertiesHeadersServiceResponseWithHeaders.headers().xMsProperties().split(",");

            if (properties.length == 0) {
              return Observable.just(propertiesTable);
            }

            for (String property : properties) {
              if (property.isEmpty()) {
                throw new InvalidFileSystemPropertyException(xMsProperties);
              }

              String[] nameValue = property.split("=");
              if (nameValue.length != 2) {
                throw new InvalidFileSystemPropertyException(xMsProperties);
              }

              propertiesTable.put(nameValue[0], nameValue[1]);
            }

            return Observable.just(propertiesTable);
          } catch (Throwable t) {
            return Observable.error(t);
          }
        }

        return Observable.just(propertiesTable);
      }
    });
  }

  @Override
  public void setFilesystemProperties(final AzureDistributedFileSystem azureDistributedFileSystem, final Hashtable<String, String> properties) throws
      AzureDistributedFileSystemException {
    execute(new Callable<Void>() {
      @Override
      public Void call() throws Exception {
        setFilesystemPropertiesAsync(azureDistributedFileSystem, properties).toBlocking().single();
        return null;
      }
    });
  }

  @Override
  public Observable<Void> setFilesystemPropertiesAsync(final AzureDistributedFileSystem azureDistributedFileSystem, final Hashtable<String, String> properties)
      throws
      AzureDistributedFileSystemException {
    if (properties == null || properties.size() == 0) {
      return Observable.just(null);
    }

    final AdfsHttpClient adfsHttpClient = this.getFileSystemClient(azureDistributedFileSystem);
    return execute(new Callable<Observable<Void>>() {
      @Override
      public Observable<Void> call() throws Exception {
        String serializedProperties = "";
        Set<String> keys = properties.keySet();
        Iterator<String> itr = keys.iterator();

        while (itr.hasNext()) {
          String key = itr.next();
          serializedProperties += key + "=" + properties.get(key);

          if (itr.hasNext()) {
            serializedProperties += ",";
          }
        }

        return adfsHttpClient.setFilesystemPropertiesAsync(
            COMP_PROPERTIES,
            adfsHttpClient.getSession().getFileSystem(),
            FILE_SYSTEM,
            serializedProperties,
            null,
            0,
            null,
            null);
      }
    });
  }

  @Override
  public void createFilesystem(final AzureDistributedFileSystem azureDistributedFileSystem) throws AzureDistributedFileSystemException {
    execute(new Callable<Void>() {
      @Override
      public Void call() throws Exception {
        createFilesystemAsync(azureDistributedFileSystem).toBlocking().single();
        return null;
      }
    });
  }

  @Override
  public Observable<Void> createFilesystemAsync(final AzureDistributedFileSystem azureDistributedFileSystem) throws AzureDistributedFileSystemException {
    if (!this.adfsHttpClientCache.containsKey(azureDistributedFileSystem)) {
      this.adfsHttpClientCache.put(
          azureDistributedFileSystem,
          this.adfsHttpClientFactory.create(azureDistributedFileSystem));
    }

    final AdfsHttpClient adfsHttpClient = this.getFileSystemClient(azureDistributedFileSystem);
    return adfsHttpClient.createFilesystemAsync(
        adfsHttpClient.getSession().getFileSystem(),
        FILE_SYSTEM,
        null,
        null,
        null,
        null,
        null,
        null);
  }

  @Override
  public void deleteFilesystem(final AzureDistributedFileSystem azureDistributedFileSystem) throws AzureDistributedFileSystemException {
    execute(new Callable<Void>() {
      @Override
      public Void call() throws Exception {
        deleteFilesystemAsync(azureDistributedFileSystem).toBlocking().single();
        return null;
      }
    });
  }

  @Override
  public Observable<Void> deleteFilesystemAsync(final AzureDistributedFileSystem azureDistributedFileSystem) throws AzureDistributedFileSystemException {
    final AdfsHttpClient adfsHttpClient = this.getFileSystemClient(azureDistributedFileSystem);
    return adfsHttpClient.deleteFilesystemAsync(
        adfsHttpClient.getSession().getFileSystem(), FILE_SYSTEM);
  }

  @Override
  public OutputStream createPath(final AzureDistributedFileSystem azureDistributedFileSystem, final Path path, final boolean isDirectory) throws
      AzureDistributedFileSystemException {
    return execute(new Callable<OutputStream>() {
      @Override
      public OutputStream call() throws Exception {
        return createPathAsync(azureDistributedFileSystem, path, isDirectory).toBlocking().single();
      }
    });
  }

  @Override
  public Observable<OutputStream> createPathAsync(final AzureDistributedFileSystem azureDistributedFileSystem, final Path path, final boolean isDirectory)
      throws AzureDistributedFileSystemException {
    final AdfsHttpClient adfsHttpClient = this.getFileSystemClient(azureDistributedFileSystem);
    return adfsHttpClient.createPathAsync(
        getResource(isDirectory),
        adfsHttpClient.getSession().getFileSystem(),
        getRelativePath(path)).
        flatMap(new Func1<Void, Observable<OutputStream>>() {
          @Override
          public Observable<OutputStream> call(Void aVoid) {
            return Observable.just(null);
          }
        });
  }

  @Override
  public InputStream readPath(final AzureDistributedFileSystem azureDistributedFileSystem, final Path path) throws AzureDistributedFileSystemException {
    return execute(new Callable<InputStream>() {
      @Override
      public InputStream call() throws Exception {
        return readPathAsync(azureDistributedFileSystem, path).toBlocking().single();
      }
    });
  }

  @Override
  public Observable<InputStream> readPathAsync(final AzureDistributedFileSystem azureDistributedFileSystem, final Path path) throws
      AzureDistributedFileSystemException {
    final AdfsHttpClient adfsHttpClient = this.getFileSystemClient(azureDistributedFileSystem);
    return adfsHttpClient.readPathAsync(adfsHttpClient.getSession().getFileSystem(), getRelativePath(path));
  }

  @Override
  public void updatePath(final AzureDistributedFileSystem azureDistributedFileSystem, final Path path, final boolean isDirectory) throws
      AzureDistributedFileSystemException {
    execute(new Callable<Void>() {
      @Override
      public Void call() throws Exception {
        return updatePathAsync(azureDistributedFileSystem, path, isDirectory).toBlocking().single();
      }
    });
  }

  @Override
  public Observable<Void> updatePathAsync(final AzureDistributedFileSystem azureDistributedFileSystem, final Path path, final boolean isDirectory) throws
      AzureDistributedFileSystemException {
    final AdfsHttpClient adfsHttpClient = this.getFileSystemClient(azureDistributedFileSystem);
    return adfsHttpClient.updatePathAsync(
        getResource(isDirectory),
        adfsHttpClient.getSession().getFileSystem(),
        getRelativePath(path));
  }

  @Override
  public void renamePath(final AzureDistributedFileSystem azureDistributedFileSystem, final Path source, final Path destination, final boolean isDirectory)
      throws
      AzureDistributedFileSystemException {
    execute(new Callable<Void>() {
      @Override
      public Void call() throws Exception {
        renamePathAsync(azureDistributedFileSystem, source, destination, isDirectory).toBlocking().single();
        return null;
      }
    });
  }

  @Override
  public Observable<Void> renamePathAsync(final AzureDistributedFileSystem azureDistributedFileSystem, final Path source, final Path destination, final boolean
      isDirectory) throws AzureDistributedFileSystemException {
    final AdfsHttpClient adfsHttpClient = this.getFileSystemClient(azureDistributedFileSystem);
    return adfsHttpClient.createPathAsync(
        getResource(isDirectory),
        adfsHttpClient.getSession().getFileSystem(),
        getRelativePath(destination),
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        Path.SEPARATOR + adfsHttpClient.getSession().getFileSystem() + Path.SEPARATOR + getRelativePath(source),
        LEASE_ACTION_AQUIRE,
        null,
        null,
        null,
        null,
        FileSystemConfigurations.FS_AZURE_DEFAULT_CONNECTION_TIMEOUT,
        null,
        null);
  }

  @Override
  public void deletePath(final AzureDistributedFileSystem azureDistributedFileSystem, final Path path, final boolean isDirectory) throws
      AzureDistributedFileSystemException {
    execute(new Callable<Void>() {
      @Override
      public Void call() throws Exception {
        deletePathAsync(azureDistributedFileSystem, path, isDirectory).toBlocking().single();
        return null;
      }
    });
  }

  @Override
  public Observable<Void> deletePathAsync(final AzureDistributedFileSystem azureDistributedFileSystem, final Path path, final boolean isDirectory) throws
      AzureDistributedFileSystemException {
    final AdfsHttpClient adfsHttpClient = this.getFileSystemClient(azureDistributedFileSystem);
    return adfsHttpClient.deletePathAsync(FILE, adfsHttpClient.getSession().getFileSystem(), getRelativePath(path));
  }

  @Override
  public FileStatus[] listStatus(final AzureDistributedFileSystem azureDistributedFileSystem, final Path path) throws AzureDistributedFileSystemException {
    return execute(new Callable<FileStatus[]>() {
      @Override
      public FileStatus[] call() throws Exception {
        return listStatusAsync(azureDistributedFileSystem, path).toBlocking().single();
      }
    });
  }

  @Override
  public Observable<FileStatus[]> listStatusAsync(final AzureDistributedFileSystem azureDistributedFileSystem, final Path path) throws
      AzureDistributedFileSystemException {
    final AdfsHttpClient adfsHttpClient = this.getFileSystemClient(azureDistributedFileSystem);
    return adfsHttpClient.listPathsAsync(
        true,
        adfsHttpClient.getSession().getFileSystem(),
        FILE_SYSTEM,
        getRelativePath(path),
        null,
        LIST_MAX_RESULTS,
        null,
        FileSystemConfigurations.FS_AZURE_DEFAULT_CONNECTION_TIMEOUT,
        null,
        null).flatMap(new Func1<ListSchema, Observable<FileStatus[]>>() {
      @Override
      public Observable<FileStatus[]> call(ListSchema listSchema) {
        final List<FileStatus> fileStatus = new LinkedList<>();
        for (ListEntrySchema entry : listSchema.paths()) {
          final DateTime dateTime = DateTime.parse(
              entry.lastModified(),
              DateTimeFormat.forPattern(DATE_TIME_PATTERN).withZoneUTC());

          fileStatus.add(
              new FileStatus(entry.contentLength(),
                  entry.eTag() != null && entry.eTag().contains(HEADER_PROPERTIES_IS_DIR),
                  0,
                  0,
                  dateTime.getMillis(),
                  new Path(entry.name())));
        }

        return Observable.just(fileStatus.toArray(new FileStatus[0]));
      }
    });
  }

  @Override
  public void closeFileSystem(final AzureDistributedFileSystem azureDistributedFileSystem) throws AzureDistributedFileSystemException {
    this.adfsHttpClientCache.remove(azureDistributedFileSystem);
  }

  private String getRelativePath(final Path path) {
    Preconditions.checkNotNull(path, "path");
    final String relativePath = path.toUri().getPath();

    if (relativePath.length() == 0) {
      return relativePath;
    }

    if (relativePath.charAt(0) == Path.SEPARATOR_CHAR) {
      if (relativePath.length() == 1) {
        return "";
      }

      return relativePath.substring(1);
    }

    return relativePath;
  }

  private AdfsHttpClient getFileSystemClient(final AzureDistributedFileSystem azureDistributedFileSystem) {
    Preconditions.checkNotNull(azureDistributedFileSystem, "azureDistributedFileSystem");
    return this.adfsHttpClientCache.get(azureDistributedFileSystem);
  }

  private <T> T execute(final Callable<T> callableRestOperation) throws
      AzureDistributedFileSystemException {

    try {
      return callableRestOperation.call();
    } catch (ErrorSchemaException exception) {
      int statusCode = -1;
      String errorCode = "";
      String errorMessage = "";

      if (exception.response() != null
          && exception.response().raw() != null
          && exception.response().raw().networkResponse() != null) {
        statusCode = exception.response().raw().networkResponse().code();
      }

      if (exception.body() != null && exception.body().error() != null) {
        errorCode = exception.body().error().code();
        errorMessage = exception.body().error().message();
      }

      throw new AzureServiceErrorResponseException(statusCode, errorCode, errorMessage, exception);
    } catch (Exception exception) {
      throw new InvalidAzureServiceErrorResponseException(exception);
    }
  }

  private String getResource(final boolean isDirectory) {
    return isDirectory ? DIRECTORY : FILE;
  }
}