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

import java.io.File;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import com.google.common.base.Preconditions;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.microsoft.azure.dfs.rest.client.generated.models.ErrorSchemaException;
import com.microsoft.azure.dfs.rest.client.generated.models.GetFilesystemPropertiesHeaders;
import com.microsoft.azure.dfs.rest.client.generated.models.GetPathPropertiesHeaders;
import com.microsoft.azure.dfs.rest.client.generated.models.ListEntrySchema;
import com.microsoft.azure.dfs.rest.client.generated.models.ListSchema;
import com.microsoft.rest.ServiceResponseWithHeaders;
import io.netty.buffer.ByteBuf;
import okhttp3.Headers;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import rx.Observable;
import rx.functions.Func1;

import org.apache.commons.lang.WordUtils;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.commons.lang3.concurrent.ConcurrentUtils;
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
import org.apache.hadoop.fs.azuredfs.contracts.services.AdfsHttpClientSessionState;
import org.apache.hadoop.fs.azuredfs.contracts.services.AdfsHttpService;
import org.apache.hadoop.fs.azuredfs.contracts.services.AdfsStreamFactory;
import org.apache.hadoop.fs.azuredfs.contracts.services.AzureServiceErrorCode;
import org.apache.hadoop.fs.azuredfs.contracts.services.ConfigurationService;
import org.apache.hadoop.util.StringUtils;

import static com.google.common.net.HttpHeaders.CONTENT_LENGTH;
import static com.google.common.net.HttpHeaders.LAST_MODIFIED;
import static org.apache.hadoop.fs.azuredfs.constants.FileSystemConfigurations.HDI_IS_FOLDER;

@Singleton
@InterfaceAudience.Private
@InterfaceStability.Evolving
final class AdfsHttpServiceImpl implements AdfsHttpService {
  private static final String FILE_SYSTEM = "filesystem";
  private static final String FILE = "file";
  private static final String DIRECTORY = "directory";
  private static final String DATE_TIME_PATTERN = "E, dd MMM yyyy HH:mm:ss 'GMT'";
  private static final String SOURCE_LEASE_ACTION_ACQUIRE = "acquire";
  private static final String COMP_PROPERTIES = "properties";
  private static final int LIST_MAX_RESULTS = 5000;
  private static final int WRITE_BUFFER_SIZE = 8192;

  private final AdfsHttpClientFactory adfsHttpClientFactory;
  private final AdfsStreamFactory adfsStreamFactory;
  private final ConcurrentHashMap<URI, AdfsHttpClient> adfsHttpClientCache;
  private final ThreadPoolExecutor writeExecutorService;
  private final ThreadPoolExecutor readExecutorService;
  private final ConfigurationService configurationService;

  @Inject
  AdfsHttpServiceImpl(
      final ConfigurationService configurationService,
      final AdfsHttpClientFactory adfsHttpClientFactory,
      final AdfsStreamFactory adfsStreamFactory) {
    Preconditions.checkNotNull(adfsHttpClientFactory, "adfsHttpClientFactory");
    Preconditions.checkNotNull(adfsStreamFactory, "adfsStreamFactory");
    Preconditions.checkNotNull(configurationService, "configurationService");

    this.configurationService = configurationService;
    this.adfsStreamFactory = adfsStreamFactory;
    this.adfsHttpClientCache = new ConcurrentHashMap<>();
    this.adfsHttpClientFactory = adfsHttpClientFactory;

    this.readExecutorService = createThreadPoolExecutor(this.configurationService.getMaxConcurrentThreads());
    this.writeExecutorService = createThreadPoolExecutor(this.configurationService.getMaxConcurrentThreads());
  }

  @Override
  protected void finalize() throws Throwable {
    try {
      this.readExecutorService.shutdown();
      this.writeExecutorService.shutdown();

      this.readExecutorService.awaitTermination(10, TimeUnit.SECONDS);
      this.writeExecutorService.awaitTermination(10, TimeUnit.SECONDS);
    }
    finally {
      super.finalize();
    }
  }

  @Override
  public Hashtable<String, String> getFilesystemProperties(final AzureDistributedFileSystem azureDistributedFileSystem)
      throws AzureDistributedFileSystemException {
    return execute(new Callable<Hashtable<String, String>>() {
      @Override
      public Hashtable<String, String> call() throws Exception {
        return getFilesystemPropertiesAsync(azureDistributedFileSystem).get();
      }
    });
  }

  @Override
  public Future<Hashtable<String, String>> getFilesystemPropertiesAsync(final AzureDistributedFileSystem azureDistributedFileSystem) throws
      AzureDistributedFileSystemException {
    final AdfsHttpClient adfsHttpClient = this.getFileSystemClient(azureDistributedFileSystem);

    final Callable<Hashtable<String, String>> asyncCallable = new Callable<Hashtable<String, String>>() {
      @Override
      public Hashtable<String, String> call() throws Exception {
        return getFileSystemPropertiesInternal(adfsHttpClient).toBlocking().single();
      }
    };

    return submitTaskToQueue(adfsHttpClient, readExecutorService, asyncCallable);
  }

  @Override
  public void setFilesystemProperties(final AzureDistributedFileSystem azureDistributedFileSystem, final Hashtable<String, String> properties) throws
      AzureDistributedFileSystemException {
    execute(new Callable<Void>() {
      @Override
      public Void call() throws Exception {
        return setFilesystemPropertiesAsync(azureDistributedFileSystem, properties).get();
      }
    });
  }

  @Override
  public Future<Void> setFilesystemPropertiesAsync(final AzureDistributedFileSystem azureDistributedFileSystem, final Hashtable<String, String> properties)
      throws
      AzureDistributedFileSystemException {
    if (properties == null || properties.size() == 0) {
      return ConcurrentUtils.constantFuture(null);
    }

    final AdfsHttpClient adfsHttpClient = this.getFileSystemClient(azureDistributedFileSystem);
    final Callable<Void> asyncCallable = new Callable<Void>() {
      @Override
      public Void call() throws Exception {
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
            null).toBlocking().single();
      }
    };

    return submitTaskToQueue(adfsHttpClient, writeExecutorService, asyncCallable);
  }

  @Override
  public Hashtable<String, String> getPathProperties(final AzureDistributedFileSystem azureDistributedFileSystem, final Path path) throws
      AzureDistributedFileSystemException {
    return execute(new Callable<Hashtable<String, String>>() {
      @Override
      public Hashtable<String, String> call() throws Exception {
        return getPathPropertiesAsync(azureDistributedFileSystem, path).get();
      }
    });
  }

  @Override
  public Future<Hashtable<String, String>> getPathPropertiesAsync(final AzureDistributedFileSystem azureDistributedFileSystem, final Path path) throws
      AzureDistributedFileSystemException {
    final AdfsHttpClient adfsHttpClient = this.getFileSystemClient(azureDistributedFileSystem);

    final Callable<Hashtable<String, String>> asyncCallable = new Callable<Hashtable<String, String>>() {
      @Override
      public Hashtable<String, String> call() throws Exception {
        return getPathPropertiesInternal(adfsHttpClient, path).toBlocking().firstOrDefault(new Hashtable<String, String>());
      }
    };

    return submitTaskToQueue(adfsHttpClient, readExecutorService, asyncCallable);
  }

  @Override
  public void createFilesystem(final AzureDistributedFileSystem azureDistributedFileSystem) throws AzureDistributedFileSystemException {
    execute(new Callable<Void>() {
      @Override
      public Void call() throws Exception {
        return createFilesystemAsync(azureDistributedFileSystem).get();
      }
    });
  }

  @Override
  public Future<Void> createFilesystemAsync(final AzureDistributedFileSystem azureDistributedFileSystem) throws AzureDistributedFileSystemException {
    this.adfsHttpClientCache.putIfAbsent(azureDistributedFileSystem.getUri(), this.adfsHttpClientFactory.create(azureDistributedFileSystem));

    final AdfsHttpClient adfsHttpClient = this.getFileSystemClient(azureDistributedFileSystem);

    final Callable<Void> asyncCallable = new Callable<Void>() {
      @Override
      public Void call() throws Exception {
        return adfsHttpClient.createFilesystemAsync(
            adfsHttpClient.getSession().getFileSystem(),
            FILE_SYSTEM,
            null,
            null,
            null,
            null,
            null,
            null).toBlocking().single();
      }
    };

    return submitTaskToQueue(adfsHttpClient, writeExecutorService, asyncCallable);
  }

  @Override
  public void deleteFilesystem(final AzureDistributedFileSystem azureDistributedFileSystem) throws AzureDistributedFileSystemException {
    execute(new Callable<Void>() {
      @Override
      public Void call() throws Exception {
        return deleteFilesystemAsync(azureDistributedFileSystem).get();
      }
    });
  }

  @Override
  public Future<Void> deleteFilesystemAsync(final AzureDistributedFileSystem azureDistributedFileSystem) throws AzureDistributedFileSystemException {
    final AdfsHttpClient adfsHttpClient = this.getFileSystemClient(azureDistributedFileSystem);
    final Callable<Void> asyncCallable = new Callable<Void>() {
      @Override
      public Void call() throws Exception {
        return adfsHttpClient.deleteFilesystemAsync(
            adfsHttpClient.getSession().getFileSystem(), FILE_SYSTEM).toBlocking().single();
      }
    };

    return submitTaskToQueue(adfsHttpClient, writeExecutorService, asyncCallable);
  }

  @Override
  public OutputStream createFile(final AzureDistributedFileSystem azureDistributedFileSystem, final Path path, final boolean overwrite) throws
      AzureDistributedFileSystemException {
    return execute(new Callable<OutputStream>() {
      @Override
      public OutputStream call() throws Exception {
        return createFileAsync(azureDistributedFileSystem, path, overwrite).get();
      }
    });
  }

  @Override
  public Future<OutputStream> createFileAsync(final AzureDistributedFileSystem azureDistributedFileSystem, final Path path, final boolean overwrite)
      throws AzureDistributedFileSystemException {
    final AdfsHttpClient adfsHttpClient = this.getFileSystemClient(azureDistributedFileSystem);
    final Callable<OutputStream> asyncCallable = new Callable<OutputStream>() {
      @Override
      public OutputStream call() throws Exception {
        // This needs to be updated with swagger.
        if (!overwrite) {
          try {
            getPathPropertiesInternal(adfsHttpClient, path).toBlocking().single();
            // We shouldn't be here
            throw new AzureServiceErrorResponseException(
                AzureServiceErrorCode.PRE_CONDITION_FAILED.getStatusCode(),
                AzureServiceErrorCode.PRE_CONDITION_FAILED.getErrorCode(),
                "Overwrite is false and file exists, createFileAsync should fail.",
                null);
          }
          catch (ErrorSchemaException ex) {
            if (ex.response().code() != AzureServiceErrorCode.PATH_NOT_FOUND.getStatusCode()) {
              throw ex;
            }
          }
        }

        return adfsHttpClient.createPathAsync(
            getResource(false),
            adfsHttpClient.getSession().getFileSystem(),
            getRelativePath(path)).
            flatMap(new Func1<Void, Observable<OutputStream>>() {
              @Override
              public Observable<OutputStream> call(Void aVoid) {
                try {
                  return openFileForWriteInternal(azureDistributedFileSystem, adfsHttpClient, path, overwrite);
                } catch (AzureDistributedFileSystemException ex) {
                  return Observable.error(ex);
                }
              }
            }).toBlocking().single();
      }
    };

    return submitTaskToQueue(adfsHttpClient, writeExecutorService, asyncCallable);
  }

  @Override
  public Void createDirectory(final AzureDistributedFileSystem azureDistributedFileSystem, final Path path) throws AzureDistributedFileSystemException {
    return execute(new Callable<Void>() {
      @Override
      public Void call() throws Exception {
        return createDirectoryAsync(azureDistributedFileSystem, path).get();
      }
    });
  }

  @Override
  public Future<Void> createDirectoryAsync(final AzureDistributedFileSystem azureDistributedFileSystem, final Path path) throws
      AzureDistributedFileSystemException {
    final AdfsHttpClient adfsHttpClient = this.getFileSystemClient(azureDistributedFileSystem);
    final Callable<Void> asyncCallable = new Callable<Void>() {
      @Override
      public Void call() throws Exception {
        return adfsHttpClient.createPathAsync(
            getResource(true),
            adfsHttpClient.getSession().getFileSystem(),
            getRelativePath(path)).toBlocking().single();
      }
    };

    return submitTaskToQueue(adfsHttpClient, writeExecutorService, asyncCallable);
  }

  @Override
  public InputStream openFileForRead(final AzureDistributedFileSystem azureDistributedFileSystem, final Path path) throws AzureDistributedFileSystemException {
    return execute(new Callable<InputStream>() {
      @Override
      public InputStream call() throws Exception {
        return openFileForReadAsync(azureDistributedFileSystem, path).get();
      }
    });
  }

  @Override
  public Future<InputStream> openFileForReadAsync(final AzureDistributedFileSystem azureDistributedFileSystem, final Path path) throws
      AzureDistributedFileSystemException {
    final AdfsHttpClient adfsHttpClient = getFileSystemClient(azureDistributedFileSystem);
    final Callable<InputStream> asyncCallable = new Callable<InputStream>() {
      @Override
      public InputStream call() throws Exception {
        return getFileStatusInternal(adfsHttpClient, path)
            .flatMap(new Func1<FileStatus, Observable<InputStream>>() {
              @Override
              public Observable<InputStream> call(FileStatus fileStatus) {
                if (fileStatus.isDirectory()) {
                  return Observable.error(new AzureServiceErrorResponseException(
                      AzureServiceErrorCode.PRE_CONDITION_FAILED.getStatusCode(),
                      AzureServiceErrorCode.PRE_CONDITION_FAILED.getErrorCode(),
                      "openFileForReadAsync must be used with files and not directories",
                      null));
                }

                return Observable.just(adfsStreamFactory.createReadStream(
                    azureDistributedFileSystem,
                    path,
                    fileStatus.getLen()));
              }
            }).toBlocking().single();
      }
    };

    return submitTaskToQueue(adfsHttpClient, readExecutorService, asyncCallable);
  }


  @Override
  public OutputStream openFileForWrite(final AzureDistributedFileSystem azureDistributedFileSystem, final Path path, final boolean overwrite) throws
      AzureDistributedFileSystemException {
    return execute(new Callable<OutputStream>() {
      @Override
      public OutputStream call() throws Exception {
        return openFileForWriteAsync(azureDistributedFileSystem, path, overwrite).get();
      }
    });
  }

  @Override
  public Future<OutputStream> openFileForWriteAsync(final AzureDistributedFileSystem azureDistributedFileSystem, final Path path, final boolean overwrite)
      throws AzureDistributedFileSystemException {
    final AdfsHttpClient adfsHttpClient = getFileSystemClient(azureDistributedFileSystem);
    final Callable<OutputStream> asyncCallable = new Callable<OutputStream>() {
      @Override
      public OutputStream call() throws Exception {
        return openFileForWriteInternal(azureDistributedFileSystem, adfsHttpClient, path, overwrite).toBlocking().single();
      }
    };

    return submitTaskToQueue(adfsHttpClient, readExecutorService, asyncCallable);
  }

  @Override
  public Void readFile(final AzureDistributedFileSystem azureDistributedFileSystem, final Path path, final long offset, final int length, final ByteBuf
      readBuffer, final int readBufferOffset) throws
      AzureDistributedFileSystemException {
    return execute(new Callable<Void>() {
      @Override
      public Void call() throws Exception {
        return readFileAsync(azureDistributedFileSystem, path, offset, length, readBuffer, readBufferOffset).get();
      }
    });
  }

  @Override
  public Future<Void> readFileAsync(final AzureDistributedFileSystem azureDistributedFileSystem, final Path path, final long offset, final int length, final
  ByteBuf targetBuffer, final int targetBufferOffset) throws
      AzureDistributedFileSystemException {
    final AdfsHttpClient adfsHttpClient = this.getFileSystemClient(azureDistributedFileSystem);
    final Callable<Void> asyncCallable = new Callable<Void>() {
      @Override
      public Void call() throws Exception {
        return adfsHttpClient.readPathAsync(
            adfsHttpClient.getSession().getFileSystem(),
            getRelativePath(path),
            getRangeHeader(offset, length),
            null,
            FileSystemConfigurations.FS_AZURE_DEFAULT_CONNECTION_READ_TIMEOUT,
            null,
            null).
            flatMap(new Func1<InputStream, Observable<Void>>() {
              @Override
              public Observable<Void> call(InputStream inputStream) {
                try {
                  byte[] buffer = new byte[WRITE_BUFFER_SIZE];
                  targetBuffer.setIndex(0, targetBufferOffset);
                  int readBytes = 0;
                  while ((readBytes = inputStream.read(buffer)) != -1) {
                    targetBuffer.writeBytes(buffer, 0, readBytes);
                  }
                  inputStream.close();
                  return Observable.just(null);
                } catch (Exception ex) {
                  return Observable.error(ex);
                }
              }
            }).toBlocking().single();
      }
    };

    return submitTaskToQueue(adfsHttpClient, readExecutorService, asyncCallable);
  }

  @Override
  public void writeFile(final AzureDistributedFileSystem azureDistributedFileSystem, final Path path, final ByteBuf body, final long offset) throws
      AzureDistributedFileSystemException {
    execute(new Callable<Void>() {
      @Override
      public Void call() throws Exception {
        return writeFileAsync(azureDistributedFileSystem, path, body, offset).get();
      }
    });
  }

  @Override
  public Future<Void> writeFileAsync(final AzureDistributedFileSystem azureDistributedFileSystem, final Path path, final
  ByteBuf body, final long offset) throws AzureDistributedFileSystemException {
    final AdfsHttpClient adfsHttpClient = this.getFileSystemClient(azureDistributedFileSystem);
    final Callable<Void> asyncCallable = new Callable<Void>() {
      @Override
      public Void call() throws Exception {
        final int length = body.readableBytes();
        byte[] bytes = new byte[length];
        body.getBytes(body.readerIndex(), bytes);

        Observable<Void> updatePathAsync = adfsHttpClient.updatePathAsync(
            getResource(false),
            adfsHttpClient.getSession().getFileSystem(),
            getRelativePath(path),
            "data",
            null,
            Long.valueOf(offset),
            null,
            Integer.toString(body.readableBytes()),
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            bytes,
            null,
            FileSystemConfigurations.FS_AZURE_DEFAULT_CONNECTION_TIMEOUT,
            null,
            null);

        bytes = null;
        return updatePathAsync.toBlocking().single();
      }
    };

    return submitTaskToQueue(adfsHttpClient, writeExecutorService, asyncCallable);
  }

  @Override
  public void flushFile(final AzureDistributedFileSystem azureDistributedFileSystem, final Path path, final long offset) throws
      AzureDistributedFileSystemException {
    execute(new Callable<Void>() {
      @Override
      public Void call() throws Exception {
        return flushFileAsync(azureDistributedFileSystem, path, offset).get();
      }
    });
  }

  @Override
  public Future<Void> flushFileAsync(final AzureDistributedFileSystem azureDistributedFileSystem, final Path path, final long offset) throws
      AzureDistributedFileSystemException {
    final AdfsHttpClient adfsHttpClient = this.getFileSystemClient(azureDistributedFileSystem);
    final Callable<Void> asyncCallable = new Callable<Void>() {
      @Override
      public Void call() throws Exception {
        return adfsHttpClient.updatePathAsync(
            getResource(false),
            adfsHttpClient.getSession().getFileSystem(),
            getRelativePath(path),
            "commit",
            null,
            Long.valueOf(offset),
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
            null,
            FileSystemConfigurations.FS_AZURE_DEFAULT_CONNECTION_TIMEOUT,
            null,
            null).toBlocking().single();
      }
    };

    return submitTaskToQueue(adfsHttpClient, writeExecutorService, asyncCallable);
  }

  @Override
  public void renameFile(final AzureDistributedFileSystem azureDistributedFileSystem, final Path source, final Path destination)
      throws
      AzureDistributedFileSystemException {
    execute(new Callable<Void>() {
      @Override
      public Void call() throws Exception {
        renameFileAsync(azureDistributedFileSystem, source, destination).get();
        return null;
      }
    });
  }

  @Override
  public Future<Void> renameFileAsync(final AzureDistributedFileSystem azureDistributedFileSystem, final Path source, final Path destination) throws
      AzureDistributedFileSystemException {
    final AdfsHttpClient adfsHttpClient = this.getFileSystemClient(azureDistributedFileSystem);
    final Callable<Void> asyncCallable = new Callable<Void>() {
      @Override
      public Void call() throws Exception {
        return adfsHttpClient.createPathAsync(
            getResource(false),
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
            null,
            null,
            null,
            SOURCE_LEASE_ACTION_ACQUIRE,
            null,
            null,
            null,
            null,
            null,
            FileSystemConfigurations.FS_AZURE_DEFAULT_CONNECTION_TIMEOUT,
            null,
            null).toBlocking().single();
      }
    };

    return submitTaskToQueue(adfsHttpClient, writeExecutorService, asyncCallable);
  }

  @Override
  public void deleteFile(final AzureDistributedFileSystem azureDistributedFileSystem, final Path path) throws
      AzureDistributedFileSystemException {
    execute(new Callable<Void>() {
      @Override
      public Void call() throws Exception {
        deleteFileAsync(azureDistributedFileSystem, path).get();
        return null;
      }
    });
  }

  @Override
  public Future<Void> deleteFileAsync(final AzureDistributedFileSystem azureDistributedFileSystem, final Path path) throws
      AzureDistributedFileSystemException {
    final AdfsHttpClient adfsHttpClient = this.getFileSystemClient(azureDistributedFileSystem);
    final Callable<Void> asyncCallable = new Callable<Void>() {
      @Override
      public Void call() throws Exception {
        return adfsHttpClient.deletePathAsync(getResource(false), adfsHttpClient.getSession().getFileSystem(), getRelativePath(path)).toBlocking().single();
      }
    };

    return submitTaskToQueue(adfsHttpClient, writeExecutorService, asyncCallable);
  }

  @Override
  public FileStatus getFileStatus(final AzureDistributedFileSystem azureDistributedFileSystem, final Path path) throws AzureDistributedFileSystemException {
    return execute(new Callable<FileStatus>() {
      @Override
      public FileStatus call() throws Exception {
        return getFileStatusAsync(azureDistributedFileSystem, path).get();
      }
    });
  }

  @Override
  public Future<FileStatus> getFileStatusAsync(final AzureDistributedFileSystem azureDistributedFileSystem, final Path path) throws
      AzureDistributedFileSystemException {
    final AdfsHttpClient adfsHttpClient = this.getFileSystemClient(azureDistributedFileSystem);

    final Callable<FileStatus> asyncCallable = new Callable<FileStatus>() {
      @Override
      public FileStatus call() throws Exception {
        return getFileStatusInternal(adfsHttpClient, path).toBlocking().single();
      }
    };

    return submitTaskToQueue(adfsHttpClient, readExecutorService, asyncCallable);
  }

  @Override
  public FileStatus[] listStatus(final AzureDistributedFileSystem azureDistributedFileSystem, final Path path) throws AzureDistributedFileSystemException {
    return execute(new Callable<FileStatus[]>() {
      @Override
      public FileStatus[] call() throws Exception {
        return listStatusAsync(azureDistributedFileSystem, path).get();
      }
    });
  }

  @Override
  public Future<FileStatus[]> listStatusAsync(final AzureDistributedFileSystem azureDistributedFileSystem, final Path path) throws
      AzureDistributedFileSystemException {
    final AdfsHttpClient adfsHttpClient = this.getFileSystemClient(azureDistributedFileSystem);

    final Callable<FileStatus[]> asyncCallable = new Callable<FileStatus[]>() {
      @Override
      public FileStatus[] call() throws Exception {
        Observable<Void> result = Observable.just(null);
        if (!path.isRoot()) {
          result = getFileStatusInternal(adfsHttpClient, path).flatMap(new Func1<FileStatus, Observable<Void>>() {
            @Override
            public Observable<Void> call(FileStatus fileStatus) {
              if (!fileStatus.isDirectory()) {
                return Observable.error(new AzureServiceErrorResponseException(
                    AzureServiceErrorCode.PRE_CONDITION_FAILED.getStatusCode(),
                    AzureServiceErrorCode.PRE_CONDITION_FAILED.getErrorCode(),
                    "listStatusAsync must be used with directories and not directories",
                    null));
              }

              return Observable.just(null);
            }
          });
        }

        return result.flatMap(new Func1<Void, Observable<FileStatus[]>>() {
          @Override
          public Observable<FileStatus[]> call(Void voidObj) {
            String relativePath = path.isRoot() ? null : getRelativePath(path);
            if (relativePath != null && !relativePath.endsWith(File.separator)) {
              relativePath += File.separator;
            }
            boolean remaining = true;
            String lastSegmentId = null;
            ArrayList<FileStatus> fileStatuses = new ArrayList<>();

            while (remaining) {
              ListSchema retrievedSchema = adfsHttpClient.listPathsAsync(
                  false,
                  adfsHttpClient.getSession().getFileSystem(),
                  FILE_SYSTEM,
                  relativePath,
                  lastSegmentId,
                  LIST_MAX_RESULTS,
                  null,
                  FileSystemConfigurations.FS_AZURE_DEFAULT_CONNECTION_TIMEOUT,
                  null,
                  null).toBlocking().single();

              long blockSize = configurationService.getAzureBlockSize();

              for (ListEntrySchema entry : retrievedSchema.paths()) {
                final DateTime dateTime = DateTime.parse(
                    entry.lastModified(),
                    DateTimeFormat.forPattern(DATE_TIME_PATTERN).withZoneUTC());

                fileStatuses.add(
                    new FileStatus(
                        entry.contentLength(),
                        entry.isDirectory() == null ? false : true,
                        1,
                        blockSize,
                        dateTime.getMillis(),
                        azureDistributedFileSystem.makeQualified(new Path(File.separator + entry.name()))));
              }

              lastSegmentId = retrievedSchema.segmentId();
              if (lastSegmentId == null || lastSegmentId.isEmpty()) {
                remaining = false;
              }
            }

            return Observable.just(fileStatuses.toArray(new FileStatus[0]));
          }
        }).toBlocking().single();
      }
    };

    return submitTaskToQueue(adfsHttpClient, readExecutorService, asyncCallable);
  }

  @Override
  public synchronized void closeFileSystem(final AzureDistributedFileSystem azureDistributedFileSystem) throws AzureDistributedFileSystemException {
    final AdfsHttpClient adfsHttpClient = this.adfsHttpClientCache.remove(azureDistributedFileSystem.getUri());

    if (adfsHttpClient != null) {
      adfsHttpClient.close();
    }
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
    return this.adfsHttpClientCache.get(azureDistributedFileSystem.getUri());
  }

  private <T> T execute(final Callable<T> callableRestOperation) throws
      AzureDistributedFileSystemException {

    try {
      return callableRestOperation.call();
    } catch (ErrorSchemaException exception) {
      throw parseErrorSchemaException(exception);
    } catch (ExecutionException exception) {
      Throwable rootCause = ExceptionUtils.getRootCause(exception);
      if (rootCause instanceof ErrorSchemaException) {
        throw parseErrorSchemaException((ErrorSchemaException) rootCause);
      }
      else {
        throw new InvalidAzureServiceErrorResponseException(exception);
      }
    } catch (Exception exception) {
      throw new InvalidAzureServiceErrorResponseException(exception);
    }
  }

  private AzureServiceErrorResponseException parseErrorSchemaException(ErrorSchemaException exception) {
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

    return new AzureServiceErrorResponseException(statusCode, errorCode, errorMessage, exception);
  }

  private String getResource(final boolean isDirectory) {
    return isDirectory ? DIRECTORY : FILE;
  }

  private long getContentLength(final Hashtable<String, String> properties) {
    if (properties.containsKey(CONTENT_LENGTH)) {
      return Long.parseLong(properties.get(CONTENT_LENGTH));
    }

    return 0;
  }

  private boolean isDirectory(final Hashtable<String, String> properties) {
    return properties.containsKey(HDI_IS_FOLDER) && Boolean.parseBoolean(properties.get(HDI_IS_FOLDER));
  }

  private DateTime getLastModifiedTime(final Hashtable<String, String> properties) {
    return DateTime.parse(
        properties.get(LAST_MODIFIED),
        DateTimeFormat.forPattern(DATE_TIME_PATTERN).withZoneUTC());
  }

  private <T> Future<T> submitTaskToQueue(
      final AdfsHttpClient adfsHttpClient,
      final ThreadPoolExecutor threadPoolExecutor,
      final Callable<T> task) {
    if (adfsHttpClient.getSession().getSessionState() != AdfsHttpClientSessionState.OPEN) {
      throw new IllegalStateException("Cannot execute task in a closed session");
    }

    for (;;){
      try {
        return threadPoolExecutor.submit(task);
      }
      catch (RejectedExecutionException ex) {
        // Ignore and retry
      }
    }
  }

  private ThreadPoolExecutor createThreadPoolExecutor(int maxConcurrentThreads) {
    return new ThreadPoolExecutor(
        maxConcurrentThreads,
        maxConcurrentThreads,
        0L,
        TimeUnit.MILLISECONDS,
        new SynchronousQueue<Runnable>(),
        new ThreadPoolExecutor.AbortPolicy());
  }

  // Internal calls are provided to prevent deadlocks in the thread executor pool
  private Observable<Hashtable<String, String>> getFileSystemPropertiesInternal(final AdfsHttpClient adfsHttpClient) {
    final Hashtable<String, String> properties = new Hashtable<>();

    Observable<ServiceResponseWithHeaders<Void, GetFilesystemPropertiesHeaders>> response =
        adfsHttpClient.getFilesystemPropertiesWithServiceResponseAsync(
            adfsHttpClient.getSession().getFileSystem(),
            FILE_SYSTEM);

    return response.flatMap(new Func1<ServiceResponseWithHeaders<Void, GetFilesystemPropertiesHeaders>, Observable<Hashtable<String, String>>>() {
      @Override
      public Observable<Hashtable<String, String>> call(ServiceResponseWithHeaders<Void, GetFilesystemPropertiesHeaders>
          voidGetFilesystemPropertiesHeadersServiceResponseWithHeaders) {
        try {
          Headers headers = voidGetFilesystemPropertiesHeadersServiceResponseWithHeaders.headResponse().raw().headers();
          properties.putAll(parseHeaders(headers));
          String xMsProperties = voidGetFilesystemPropertiesHeadersServiceResponseWithHeaders.headers().xMsProperties();
          properties.putAll(parseXMsProperties(xMsProperties));
          return Observable.just(properties);
        }
        catch (Exception ex) {
          return Observable.error(ex);
        }
      }
    });
  }

  // Internal calls are provided to prevent deadlocks in the thread executor pool
  private Observable<Hashtable<String, String>> getPathPropertiesInternal(final AdfsHttpClient adfsHttpClient, final Path path) {
    final Hashtable<String, String> properties = new Hashtable<>();
    Observable<ServiceResponseWithHeaders<Void, GetPathPropertiesHeaders>> response =
        adfsHttpClient.getPathPropertiesWithServiceResponseAsync(
            adfsHttpClient.getSession().getFileSystem(),
            getRelativePath(path));

    return response.flatMap(new Func1<ServiceResponseWithHeaders<Void, GetPathPropertiesHeaders>, Observable<Hashtable<String, String>>>() {
      @Override
      public Observable<Hashtable<String, String>> call(ServiceResponseWithHeaders<Void, GetPathPropertiesHeaders>
          voidGetPathPropertiesHeadersServiceResponseWithHeaders) {
        try {
          Headers headers = voidGetPathPropertiesHeadersServiceResponseWithHeaders.headResponse().headers();
          properties.putAll(parseHeaders(headers));
          String xMsProperties = voidGetPathPropertiesHeadersServiceResponseWithHeaders.headers().xMsProperties();
          properties.putAll(parseXMsProperties(xMsProperties));
          return Observable.just(properties);
        }
        catch (Exception ex) {
          return Observable.error(ex);
        }
      }
    });
  }

  // Internal calls are provided to prevent deadlocks in the thread executor pool
  private Observable<OutputStream> openFileForWriteInternal(
      final AzureDistributedFileSystem azureDistributedFileSystem,
      final AdfsHttpClient adfsHttpClient, final Path path, final boolean overwrite) throws AzureDistributedFileSystemException {
    return getFileStatusInternal(adfsHttpClient, path).flatMap(new Func1<FileStatus, Observable<OutputStream>>() {
      @Override
      public Observable<OutputStream> call(FileStatus fileStatus) {
        if (fileStatus.isDirectory()) {
          return Observable.error(new AzureServiceErrorResponseException(
              AzureServiceErrorCode.PRE_CONDITION_FAILED.getStatusCode(),
              AzureServiceErrorCode.PRE_CONDITION_FAILED.getErrorCode(),
              "openFileForWriteAsync must be used with files and not directories",
              null));
        }

        long offset = fileStatus.getLen();

        if (overwrite) {
          offset = 0;
        }

        return Observable.just(adfsStreamFactory.createWriteStream(
            azureDistributedFileSystem,
            path,
            offset));
      }
    });
  }

  // Internal calls are provided to prevent deadlocks in the thread executor pool
  private Observable<FileStatus> getFileStatusInternal(final AdfsHttpClient adfsHttpClient, final Path path)
      throws AzureDistributedFileSystemException {
    Observable<Hashtable<String, String>> getPropertiesObservable;

    if (path.isRoot()) {
      getPropertiesObservable = getFileSystemPropertiesInternal(adfsHttpClient);
    }
    else {
      getPropertiesObservable = getPathPropertiesInternal(adfsHttpClient, path);
    }

    return getPropertiesObservable.flatMap(new Func1<Hashtable<String, String>, Observable<FileStatus>>() {
      @Override
      public Observable<FileStatus> call(Hashtable<String, String> properties) {
        long blockSize = configurationService.getAzureBlockSize();

        return Observable.just(
            new FileStatus(
                getContentLength(properties),
                path.isRoot() || isDirectory(properties),
                1,
                blockSize,
                getLastModifiedTime(properties).getMillis(), path));
      }
    });
  }

  private Hashtable<String, String> parseHeaders(Headers headers) {
    Map<String, List<String>> headersMap = headers.toMultimap();
    Hashtable<String, String> properties = new Hashtable<>();

    for (Map.Entry<String, List<String>> header : headersMap.entrySet()) {
      String key = WordUtils.capitalizeFully(header.getKey(), "-".toCharArray());
      List<String> values = header.getValue();
      properties.put(key, StringUtils.join(',', values));
    }

    return properties;
  }

  private Hashtable<String, String> parseXMsProperties(String xMsProperties) throws InvalidFileSystemPropertyException {
    Hashtable<String, String> properties = new Hashtable<>();

    if (xMsProperties != null && !xMsProperties.isEmpty()) {
      String[] userProperties = xMsProperties.split(",");

      if (userProperties.length == 0) {
        return properties;
      }

      for (String property : userProperties) {
        if (property.isEmpty()) {
          throw new InvalidFileSystemPropertyException(xMsProperties);
        }

        String[] nameValue = property.split("=");
        if (nameValue.length != 2) {
          throw new InvalidFileSystemPropertyException(xMsProperties);
        }

        properties.put(nameValue[0], nameValue[1]);
      }
    }

    return properties;
  }

  private String getRangeHeader(long offset, long length) {
    return "bytes=" + offset + "-" + (offset + length - 1);
  }
}