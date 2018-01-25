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
import com.microsoft.azure.dfs.rest.client.generated.models.ContentTypes;
import com.microsoft.azure.dfs.rest.client.generated.models.CreatePathHeaders;
import com.microsoft.azure.dfs.rest.client.generated.models.DeletePathHeaders;
import com.microsoft.azure.dfs.rest.client.generated.models.ErrorSchemaException;
import com.microsoft.azure.dfs.rest.client.generated.models.GetFilesystemPropertiesHeaders;
import com.microsoft.azure.dfs.rest.client.generated.models.GetPathPropertiesHeaders;
import com.microsoft.azure.dfs.rest.client.generated.models.ListEntrySchema;
import com.microsoft.azure.dfs.rest.client.generated.models.ListSchema;
import com.microsoft.azure.dfs.rest.client.generated.models.ReadPathHeaders;
import com.microsoft.rest.ServiceResponseWithHeaders;
import com.sun.istack.Nullable;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufInputStream;
import okio.BufferedSource;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import rx.Observable;
import rx.functions.Func1;

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
import org.apache.hadoop.fs.azuredfs.contracts.exceptions.AzureServiceNetworkException;
import org.apache.hadoop.fs.azuredfs.contracts.exceptions.InvalidAzureServiceErrorResponseException;
import org.apache.hadoop.fs.azuredfs.contracts.exceptions.InvalidFileSystemPropertyException;
import org.apache.hadoop.fs.azuredfs.contracts.exceptions.TimeoutException;
import org.apache.hadoop.fs.azuredfs.contracts.services.AdfsHttpClient;
import org.apache.hadoop.fs.azuredfs.contracts.services.AdfsHttpClientFactory;
import org.apache.hadoop.fs.azuredfs.contracts.services.AdfsHttpClientSessionState;
import org.apache.hadoop.fs.azuredfs.contracts.services.AdfsHttpService;
import org.apache.hadoop.fs.azuredfs.contracts.services.AdfsStreamFactory;
import org.apache.hadoop.fs.azuredfs.contracts.services.AzureServiceErrorCode;
import org.apache.hadoop.fs.azuredfs.contracts.services.ConfigurationService;
import org.apache.hadoop.fs.azuredfs.contracts.services.LoggingService;
import org.apache.hadoop.fs.azuredfs.contracts.services.TracingService;
import org.apache.htrace.core.SpanId;
import org.apache.htrace.core.TraceScope;

import static org.apache.hadoop.util.Time.now;

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
  private static final String CONDITIONAL_ALL = "*";
  private static final int LIST_MAX_RESULTS = 5000;
  private static final int DELETE_DIRECTORY_TIMEOUT_MILISECONDS = 120000;
  private static final int RENAME_DIRECTORY_TIMEOUT_MILISECONDS = 120000;

  private final AdfsHttpClientFactory adfsHttpClientFactory;
  private final AdfsStreamFactory adfsStreamFactory;
  private final ConcurrentHashMap<URI, AdfsHttpClient> adfsHttpClientCache;
  private final ThreadPoolExecutor writeExecutorService;
  private final ThreadPoolExecutor readExecutorService;
  private final ConfigurationService configurationService;
  private final TracingService tracingService;
  private final LoggingService loggingService;

  @Inject
  AdfsHttpServiceImpl(
      final ConfigurationService configurationService,
      final AdfsHttpClientFactory adfsHttpClientFactory,
      final AdfsStreamFactory adfsStreamFactory,
      final TracingService tracingService,
      final LoggingService loggingService) {
    Preconditions.checkNotNull(adfsHttpClientFactory, "adfsHttpClientFactory");
    Preconditions.checkNotNull(adfsStreamFactory, "adfsStreamFactory");
    Preconditions.checkNotNull(configurationService, "configurationService");
    Preconditions.checkNotNull(tracingService, "tracingService");
    Preconditions.checkNotNull(loggingService, "loggingService");

    this.configurationService = configurationService;
    this.adfsStreamFactory = adfsStreamFactory;
    this.adfsHttpClientCache = new ConcurrentHashMap<>();
    this.adfsHttpClientFactory = adfsHttpClientFactory;
    this.tracingService = tracingService;
    this.loggingService = loggingService.get(AdfsHttpService.class);

    int maxConcurrentWriteThreads = this.configurationService.getMaxConcurrentWriteThreads();
    int maxConcurrentReadThreads = this.configurationService.getMaxConcurrentReadThreads();

    this.loggingService.debug(
        "Creating AdfsHttpServiceImpl with read pool capacity of {0} and write pool capacity of {1}",
        maxConcurrentReadThreads,
        maxConcurrentWriteThreads);

    this.readExecutorService = createThreadPoolExecutor(maxConcurrentReadThreads);
    this.writeExecutorService = createThreadPoolExecutor(maxConcurrentWriteThreads);
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
    return execute(
      "AdfsHttpServiceImpl.getFilesystemProperties",
      new Callable<Hashtable<String, String>>() {
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

    this.loggingService.debug(
        "getFilesystemPropertiesAsync for filesystem: {0}",
        adfsHttpClient.getSession().getFileSystem());

    final Callable<Hashtable<String, String>> asyncCallable = new Callable<Hashtable<String, String>>() {
      @Override
      public Hashtable<String, String> call() throws Exception {
        String xmsProperties = getFileSystemPropertiesInternal(adfsHttpClient).toBlocking().single().xMsProperties();
        Hashtable<String, String> parsedXmsProperties = parseXMsProperties(xmsProperties);

        loggingService.debug(
            "Fetched filesystem properties for fs: {0}, properties: {1}",
            adfsHttpClient.getSession().getFileSystem(),
            parsedXmsProperties);

        return parsedXmsProperties;
      }
    };

    return executeAsync("AdfsHttpServiceImpl.getFilesystemPropertiesAsync", adfsHttpClient, readExecutorService, asyncCallable);
  }

  @Override
  public void setFilesystemProperties(final AzureDistributedFileSystem azureDistributedFileSystem, final Hashtable<String, String> properties) throws
      AzureDistributedFileSystemException {
    execute("AdfsHttpServiceImpl.setFilesystemProperties",
      new Callable<Void>() {
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
    this.loggingService.debug(
        "setFilesystemPropertiesAsync for filesystem: {0} with properties: {1}",
        adfsHttpClient.getSession().getFileSystem(),
        properties);

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
            null /* ifMatch */,
            null /* ifNoneMatch */,
            null /* ifModifiedSince */,
            null /* ifUnmodifiedSince */,
            null /* xMsClientRequestId */,
            FileSystemConfigurations.FS_AZURE_DEFAULT_CONNECTION_TIMEOUT,
            null /* xMsDate */).toBlocking().single();
      }
    };

    return executeAsync("AdfsHttpServiceImpl.setFilesystemPropertiesAsync", adfsHttpClient, writeExecutorService, asyncCallable);
  }

  @Override
  public Hashtable<String, String> getPathProperties(final AzureDistributedFileSystem azureDistributedFileSystem, final Path path) throws
      AzureDistributedFileSystemException {
    return execute(
      "AdfsHttpServiceImpl.getPathProperties",
      new Callable<Hashtable<String, String>>() {
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

    this.loggingService.debug(
        "getPathPropertiesAsync for filesystem: {0} path: {1}",
        adfsHttpClient.getSession().getFileSystem(),
        path.toString());

    final Callable<Hashtable<String, String>> asyncCallable = new Callable<Hashtable<String, String>>() {
      @Override
      public Hashtable<String, String> call() throws Exception {
        String xmsProperties = getPathPropertiesInternal(adfsHttpClient, path).toBlocking().single().xMsProperties();
        Hashtable<String, String> parsedXmsProperties = parseXMsProperties(xmsProperties);

        loggingService.debug(
            "getPathPropertiesAsync properties for filesystem: {0} path: {1} properties: {2}",
            adfsHttpClient.getSession().getFileSystem(),
            path.toString(),
            parsedXmsProperties);

        return parsedXmsProperties;
      }
    };

    return executeAsync("AdfsHttpServiceImpl.getPathPropertiesAsync", adfsHttpClient, readExecutorService, asyncCallable);
  }

  @Override
  public void createFilesystem(final AzureDistributedFileSystem azureDistributedFileSystem) throws AzureDistributedFileSystemException {
    execute(
      "AdfsHttpServiceImpl.createFilesystem",
      new Callable<Void>() {
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

    this.loggingService.debug(
        "createFilesystemAsync for filesystem: {0}",
        adfsHttpClient.getSession().getFileSystem());

    final Callable<Void> asyncCallable = new Callable<Void>() {
      @Override
      public Void call() throws Exception {
        return adfsHttpClient.createFilesystemAsync(
            adfsHttpClient.getSession().getFileSystem(),
            FILE_SYSTEM,
            null /* xMsProperties */,
            null /* xMsOriginationId */,
            null /* xMsClientRequestId */,
            FileSystemConfigurations.FS_AZURE_DEFAULT_CONNECTION_TIMEOUT,
            null /* xMsDate */).toBlocking().single();
      }
    };

    return executeAsync("AdfsHttpServiceImpl.createFilesystemAsync", adfsHttpClient, writeExecutorService, asyncCallable);
  }

  @Override
  public void deleteFilesystem(final AzureDistributedFileSystem azureDistributedFileSystem) throws AzureDistributedFileSystemException {
    execute(
      "AdfsHttpServiceImpl.deleteFilesystem",
      new Callable<Void>() {
        @Override
        public Void call() throws Exception {
          return deleteFilesystemAsync(azureDistributedFileSystem).get();
        }
      });
  }

  @Override
  public Future<Void> deleteFilesystemAsync(final AzureDistributedFileSystem azureDistributedFileSystem) throws AzureDistributedFileSystemException {
    final AdfsHttpClient adfsHttpClient = this.getFileSystemClient(azureDistributedFileSystem);
    this.loggingService.debug(
        "deleteFilesystemAsync for filesystem: {0}",
        adfsHttpClient.getSession().getFileSystem());

    final Callable<Void> asyncCallable = new Callable<Void>() {
      @Override
      public Void call() throws Exception {
        return adfsHttpClient.deleteFilesystemAsync(
            adfsHttpClient.getSession().getFileSystem(), FILE_SYSTEM).toBlocking().single();
      }
    };

    return executeAsync("AdfsHttpServiceImpl.deleteFilesystemAsync", adfsHttpClient, writeExecutorService, asyncCallable);
  }

  @Override
  public OutputStream createFile(final AzureDistributedFileSystem azureDistributedFileSystem, final Path path, final boolean overwrite) throws
      AzureDistributedFileSystemException {
    return execute(
      "AdfsHttpServiceImpl.createFile",
      new Callable<OutputStream>() {
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

    this.loggingService.debug(
        "createFileAsync filesystem: {0} path: {1} overwrite: {2}",
        adfsHttpClient.getSession().getFileSystem(),
        path.toString(),
        overwrite);

    final Callable<OutputStream> asyncCallable = new Callable<OutputStream>() {
      @Override
      public OutputStream call() throws Exception {
        String ifNoneMatch = null;
        if (!overwrite) {
          ifNoneMatch = CONDITIONAL_ALL;
        }

        return adfsHttpClient.createPathAsync(
            getResource(false),
            ContentTypes.APPLICATIONOCTET_STREAM.toString(),
            adfsHttpClient.getSession().getFileSystem(),
            getRelativePath(path),
            null /* continuation */,
            null /* contentLength */,
            null /* contentEncoding */,
            null /* contentLanguage */,
            null /* contentMD5 */,
            null /* xMsCacheControl */,
            null /* xMsContentType */,
            null /* xMsContentEncoding */,
            null /* xMsContentLanguage */,
            null /* xMsContentMd5 */,
            null /* xMsRenameSource */,
            null /* xMsLeaseAction */,
            null /* xMsLeaseId */,
            null /* xMsProposedLeaseId */,
            null /* xMsSourceLeaseAction */,
            null /* xMsSourceLeaseId */,
            null /* xMsProperties */,
            null /* xMsOriginationId */,
            null /* ifMatch */,
            ifNoneMatch,
            null /* ifModifiedSince */,
            null /* ifUnmodifiedSince */,
            null /* xMsSourceIfMatch */,
            null /* xMsSourceIfNoneMatch */,
            null /* xMsSourceIfModifiedSince */,
            null /* xMsSourceIfUnmodifiedSince */,
            null /* requestBody */,
            null /* xMsClientRequestId */,
            FileSystemConfigurations.FS_AZURE_DEFAULT_CONNECTION_TIMEOUT,
            null /* xMsDate */).
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

    return executeAsync("AdfsHttpServiceImpl.createFileAsync", adfsHttpClient, writeExecutorService, asyncCallable);
  }

  @Override
  public Void createDirectory(final AzureDistributedFileSystem azureDistributedFileSystem, final Path path) throws AzureDistributedFileSystemException {
    return execute(
      "AdfsHttpServiceImpl.createDirectory",
      new Callable<Void>() {
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
    this.loggingService.debug(
        "createDirectoryAsync filesystem: {0} path: {1}",
        adfsHttpClient.getSession().getFileSystem(),
        path.toString());

    final Callable<Void> asyncCallable = new Callable<Void>() {
      @Override
      public Void call() throws Exception {
        return adfsHttpClient.createPathAsync(
            getResource(true),
            ContentTypes.APPLICATIONOCTET_STREAM.toString(),
            adfsHttpClient.getSession().getFileSystem(),
            getRelativePath(path),
            null /* continuation */,
            "0",
            null /* contentEncoding */,
            null /* contentLanguage */,
            null /* contentMD5 */,
            null /* xMsCacheControl */,
            null /* xMsContentType */,
            null /* xMsContentEncoding */,
            null /* xMsContentLanguage */,
            null /* xMsContentMd5 */,
            null /* xMsRenameSource */,
            null /* xMsLeaseAction */,
            null /* xMsLeaseId */,
            null /* xMsProposedLeaseId */,
            null /* xMsSourceLeaseAction */,
            null /* xMsSourceLeaseId */,
            null /* xMsProperties */,
            null /* xMsOriginationId */,
            null /* ifMatch */,
            null /* ifNoneMatch */,
            null /* ifModifiedSince */,
            null /* ifUnmodifiedSince */,
            null /* xMsSourceIfMatch */,
            null /* xMsSourceIfNoneMatch */,
            null /* xMsSourceIfModifiedSince */,
            null /* xMsSourceIfUnmodifiedSince */,
            null /* requestBody */,
            null /* xMsClientRequestId */,
            FileSystemConfigurations.FS_AZURE_DEFAULT_CONNECTION_TIMEOUT,
            null /* xMsDate */).toBlocking().single();
      }
    };

    return executeAsync("AdfsHttpServiceImpl.createDirectoryAsync", adfsHttpClient, writeExecutorService, asyncCallable);
  }

  @Override
  public InputStream openFileForRead(final AzureDistributedFileSystem azureDistributedFileSystem, final Path path) throws AzureDistributedFileSystemException {
    return execute(
      "AdfsHttpServiceImpl.openFileForRead",
      new Callable<InputStream>() {
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
    this.loggingService.debug(
        "openFileForReadAsync filesystem: {0} path: {1}",
        adfsHttpClient.getSession().getFileSystem(),
        path.toString());

    final Callable<InputStream> asyncCallable = new Callable<InputStream>() {
      @Override
      public InputStream call() throws Exception {
        return getFileStatusInternal(adfsHttpClient, path)
            .flatMap(new Func1<VersionedFileStatus, Observable<InputStream>>() {
              @Override
              public Observable<InputStream> call(VersionedFileStatus fileStatus) {
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
                    fileStatus.getLen(),
                    fileStatus.version));
              }
            }).toBlocking().single();
      }
    };

    return executeAsync("AdfsHttpServiceImpl.openFileForReadAsync", adfsHttpClient, readExecutorService, asyncCallable);
  }


  @Override
  public OutputStream openFileForWrite(final AzureDistributedFileSystem azureDistributedFileSystem, final Path path, final boolean overwrite) throws
      AzureDistributedFileSystemException {
    return execute(
      "AdfsHttpServiceImpl.openFileForWrite",
      new Callable<OutputStream>() {
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
    this.loggingService.debug(
        "openFileForWriteAsync filesystem: {0} path: {1} overwrite: {2}",
        adfsHttpClient.getSession().getFileSystem(),
        path.toString(),
        overwrite);

    final Callable<OutputStream> asyncCallable = new Callable<OutputStream>() {
      @Override
      public OutputStream call() throws Exception {
        return openFileForWriteInternal(azureDistributedFileSystem, adfsHttpClient, path, overwrite).toBlocking().single();
      }
    };

    return executeAsync("AdfsHttpServiceImpl.openFileForWriteAsync", adfsHttpClient, readExecutorService, asyncCallable);
  }

  @Override
  public Integer readFile(final AzureDistributedFileSystem azureDistributedFileSystem, final Path path, final String version, final long offset, final int
      length, final ByteBuf readBuffer, final int readBufferOffset) throws
      AzureDistributedFileSystemException {
    return execute(
      "AdfsHttpServiceImpl.readFile",
      new Callable<Integer>() {
        @Override
        public Integer call() throws Exception {
          return readFileAsync(azureDistributedFileSystem, path, version, offset, length, readBuffer, readBufferOffset).get();
        }
      });
  }

  @Override
  public Future<Integer> readFileAsync(final AzureDistributedFileSystem azureDistributedFileSystem, final Path path, final String version, final long offset,
      final int length, final ByteBuf targetBuffer, final int targetBufferOffset) throws
      AzureDistributedFileSystemException {
    final AdfsHttpClient adfsHttpClient = this.getFileSystemClient(azureDistributedFileSystem);
    this.loggingService.debug(
        "readFileAsync filesystem: {0} path: {1} offset: {2} length: {3} targetBufferOffset: {4}",
        adfsHttpClient.getSession().getFileSystem(),
        path.toString(),
        offset,
        length,
        targetBufferOffset);

    final Callable<Integer> asyncCallable = new Callable<Integer>() {
      @Override
      public Integer call() throws Exception {
        final boolean tolerateOobAppends = configurationService.getTolerateOobAppends();
        return adfsHttpClient.readPathWithServiceResponseAsync(
            adfsHttpClient.getSession().getFileSystem(),
            getRelativePath(path),
            getRangeHeader(offset, length),
            tolerateOobAppends ? CONDITIONAL_ALL : version,
            null /* ifNoneMatch */,
            null /* ifModifiedSince */,
            null /* ifUnmodifiedSince */,
            null /* xMsClientRequestId */,
            FileSystemConfigurations.FS_AZURE_DEFAULT_CONNECTION_READ_TIMEOUT,
            null /* xMsDate */).
            flatMap(new Func1<ServiceResponseWithHeaders<InputStream, ReadPathHeaders>, Observable<Integer>>() {
              @Override
              public Observable<Integer> call(ServiceResponseWithHeaders<InputStream, ReadPathHeaders> serviceResponseWithHeaders) {
                try {
                  int readBytes;
                  int remainingBytes = length;
                  int offset = targetBufferOffset;
                  BufferedSource inputStream = serviceResponseWithHeaders.response().body().source();
                  while ((readBytes = inputStream.read(targetBuffer.array(), offset, remainingBytes)) != -1) {
                    offset += readBytes;
                    remainingBytes -= readBytes;
                  }

                  inputStream.close();
                  return Observable.just(offset - targetBufferOffset);
                } catch (Exception ex) {
                  return Observable.error(ex);
                }
              }
            }).toBlocking().single();
      }
    };

    return executeAsync("AdfsHttpServiceImpl.readFileAsync", adfsHttpClient, readExecutorService, asyncCallable);
  }

  @Override
  public void writeFile(final AzureDistributedFileSystem azureDistributedFileSystem, final Path path, final ByteBuf body, final long offset) throws
      AzureDistributedFileSystemException {
    execute(
      "AdfsHttpServiceImpl.writeFile",
      new Callable<Void>() {
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
    this.loggingService.debug(
        "writeFileAsync filesystem: {0} path: {1} offset: {2}",
        adfsHttpClient.getSession().getFileSystem(),
        path.toString(),
        offset);

    final Callable<Void> asyncCallable = new Callable<Void>() {
      @Override
      public Void call() throws Exception {
        Observable<Void> updatePathAsync = adfsHttpClient.updatePathAsync(
            getResource(false),
            adfsHttpClient.getSession().getFileSystem(),
            getRelativePath(path),
            "data",
            offset,
            false /* retainUncommittedData */,
            Integer.toString(body.readableBytes()),
            null /* contentMD5 */,
            null /* origin */,
            null /* xMsLeaseAction */,
            null /* xMsLeaseId */,
            null /* xMsCacheControl */,
            null /* xMsContentDisposition */,
            null /* xMsContentEncoding */,
            null /* xMsContentLanguage */,
            null /* xMsContentMd5 */,
            null /* xMsContentType */,
            null /* xMsProperties */,
            null /* ifMatch */,
            null /* ifNoneMatch */,
            null /* ifModifiedSince */,
            null /* ifUnmodifiedSince */,
            new ByteBufInputStream(body),
            null /* xMsClientRequestId */,
            FileSystemConfigurations.FS_AZURE_DEFAULT_CONNECTION_TIMEOUT,
            null /* xMsDate */);

        return updatePathAsync.toBlocking().single();
      }
    };

    return executeAsync("AdfsHttpServiceImpl.writeFileAsync", adfsHttpClient, writeExecutorService, asyncCallable);
  }

  @Override
  public void flushFile(final AzureDistributedFileSystem azureDistributedFileSystem, final Path path, final long offset, final boolean retainUncommitedData)
      throws
      AzureDistributedFileSystemException {
    execute(
      "AdfsHttpServiceImpl.flushFile",
      new Callable<Void>() {
        @Override
        public Void call() throws Exception {
          return flushFileAsync(azureDistributedFileSystem, path, offset, retainUncommitedData).get();
        }
      });
  }

  @Override
  public Future<Void> flushFileAsync(final AzureDistributedFileSystem azureDistributedFileSystem, final Path path, final long offset, final boolean
      retainUncommitedData) throws AzureDistributedFileSystemException {
    final AdfsHttpClient adfsHttpClient = this.getFileSystemClient(azureDistributedFileSystem);
    this.loggingService.debug(
        "flushFileAsync filesystem: {0} path: {1} offset: {2}",
        adfsHttpClient.getSession().getFileSystem(),
        path.toString(),
        offset);

    final Callable<Void> asyncCallable = new Callable<Void>() {
      @Override
      public Void call() throws Exception {
        return adfsHttpClient.updatePathAsync(
            getResource(false),
            adfsHttpClient.getSession().getFileSystem(),
            getRelativePath(path),
            "commit",
            offset,
            retainUncommitedData /* retainUncommitedData */,
            null /* contentLength */,
            null /* contentMD5 */,
            null /* origin */,
            null /* xMsLeaseAction */,
            null /* xMsLeaseId */,
            null /* xMsCacheControl */,
            null /* xMsContentDisposition */,
            null /* xMsContentEncoding */,
            null /* xMsContentLanguage */,
            null /* xMsContentMd5 */,
            null /* xMsContentType */,
            null /* xMsProperties */,
            null /* ifMatch */,
            null /* ifNoneMatch */,
            null /* ifModifiedSince */,
            null /* ifUnmodifiedSince */,
            null /* requestBody */,
            null /* xMsClientRequestId */,
            FileSystemConfigurations.FS_AZURE_DEFAULT_CONNECTION_TIMEOUT,
            null /* xMsDate */).toBlocking().single();
      }
    };

    return executeAsync("AdfsHttpServiceImpl.flushFileAsync", adfsHttpClient, writeExecutorService, asyncCallable);
  }

  @Override
  public void renameFile(final AzureDistributedFileSystem azureDistributedFileSystem, final Path source, final Path destination)
      throws
      AzureDistributedFileSystemException {
    execute(
      "AdfsHttpServiceImpl.renameFile",
      new Callable<Void>() {
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
    this.loggingService.debug(
        "renameFileAsync filesystem: {0} source: {1} destination: {2}",
        adfsHttpClient.getSession().getFileSystem(),
        source.toString(),
        destination.toString());

    final Callable<Void> asyncCallable = new Callable<Void>() {
      @Override
      public Void call() throws Exception {
        return adfsHttpClient.createPathAsync(
            getResource(false),
            ContentTypes.APPLICATIONOCTET_STREAM.toString(),
            adfsHttpClient.getSession().getFileSystem(),
            getRelativePath(destination),
            null /* continuation */,
            null /* contentLength */,
            null /* contentEncoding */,
            null /* contentLanguage */,
            null /* contentMD5 */,
            null /* xMsCacheControl */,
            null /* xMsContentType */,
            null /* xMsContentEncoding */,
            null /* xMsContentLanguage */,
            null /* xMsContentMd5 */,
            Path.SEPARATOR + adfsHttpClient.getSession().getFileSystem() + Path.SEPARATOR + getRelativePath(source),
            null /* xMsLeaseAction */,
            null /* xMsLeaseId */,
            null /* xMsProposedLeaseId */,
            SOURCE_LEASE_ACTION_ACQUIRE,
            null /* xMsSourceLeaseId */,
            null /* xMsProperties */,
            null /* xMsOriginationId */,
            null /* ifMatch */,
            null /* ifNoneMatch */,
            null /* ifModifiedSince */,
            null /* ifUnmodifiedSince */,
            null /* xMsSourceIfMatch */,
            null /* xMsSourceIfNoneMatch */,
            null /* xMsSourceIfModifiedSince */,
            null /* xMsSourceIfUnmodifiedSince */,
            null /* requestBody */,
            null /* xMsClientRequestId */,
            FileSystemConfigurations.FS_AZURE_DEFAULT_CONNECTION_TIMEOUT,
            null /* xMsDate */).toBlocking().single();
      }
    };

    return executeAsync("AdfsHttpServiceImpl.renameFileAsync", adfsHttpClient, writeExecutorService, asyncCallable);
  }

  @Override
  public void renameDirectory(final AzureDistributedFileSystem azureDistributedFileSystem, final Path source, final Path destination) throws
      AzureDistributedFileSystemException {
    execute(
        "AdfsHttpServiceImpl.renameDirectory",
        new Callable<Void>() {
          @Override
          public Void call() throws Exception {
            renameDirectoryAsync(azureDistributedFileSystem, source, destination).get();
            return null;
          }
        });
  }

  @Override
  public Future<Void> renameDirectoryAsync(final AzureDistributedFileSystem azureDistributedFileSystem, final Path source, final Path destination) throws
      AzureDistributedFileSystemException {
    final AdfsHttpClient adfsHttpClient = this.getFileSystemClient(azureDistributedFileSystem);
    this.loggingService.debug(
        "renameDirectory filesystem: {0} source: {1} destination: {2}",
        adfsHttpClient.getSession().getFileSystem(),
        source.toString(),
        destination.toString());

    final Callable<Void> asyncCallable = new Callable<Void>() {
      @Override
      public Void call() throws Exception {
        String continuation = null;
        boolean operationPending = true;

        long deadline = now() + RENAME_DIRECTORY_TIMEOUT_MILISECONDS;
        while (operationPending) {
          if (now() > deadline) {
            loggingService.debug(
                "Rename directory {0} to {1} timed out.",
                source,
                destination);

            throw new TimeoutException("Rename directory timed out.");
          }

          continuation = adfsHttpClient.createPathWithServiceResponseAsync(
              getResource(false),
              adfsHttpClient.getSession().getFileSystem(),
              getRelativePath(destination),
              continuation,
              null /* contentLength */,
              ContentTypes.APPLICATIONOCTET_STREAM.toString(),
              null /* contentEncoding */,
              null /* contentLanguage */,
              null /* contentMD5 */,
              null /* xMsCacheControl */,
              null /* xMsContentType */,
              null /* xMsContentEncoding */,
              null /* xMsContentLanguage */,
              null /* xMsContentMd5 */,
              Path.SEPARATOR + adfsHttpClient.getSession().getFileSystem() + Path.SEPARATOR + getRelativePath(source),
              null /* xMsLeaseAction */,
              null /* xMsLeaseId */,
              null /* xMsProposedLeaseId */,
              SOURCE_LEASE_ACTION_ACQUIRE,
              null /* xMsSourceLeaseId */,
              null /* xMsProperties */,
              null /* xMsOriginationId */,
              null /* ifMatch */,
              null /* ifNoneMatch */,
              null /* ifModifiedSince */,
              null /* ifUnmodifiedSince */,
              null /* xMsSourceIfMatch */,
              null /* xMsSourceIfNoneMatch */,
              null /* xMsSourceIfModifiedSince */,
              null /* xMsSourceIfUnmodifiedSince */,
              null /* requestBody */,
              null /* xMsClientRequestId */,
              FileSystemConfigurations.FS_AZURE_DEFAULT_CONNECTION_TIMEOUT,
              null /* xMsDate */).flatMap(new Func1<ServiceResponseWithHeaders<Void, CreatePathHeaders>, Observable<String>>() {
            @Override
            public Observable<String> call(ServiceResponseWithHeaders<Void, CreatePathHeaders> voidCreatePathHeadersServiceResponseWithHeaders) {
              return Observable.just(voidCreatePathHeadersServiceResponseWithHeaders.headers().xMsContinuation());
            }
          }).toBlocking().single();

          operationPending = continuation != null;
        }

        return null;
      }
    };

    return executeAsync("AdfsHttpServiceImpl.renameDirectoryAsync", adfsHttpClient, writeExecutorService, asyncCallable);
  }

  @Override
  public void deleteFile(final AzureDistributedFileSystem azureDistributedFileSystem, final Path path) throws
      AzureDistributedFileSystemException {
    execute(
      "AdfsHttpServiceImpl.deleteFile",
      new Callable<Void>() {
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
    this.loggingService.debug(
        "deleteFileAsync filesystem: {0} path: {1}",
        adfsHttpClient.getSession().getFileSystem(),
        path.toString());

    final Callable<Void> asyncCallable = new Callable<Void>() {
      @Override
      public Void call() throws Exception {
        return adfsHttpClient.deletePathAsync(getResource(false), adfsHttpClient.getSession().getFileSystem(), getRelativePath(path)).toBlocking().single();
      }
    };

    return executeAsync("AdfsHttpServiceImpl.deleteFileAsync", adfsHttpClient, writeExecutorService, asyncCallable);
  }

  @Override
  public void deleteDirectory(final AzureDistributedFileSystem azureDistributedFileSystem, final Path path, final boolean recursive) throws
      AzureDistributedFileSystemException {
    execute(
        "AdfsHttpServiceImpl.deleteDirectory",
        new Callable<Void>() {
          @Override
          public Void call() throws Exception {
            deleteDirectoryAsync(azureDistributedFileSystem, path, recursive).get();
            return null;
          }
        });
  }

  @Override
  public Future<Void> deleteDirectoryAsync(final AzureDistributedFileSystem azureDistributedFileSystem, final Path path, final boolean recursive) throws
      AzureDistributedFileSystemException {
    final AdfsHttpClient adfsHttpClient = this.getFileSystemClient(azureDistributedFileSystem);
    this.loggingService.debug(
        "deleteDirectoryAsync filesystem: {0} path: {1} recursive: {2}",
        adfsHttpClient.getSession().getFileSystem(),
        path.toString(),
        String.valueOf(recursive));

    final Callable<Void> asyncCallable = new Callable<Void>() {
      @Override
      public Void call() throws Exception {
        String continuation = null;
        boolean operationPending = true;

        long deadline = now() + DELETE_DIRECTORY_TIMEOUT_MILISECONDS;
        while (operationPending) {
          if (now() > deadline) {
            loggingService.debug(
                "Delete directory {0} timed out.",
                path);

            throw new TimeoutException("Delete directory timed out.");
          }

          continuation = adfsHttpClient.deletePathWithServiceResponseAsync(
              getResource(false),
              adfsHttpClient.getSession().getFileSystem(),
              getRelativePath(path),
              recursive,
              continuation,
              null /* xMsLeaseId */,
              null /* ifMatch */,
              null /* ifNonMatch */,
              null /* ifModifiedSince */,
              null /* ifUnmodifiedSince */,
              null /* xMsClientRequestId */,
              FileSystemConfigurations.FS_AZURE_DEFAULT_CONNECTION_TIMEOUT,
              null /* xMsDate */).flatMap(new Func1<ServiceResponseWithHeaders<Void, DeletePathHeaders>, Observable<String>>() {
            @Override
            public Observable<String> call(ServiceResponseWithHeaders<Void, DeletePathHeaders> voidDeletePathHeadersServiceResponseWithHeaders) {
              return Observable.just(voidDeletePathHeadersServiceResponseWithHeaders.headers().xMsContinuation());
            }
          }).toBlocking().single();

          operationPending = continuation != null;
        }

        return null;
      }
    };

    return executeAsync("AdfsHttpServiceImpl.deleteDirectoryAsync", adfsHttpClient, writeExecutorService, asyncCallable);
  }

  @Override
  public FileStatus getFileStatus(final AzureDistributedFileSystem azureDistributedFileSystem, final Path path) throws AzureDistributedFileSystemException {
    return execute(
      "AdfsHttpServiceImpl.getFileStatus",
      new Callable<FileStatus>() {
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
    this.loggingService.debug(
        "getFileStatusAsync filesystem: {0} path: {1}",
        adfsHttpClient.getSession().getFileSystem(),
        path.toString());

    final Callable<FileStatus> asyncCallable = new Callable<FileStatus>() {
      @Override
      public FileStatus call() throws Exception {
        return getFileStatusInternal(adfsHttpClient, path).toBlocking().single();
      }
    };

    return executeAsync("AdfsHttpServiceImpl.getFileStatusAsync", adfsHttpClient, readExecutorService, asyncCallable);
  }

  @Override
  public FileStatus[] listStatus(final AzureDistributedFileSystem azureDistributedFileSystem, final Path path) throws AzureDistributedFileSystemException {
    return execute(
      "AdfsHttpServiceImpl.listStatus",
      new Callable<FileStatus[]>() {
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
    this.loggingService.debug(
        "listStatusAsync filesystem: {0} path: {1}",
        adfsHttpClient.getSession().getFileSystem(),
        path.toString());

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
                  null /* xMsClientRequestId */,
                  FileSystemConfigurations.FS_AZURE_DEFAULT_CONNECTION_TIMEOUT,
                  null /* xMsDate */).toBlocking().single();

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

    return executeAsync("AdfsHttpServiceImpl.listStatusAsync", adfsHttpClient, readExecutorService, asyncCallable);
  }

  @Override
  public synchronized void closeFileSystem(final AzureDistributedFileSystem azureDistributedFileSystem) throws AzureDistributedFileSystemException {
    final AdfsHttpClient adfsHttpClient = this.adfsHttpClientCache.remove(azureDistributedFileSystem.getUri());

    if (adfsHttpClient != null) {
      this.loggingService.debug(
          "closeFileSystem filesystem: {0}",
          adfsHttpClient.getSession().getFileSystem());

      execute(
          "AdfsHttpServiceImpl.closeFileSystem",
          new Callable<Void>() {
            @Override
            public Void call() throws Exception {
              adfsHttpClient.close();
              return null;
            }
          }
      );
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

  private <T> T execute(
      final String scopeDescription,
      final Callable<T> callableRestOperation) throws
      AzureDistributedFileSystemException {
    return this.execute(scopeDescription, callableRestOperation, null);
  }

  private <T> T execute(
      final String scopeDescription,
      final Callable<T> callableRestOperation,
      @Nullable final SpanId currentTraceScopeId) throws
      AzureDistributedFileSystemException {

    TraceScope traceScope;
    if (currentTraceScopeId == null) {
      traceScope = this.tracingService.traceBegin(scopeDescription);
    }
    else {
      traceScope = this.tracingService.traceBegin(scopeDescription, currentTraceScopeId);
    }

    try {
      return callableRestOperation.call();
    } catch (ErrorSchemaException exception) {
      AzureServiceErrorResponseException ex = parseErrorSchemaException(exception);
      tracingService.traceException(traceScope, ex);
      throw ex;
    } catch (ExecutionException exception) {
      Throwable rootCause = ExceptionUtils.getRootCause(exception);
      if (rootCause instanceof ErrorSchemaException) {
        AzureServiceErrorResponseException ex = parseErrorSchemaException((ErrorSchemaException) rootCause);
        tracingService.traceException(traceScope, ex);
        throw ex;
      }
      else {
        InvalidAzureServiceErrorResponseException ex = new InvalidAzureServiceErrorResponseException(exception);
        tracingService.traceException(traceScope, ex);
        throw ex;
      }
    } catch (Exception exception) {
      tracingService.traceException(traceScope, new AzureServiceNetworkException(scopeDescription, exception));
      throw new InvalidAzureServiceErrorResponseException(exception);
    } finally {
      tracingService.traceEnd(traceScope);
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

  private long parseContentLength(final String contentLength) {
    if (contentLength == null) {
      return -1;
    }

    return Long.valueOf(contentLength);
  }

  private boolean parseIsDirectory(final String resourceType) {
    return resourceType.equalsIgnoreCase(DIRECTORY);
  }

  private DateTime parseLastModifiedTime(final String lastModifiedTime) {
    return DateTime.parse(
        lastModifiedTime,
        DateTimeFormat.forPattern(DATE_TIME_PATTERN).withZoneUTC());
  }

  private <T> Future<T> executeAsync(
      final String scopeDescription,
      final AdfsHttpClient adfsHttpClient,
      final ThreadPoolExecutor threadPoolExecutor,
      final Callable<T> task) {
    if (adfsHttpClient.getSession().getSessionState() != AdfsHttpClientSessionState.OPEN) {
      throw new IllegalStateException("Cannot execute task in a closed session");
    }

    final SpanId currentTraceScopeId = this.tracingService.getCurrentTraceScopeSpanId();

    for (;;){
      try {
        Callable<T> taskWithObservable = new Callable<T>() {
          @Override
          public T call() throws Exception {
            return execute(scopeDescription, task, currentTraceScopeId);
          }
        };

        return threadPoolExecutor.submit(taskWithObservable);
      }
      catch (RejectedExecutionException ex) {
        // Ignore and retry
      }
    }
  }

  private ThreadPoolExecutor createThreadPoolExecutor(int maxConcurrentThreads) {
    ThreadPoolExecutor threadPoolExecutor = new ThreadPoolExecutor(
        maxConcurrentThreads,
        maxConcurrentThreads,
        1L,
        TimeUnit.SECONDS,
        new SynchronousQueue<Runnable>(),
        new ThreadPoolExecutor.AbortPolicy());

    threadPoolExecutor.prestartCoreThread();
    threadPoolExecutor.prestartAllCoreThreads();

    return threadPoolExecutor;
  }

  // Internal calls are provided to prevent deadlocks in the thread executor pool
  private Observable<GetFilesystemPropertiesHeaders> getFileSystemPropertiesInternal(final AdfsHttpClient adfsHttpClient) {
    Observable<ServiceResponseWithHeaders<Void, GetFilesystemPropertiesHeaders>> response =
        adfsHttpClient.getFilesystemPropertiesWithServiceResponseAsync(
            adfsHttpClient.getSession().getFileSystem(),
            FILE_SYSTEM);

    return response.flatMap(new Func1<ServiceResponseWithHeaders<Void, GetFilesystemPropertiesHeaders>, Observable<GetFilesystemPropertiesHeaders>>() {
      @Override
      public Observable<GetFilesystemPropertiesHeaders> call(ServiceResponseWithHeaders<Void, GetFilesystemPropertiesHeaders>
          voidGetFilesystemPropertiesHeadersServiceResponseWithHeaders) {
        try {
          GetFilesystemPropertiesHeaders headers = voidGetFilesystemPropertiesHeadersServiceResponseWithHeaders.headers();
          return Observable.just(headers);
        }
        catch (Exception ex) {
          return Observable.error(ex);
        }
      }
    });
  }

  // Internal calls are provided to prevent deadlocks in the thread executor pool
  private Observable<GetPathPropertiesHeaders> getPathPropertiesInternal(final AdfsHttpClient adfsHttpClient, final Path path) {
    Observable<ServiceResponseWithHeaders<Void, GetPathPropertiesHeaders>> response =
        adfsHttpClient.getPathPropertiesWithServiceResponseAsync(
            adfsHttpClient.getSession().getFileSystem(),
            getRelativePath(path));

    return response.flatMap(new Func1<ServiceResponseWithHeaders<Void, GetPathPropertiesHeaders>, Observable<GetPathPropertiesHeaders>>() {
      @Override
      public Observable<GetPathPropertiesHeaders> call(ServiceResponseWithHeaders<Void, GetPathPropertiesHeaders>
          voidGetPathPropertiesHeadersServiceResponseWithHeaders) {
        try {
          GetPathPropertiesHeaders headers = voidGetPathPropertiesHeadersServiceResponseWithHeaders.headers();
          return Observable.just(headers);
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
  private Observable<VersionedFileStatus> getFileStatusInternal(final AdfsHttpClient adfsHttpClient, final Path path)
      throws AzureDistributedFileSystemException {
    final long blockSize = configurationService.getAzureBlockSize();

    if (path.isRoot()) {
      return getFileSystemPropertiesInternal(adfsHttpClient).flatMap(new Func1<GetFilesystemPropertiesHeaders, Observable<VersionedFileStatus>>() {
        @Override
        public Observable<VersionedFileStatus> call(GetFilesystemPropertiesHeaders getFilesystemPropertiesHeaders) {
          return Observable.just(
              new VersionedFileStatus(
                  parseContentLength(getFilesystemPropertiesHeaders.contentLength()),
                  true,
                  1,
                  blockSize,
                  parseLastModifiedTime(getFilesystemPropertiesHeaders.lastModified()).getMillis(),
                  path,
                  getFilesystemPropertiesHeaders.eTag()));
        }
      });
    }

    return getPathPropertiesInternal(adfsHttpClient, path).flatMap(new Func1<GetPathPropertiesHeaders, Observable<VersionedFileStatus>>() {
      @Override
      public Observable<VersionedFileStatus> call(GetPathPropertiesHeaders getPathPropertiesHeaders) {
        return Observable.just(
            new VersionedFileStatus(
                parseContentLength(getPathPropertiesHeaders.contentLength()),
                parseIsDirectory(getPathPropertiesHeaders.xMsResourceType()),
                1,
                blockSize,
                parseLastModifiedTime(getPathPropertiesHeaders.lastModified()).getMillis(),
                path,
                getPathPropertiesHeaders.eTag()));
      }
    });
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

  private class VersionedFileStatus extends FileStatus {
    private final String version;

    VersionedFileStatus(
        final long length, final boolean isdir, final int blockReplication,
        final long blocksize, final long modificationTime, final Path path,
        String version) {
      super(length, isdir, blockReplication, blocksize, modificationTime, path);

      Preconditions.checkNotNull(version, "version");
      Preconditions.checkArgument(version.length() > 0);

      this.version = version;
    }
  }
}