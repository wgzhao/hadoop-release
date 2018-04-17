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

package org.apache.hadoop.fs.azurebfs.services;

import javax.xml.bind.DatatypeConverter;
import java.io.File;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URLEncoder;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CharsetEncoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.TimeUnit;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.microsoft.azure.bfs.rest.client.generated.models.CreatePathHeaders;
import com.microsoft.azure.bfs.rest.client.generated.models.DeletePathHeaders;
import com.microsoft.azure.bfs.rest.client.generated.models.ErrorSchemaException;
import com.microsoft.azure.bfs.rest.client.generated.models.GetFilesystemPropertiesHeaders;
import com.microsoft.azure.bfs.rest.client.generated.models.GetPathPropertiesHeaders;
import com.microsoft.azure.bfs.rest.client.generated.models.ListEntrySchema;
import com.microsoft.azure.bfs.rest.client.generated.models.ListPathsHeaders;
import com.microsoft.azure.bfs.rest.client.generated.models.ListSchema;
import com.microsoft.azure.bfs.rest.client.generated.models.ReadPathHeaders;
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
import org.apache.hadoop.fs.azurebfs.AzureBlobFileSystem;
import org.apache.hadoop.fs.azurebfs.constants.FileSystemConfigurations;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.AzureBlobFileSystemException;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.AzureServiceErrorResponseException;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.AzureServiceNetworkException;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.InvalidAzureServiceErrorResponseException;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.InvalidFileSystemPropertyException;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.FileSystemOperationUnhandledException;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.TimeoutException;
import org.apache.hadoop.fs.azurebfs.contracts.services.AbfsHttpClient;
import org.apache.hadoop.fs.azurebfs.contracts.services.AbfsHttpClientFactory;
import org.apache.hadoop.fs.azurebfs.contracts.services.AbfsHttpClientSessionState;
import org.apache.hadoop.fs.azurebfs.contracts.services.AbfsHttpService;
import org.apache.hadoop.fs.azurebfs.contracts.services.AbfsStreamFactory;
import org.apache.hadoop.fs.azurebfs.contracts.services.AzureServiceErrorCode;
import org.apache.hadoop.fs.azurebfs.contracts.services.ConfigurationService;
import org.apache.hadoop.fs.azurebfs.contracts.services.LoggingService;
import org.apache.hadoop.fs.azurebfs.contracts.services.TracingService;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.htrace.core.SpanId;
import org.apache.htrace.core.TraceScope;

import static org.apache.hadoop.util.Time.now;

@Singleton
@InterfaceAudience.Private
@InterfaceStability.Evolving
final class AbfsHttpServiceImpl implements AbfsHttpService {
  private static final String FILE_SYSTEM = "filesystem";
  private static final String FILE = "file";
  private static final String DIRECTORY = "directory";
  private static final String DATE_TIME_PATTERN = "E, dd MMM yyyy HH:mm:ss 'GMT'";
  private static final String CONDITIONAL_ALL = "*";
  private static final String XMS_PROPERTIES_ENCODING = "ISO-8859-1";
  private static final int LIST_MAX_RESULTS = 5000;
  private static final int DELETE_DIRECTORY_TIMEOUT_MILISECONDS = 180000;
  private static final int RENAME_TIMEOUT_MILISECONDS = 180000;

  private final AbfsHttpClientFactory abfsHttpClientFactory;
  private final AbfsStreamFactory abfsStreamFactory;
  private final ConcurrentHashMap<AzureBlobFileSystem, AbfsHttpClient> abfsHttpClientCache;
  private final ConcurrentHashMap<AzureBlobFileSystem, ThreadPoolExecutor> abfsHttpClientWriteExecutorServiceCache;
  private final ConcurrentHashMap<AzureBlobFileSystem, ThreadPoolExecutor> abfsHttpClientReadExecutorServiceCache;
  private final ConcurrentHashMap<ThreadPoolExecutor, CompletionService> abfsHttpClientCompletionServiceCache;
  private final ConfigurationService configurationService;
  private final TracingService tracingService;
  private final LoggingService loggingService;
  private final Set<String> azureAtomicRenameDirSet;

  @Inject
  AbfsHttpServiceImpl(
      final ConfigurationService configurationService,
      final AbfsHttpClientFactory abfsHttpClientFactory,
      final AbfsStreamFactory abfsStreamFactory,
      final TracingService tracingService,
      final LoggingService loggingService) {
    Preconditions.checkNotNull(abfsHttpClientFactory, "abfsHttpClientFactory");
    Preconditions.checkNotNull(abfsStreamFactory, "abfsStreamFactory");
    Preconditions.checkNotNull(configurationService, "configurationService");
    Preconditions.checkNotNull(tracingService, "tracingService");
    Preconditions.checkNotNull(loggingService, "loggingService");

    this.configurationService = configurationService;
    this.abfsStreamFactory = abfsStreamFactory;
    this.abfsHttpClientCache = new ConcurrentHashMap<>();
    this.abfsHttpClientReadExecutorServiceCache = new ConcurrentHashMap<>();
    this.abfsHttpClientWriteExecutorServiceCache = new ConcurrentHashMap<>();
    this.abfsHttpClientCompletionServiceCache = new ConcurrentHashMap<>();
    this.abfsHttpClientFactory = abfsHttpClientFactory;
    this.tracingService = tracingService;
    this.loggingService = loggingService.get(AbfsHttpService.class);
    this.azureAtomicRenameDirSet = new HashSet<>(Arrays.asList(configurationService.getAzureAtomicRenameDirs().split(",")));
  }

  @Override
  public Hashtable<String, String> getFilesystemProperties(final AzureBlobFileSystem azureBlobFileSystem)
      throws AzureBlobFileSystemException {
    return execute(
        "AbfsHttpServiceImpl.getFilesystemProperties",
        new Callable<Hashtable<String, String>>() {
          @Override
          public Hashtable<String, String> call() throws Exception {
            return getFilesystemPropertiesAsync(azureBlobFileSystem).get();
          }
        });
  }

  @Override
  public Future<Hashtable<String, String>> getFilesystemPropertiesAsync(final AzureBlobFileSystem azureBlobFileSystem) throws
      AzureBlobFileSystemException {
    final AbfsHttpClient abfsHttpClient = this.getOrCreateFileSystemClient(azureBlobFileSystem);

    this.loggingService.debug(
        "getFilesystemPropertiesAsync for filesystem: {0}",
        abfsHttpClient.getSession().getFileSystem());

    final Callable<Hashtable<String, String>> asyncCallable = new Callable<Hashtable<String, String>>() {
      @Override
      public Hashtable<String, String> call() throws Exception {
        String xmsProperties = getFileSystemPropertiesInternal(abfsHttpClient).toBlocking().single().xMsProperties();
        Hashtable<String, String> parsedXmsProperties = parseCommaSeparatedXmsProperties(xmsProperties);

        loggingService.debug(
            "Fetched filesystem properties for fs: {0}, properties: {1}",
            abfsHttpClient.getSession().getFileSystem(),
            parsedXmsProperties);

        return parsedXmsProperties;
      }
    };

    return executeAsync("AbfsHttpServiceImpl.getFilesystemPropertiesAsync", azureBlobFileSystem,
            abfsHttpClient, ThreadPoolExecutorType.READ, asyncCallable);
  }

  @Override
  public void setFilesystemProperties(final AzureBlobFileSystem azureBlobFileSystem, final Hashtable<String, String> properties) throws
      AzureBlobFileSystemException {
    execute("AbfsHttpServiceImpl.setFilesystemProperties",
        new Callable<Void>() {
          @Override
          public Void call() throws Exception {
            return setFilesystemPropertiesAsync(azureBlobFileSystem, properties).get();
          }
        });
  }

  @Override
  public Future<Void> setFilesystemPropertiesAsync(final AzureBlobFileSystem azureBlobFileSystem, final Hashtable<String, String> properties)
      throws
      AzureBlobFileSystemException {
    if (properties == null || properties.size() == 0) {
      return ConcurrentUtils.constantFuture(null);
    }

    final AbfsHttpClient abfsHttpClient = this.getOrCreateFileSystemClient(azureBlobFileSystem);

    this.loggingService.debug(
        "setFilesystemPropertiesAsync for filesystem: {0} with properties: {1}",
        abfsHttpClient.getSession().getFileSystem(),
        properties);

    final Callable<Void> asyncCallable = new Callable<Void>() {
      @Override
      public Void call() throws Exception {
        String commaSeparatedProperties = convertXmsPropertiesToCommaSeparatedString(properties);

        return abfsHttpClient.setFilesystemPropertiesAsync(
            abfsHttpClient.getSession().getFileSystem(),
            FILE_SYSTEM,
            commaSeparatedProperties,
            null /* ifModifiedSince */,
            null /* ifUnmodifiedSince */,
            null /* xMsClientRequestId */,
            FileSystemConfigurations.FS_AZURE_DEFAULT_CONNECTION_TIMEOUT,
            null /* xMsDate */).toBlocking().single();
      }
    };

    return executeAsync("AbfsHttpServiceImpl.setFilesystemPropertiesAsync", azureBlobFileSystem,
            abfsHttpClient, ThreadPoolExecutorType.WRITE, asyncCallable);
  }

  @Override
  public Hashtable<String, String> getPathProperties(final AzureBlobFileSystem azureBlobFileSystem, final Path path) throws
      AzureBlobFileSystemException {
    return execute(
        "AbfsHttpServiceImpl.getPathProperties",
        new Callable<Hashtable<String, String>>() {
          @Override
          public Hashtable<String, String> call() throws Exception {
            return getPathPropertiesAsync(azureBlobFileSystem, path).get();
          }
        });
  }

  @Override
  public Future<Hashtable<String, String>> getPathPropertiesAsync(final AzureBlobFileSystem azureBlobFileSystem, final Path path) throws
      AzureBlobFileSystemException {
    final AbfsHttpClient abfsHttpClient = this.getOrCreateFileSystemClient(azureBlobFileSystem);

    this.loggingService.debug(
        "getPathPropertiesAsync for filesystem: {0} path: {1}",
        abfsHttpClient.getSession().getFileSystem(),
        path.toString());

    final Callable<Hashtable<String, String>> asyncCallable = new Callable<Hashtable<String, String>>() {
      @Override
      public Hashtable<String, String> call() throws Exception {
        String xmsProperties = getPathPropertiesInternal(abfsHttpClient, path).toBlocking().single().xMsProperties();
        Hashtable<String, String> parsedXmsProperties = parseCommaSeparatedXmsProperties(xmsProperties);

        loggingService.debug(
            "getPathPropertiesAsync properties for filesystem: {0} path: {1} properties: {2}",
            abfsHttpClient.getSession().getFileSystem(),
            path.toString(),
            parsedXmsProperties);

        return parsedXmsProperties;
      }
    };

    return executeAsync("AbfsHttpServiceImpl.getPathPropertiesAsync", azureBlobFileSystem,
            abfsHttpClient, ThreadPoolExecutorType.READ, asyncCallable);
  }

  @Override
  public void setPathProperties(final AzureBlobFileSystem azureBlobFileSystem, final Path path, final Hashtable<String,
      String> properties) throws
      AzureBlobFileSystemException {
    execute(
        "AbfsHttpServiceImpl.setPathProperties",
        new Callable<Void>() {
          @Override
          public Void call() throws Exception {
            return setPathPropertiesAsync(azureBlobFileSystem, path, properties).get();
          }
        });
  }

  @Override
  public Future<Void> setPathPropertiesAsync(final AzureBlobFileSystem azureBlobFileSystem, final Path path, final
  Hashtable<String, String> properties) throws
      AzureBlobFileSystemException {
    final AbfsHttpClient abfsHttpClient = this.getOrCreateFileSystemClient(azureBlobFileSystem);

    this.loggingService.debug(
        "setFilesystemPropertiesAsync for filesystem: {0} path: {1} with properties: {2}",
        abfsHttpClient.getSession().getFileSystem(),
        path.toString(),
        properties);

    final Callable<Void> asyncCallable = new Callable<Void>() {
      @Override
      public Void call() throws Exception {
        String commaSeparatedProperties = convertXmsPropertiesToCommaSeparatedString(properties);
        Observable<Void> updatePathAsync = abfsHttpClient.updatePathAsync(
            "setProperties",
            abfsHttpClient.getSession().getFileSystem(),
            getRelativePath(path),
            null /* position */,
            null /* retainUncommitedData */,
            null /* contentLength */,
            null /* xMsLeaseAction */,
            null /* xMsLeaseId */,
            null /* xMsCacheControl */,
            null /* xMsContentType */,
            null /* xMsContentDisposition */,
            null /* xMsContentEncoding */,
            null /* xMsContentLanguage */,
            commaSeparatedProperties,
            null /* ifMatch */,
            null /* ifNoneMatch */,
            null /* ifModifiedSince */,
            null /* ifUnmodifiedSince */,
            null /* requestBody */,
            null /* xMsClientRequestId */,
            FileSystemConfigurations.FS_AZURE_DEFAULT_CONNECTION_TIMEOUT,
            null /* xMsDate */);

        return updatePathAsync.toBlocking().single();
      }
    };

    return executeAsync("AbfsHttpServiceImpl.setPathPropertiesAsync", azureBlobFileSystem,
            abfsHttpClient, ThreadPoolExecutorType.WRITE, asyncCallable);
  }

  @Override
  public void createFilesystem(final AzureBlobFileSystem azureBlobFileSystem) throws AzureBlobFileSystemException {
    execute(
        "AbfsHttpServiceImpl.createFilesystem",
        new Callable<Void>() {
          @Override
          public Void call() throws Exception {
            return createFilesystemAsync(azureBlobFileSystem).get();
          }
        });
  }

  @Override
  public Future<Void> createFilesystemAsync(final AzureBlobFileSystem azureBlobFileSystem) throws AzureBlobFileSystemException {
    final AbfsHttpClient abfsHttpClient = this.getOrCreateFileSystemClient(azureBlobFileSystem);

    this.loggingService.debug(
        "createFilesystemAsync for filesystem: {0}",
        abfsHttpClient.getSession().getFileSystem());

    final Callable<Void> asyncCallable = new Callable<Void>() {
      @Override
      public Void call() throws Exception {
        return abfsHttpClient.createFilesystemAsync(
            abfsHttpClient.getSession().getFileSystem(),
            FILE_SYSTEM,
            null /* xMsProperties */,
            null /* xMsClientRequestId */,
            FileSystemConfigurations.FS_AZURE_DEFAULT_CONNECTION_TIMEOUT,
            null /* xMsDate */).toBlocking().single();
      }
    };

    return executeAsync("AbfsHttpServiceImpl.createFilesystemAsync", azureBlobFileSystem,
            abfsHttpClient, ThreadPoolExecutorType.WRITE, asyncCallable);
  }

  @Override
  public void deleteFilesystem(final AzureBlobFileSystem azureBlobFileSystem) throws AzureBlobFileSystemException {
    execute(
        "AbfsHttpServiceImpl.deleteFilesystem",
        new Callable<Void>() {
          @Override
          public Void call() throws Exception {
            return deleteFilesystemAsync(azureBlobFileSystem).get();
          }
        });
  }

  @Override
  public Future<Void> deleteFilesystemAsync(final AzureBlobFileSystem azureBlobFileSystem) throws AzureBlobFileSystemException {
    final AbfsHttpClient abfsHttpClient = this.getOrCreateFileSystemClient(azureBlobFileSystem);

    this.loggingService.debug(
        "deleteFilesystemAsync for filesystem: {0}",
        abfsHttpClient.getSession().getFileSystem());

    final Callable<Void> asyncCallable = new Callable<Void>() {
      @Override
      public Void call() throws Exception {
        return abfsHttpClient.deleteFilesystemAsync(
            abfsHttpClient.getSession().getFileSystem(), FILE_SYSTEM).toBlocking().single();
      }
    };

    return executeAsync("AbfsHttpServiceImpl.deleteFilesystemAsync", azureBlobFileSystem,
            abfsHttpClient, ThreadPoolExecutorType.WRITE, asyncCallable);
  }

  @Override
  public OutputStream createFile(final AzureBlobFileSystem azureBlobFileSystem, final Path path, final boolean overwrite) throws
      AzureBlobFileSystemException {
    return execute(
        "AbfsHttpServiceImpl.createFile",
        new Callable<OutputStream>() {
          @Override
          public OutputStream call() throws Exception {
            return createFileAsync(azureBlobFileSystem, path, overwrite).get();
          }
        });
  }

  @Override
  public Future<OutputStream> createFileAsync(final AzureBlobFileSystem azureBlobFileSystem, final Path path, final boolean overwrite)
      throws AzureBlobFileSystemException {
    final AbfsHttpClient abfsHttpClient = this.getOrCreateFileSystemClient(azureBlobFileSystem);

    this.loggingService.debug(
        "createFileAsync filesystem: {0} path: {1} overwrite: {2}",
        abfsHttpClient.getSession().getFileSystem(),
        path.toString(),
        overwrite);

    final Callable<OutputStream> asyncCallable = new Callable<OutputStream>() {
      @Override
      public OutputStream call() throws Exception {
        String ifNoneMatch = null;
        if (!overwrite) {
          ifNoneMatch = CONDITIONAL_ALL;
        }

        return abfsHttpClient.createPathAsync(
            abfsHttpClient.getSession().getFileSystem(),
            getRelativePath(path),
            getResource(false),
            null /* continuation */,
            null /* contentLength */,
            null /* cacheControl */,
            null /* contentEncoding */,
            null /* contentLanguage */,
            null /* contentDisposition */,
            null /* xMsCacheControl */,
            null /* xMsContentType */,
            null /* xMsContentEncoding */,
            null /* xMsContentLanguage */,
            null /* xMsContentDisposition */,
            null /* xMsRenameSource */,
            null /* xMsLeaseAction */,
            null /* xMsLeaseId */,
            null /* xMsProposedLeaseId */,
            null /* xMsSourceLeaseId */,
            null /* xMsProperties */,
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
                  return openFileForWriteInternal(azureBlobFileSystem, abfsHttpClient, path, overwrite);
                } catch (AzureBlobFileSystemException ex) {
                  return Observable.error(ex);
                }
              }
            }).toBlocking().single();
      }
    };

    return executeAsync("AbfsHttpServiceImpl.createFileAsync", azureBlobFileSystem,
            abfsHttpClient, ThreadPoolExecutorType.WRITE, asyncCallable);
  }

  @Override
  public Void createDirectory(final AzureBlobFileSystem azureBlobFileSystem, final Path path) throws AzureBlobFileSystemException {
    return execute(
        "AbfsHttpServiceImpl.createDirectory",
        new Callable<Void>() {
          @Override
          public Void call() throws Exception {
            return createDirectoryAsync(azureBlobFileSystem, path).get();
          }
        });
  }

  @Override
  public Future<Void> createDirectoryAsync(final AzureBlobFileSystem azureBlobFileSystem, final Path path) throws
      AzureBlobFileSystemException {
    final AbfsHttpClient abfsHttpClient = this.getOrCreateFileSystemClient(azureBlobFileSystem);

    this.loggingService.debug(
        "createDirectoryAsync filesystem: {0} path: {1}",
        abfsHttpClient.getSession().getFileSystem(),
        path.toString());

    final Callable<Void> asyncCallable = new Callable<Void>() {
      @Override
      public Void call() throws Exception {
        return abfsHttpClient.createPathAsync(
            abfsHttpClient.getSession().getFileSystem(),
            getRelativePath(path),
            getResource(true),
            null /* continuation */,
            "0",
            null /* cacheControl */,
            null /* contentEncoding */,
            null /* contentLanguage */,
            null /* contentDisposition */,
            null /* xMsCacheControl */,
            null /* xMsContentType */,
            null /* xMsContentEncoding */,
            null /* xMsContentLanguage */,
            null /* xMsContentDispoistion */,
            null /* xMsRenameSource */,
            null /* xMsLeaseAction */,
            null /* xMsLeaseId */,
            null /* xMsProposedLeaseId */,
            null /* xMsSourceLeaseId */,
            null /* xMsProperties */,
            null /* ifMatch */,
            CONDITIONAL_ALL,
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

    return executeAsync("AbfsHttpServiceImpl.createDirectoryAsync", azureBlobFileSystem,
            abfsHttpClient, ThreadPoolExecutorType.WRITE, asyncCallable);
  }

  @Override
  public InputStream openFileForRead(final AzureBlobFileSystem azureBlobFileSystem, final Path path) throws AzureBlobFileSystemException {
    return execute(
        "AbfsHttpServiceImpl.openFileForRead",
        new Callable<InputStream>() {
          @Override
          public InputStream call() throws Exception {
            return openFileForReadAsync(azureBlobFileSystem, path).get();
          }
        });
  }

  @Override
  public Future<InputStream> openFileForReadAsync(final AzureBlobFileSystem azureBlobFileSystem, final Path path) throws
      AzureBlobFileSystemException {
    final AbfsHttpClient abfsHttpClient = getOrCreateFileSystemClient(azureBlobFileSystem);

    this.loggingService.debug(
        "openFileForReadAsync filesystem: {0} path: {1}",
        abfsHttpClient.getSession().getFileSystem(),
        path.toString());

    final Callable<InputStream> asyncCallable = new Callable<InputStream>() {
      @Override
      public InputStream call() throws Exception {
        return getFileStatusInternal(azureBlobFileSystem, abfsHttpClient, path)
            .flatMap(new Func1<VersionedFileStatus, Observable<InputStream>>() {
              @Override
              public Observable<InputStream> call(VersionedFileStatus fileStatus) {
                if (fileStatus.isDirectory()) {
                  return Observable.error(new AzureServiceErrorResponseException(
                      AzureServiceErrorCode.PATH_NOT_FOUND.getStatusCode(),
                      AzureServiceErrorCode.PATH_NOT_FOUND.getErrorCode(),
                      "openFileForReadAsync must be used with files and not directories",
                      null));
                }

                return Observable.just(abfsStreamFactory.createReadStream(
                    azureBlobFileSystem,
                    path,
                    fileStatus.getLen(),
                    fileStatus.version));
              }
            }).toBlocking().single();
      }
    };

    return executeAsync("AbfsHttpServiceImpl.openFileForReadAsync", azureBlobFileSystem,
            abfsHttpClient, ThreadPoolExecutorType.READ, asyncCallable);
  }


  @Override
  public OutputStream openFileForWrite(final AzureBlobFileSystem azureBlobFileSystem, final Path path, final boolean overwrite) throws
      AzureBlobFileSystemException {
    return execute(
        "AbfsHttpServiceImpl.openFileForWrite",
        new Callable<OutputStream>() {
          @Override
          public OutputStream call() throws Exception {
            return openFileForWriteAsync(azureBlobFileSystem, path, overwrite).get();
          }
        });
  }

  @Override
  public Future<OutputStream> openFileForWriteAsync(final AzureBlobFileSystem azureBlobFileSystem, final Path path, final boolean overwrite)
      throws AzureBlobFileSystemException {
    final AbfsHttpClient abfsHttpClient = getOrCreateFileSystemClient(azureBlobFileSystem);

    this.loggingService.debug(
        "openFileForWriteAsync filesystem: {0} path: {1} overwrite: {2}",
        abfsHttpClient.getSession().getFileSystem(),
        path.toString(),
        overwrite);

    final Callable<OutputStream> asyncCallable = new Callable<OutputStream>() {
      @Override
      public OutputStream call() throws Exception {
        return openFileForWriteInternal(azureBlobFileSystem, abfsHttpClient, path, overwrite).toBlocking().single();
      }
    };

    return executeAsync("AbfsHttpServiceImpl.openFileForWriteAsync", azureBlobFileSystem,
            abfsHttpClient, ThreadPoolExecutorType.WRITE, asyncCallable);
  }

  @Override
  public Integer readFile(final AzureBlobFileSystem azureBlobFileSystem, final Path path, final String version, final long offset, final int
      length, final ByteBuf readBuffer, final int readBufferOffset) throws
      AzureBlobFileSystemException {
    return execute(
        "AbfsHttpServiceImpl.readFile",
        new Callable<Integer>() {
          @Override
          public Integer call() throws Exception {
            return readFileAsync(azureBlobFileSystem, path, version, offset, length, readBuffer, readBufferOffset).get();
          }
        });
  }

  @Override
  public Future<Integer> readFileAsync(final AzureBlobFileSystem azureBlobFileSystem, final Path path, final String version, final long offset,
      final int length, final ByteBuf targetBuffer, final int targetBufferOffset) throws
      AzureBlobFileSystemException {
    final AbfsHttpClient abfsHttpClient = this.getOrCreateFileSystemClient(azureBlobFileSystem);

    this.loggingService.debug(
        "readFileAsync filesystem: {0} path: {1} offset: {2} length: {3} targetBufferOffset: {4}",
        abfsHttpClient.getSession().getFileSystem(),
        path.toString(),
        offset,
        length,
        targetBufferOffset);

    final Callable<Integer> asyncCallable = new Callable<Integer>() {
      @Override
      public Integer call() throws Exception {
        final boolean tolerateOobAppends = configurationService.getTolerateOobAppends();
        return abfsHttpClient.readPathWithServiceResponseAsync(
            abfsHttpClient.getSession().getFileSystem(),
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

    return executeAsync("AbfsHttpServiceImpl.readFileAsync", azureBlobFileSystem,
            abfsHttpClient, ThreadPoolExecutorType.READ, asyncCallable);
  }

  @Override
  public void writeFile(final AzureBlobFileSystem azureBlobFileSystem, final Path path, final ByteBuf body, final long offset) throws
      AzureBlobFileSystemException {
    execute(
        "AbfsHttpServiceImpl.writeFile",
        new Callable<Void>() {
          @Override
          public Void call() throws Exception {
            return writeFileAsync(azureBlobFileSystem, path, body, offset).get();
          }
        });
  }

  @Override
  public Future<Void> writeFileAsync(final AzureBlobFileSystem azureBlobFileSystem, final Path path, final
  ByteBuf body, final long offset) throws AzureBlobFileSystemException {
    final AbfsHttpClient abfsHttpClient = this.getOrCreateFileSystemClient(azureBlobFileSystem);

    this.loggingService.debug(
        "writeFileAsync filesystem: {0} path: {1} offset: {2}",
        abfsHttpClient.getSession().getFileSystem(),
        path.toString(),
        offset);

    final Callable<Void> asyncCallable = new Callable<Void>() {
      @Override
      public Void call() throws Exception {
        Observable<Void> updatePathAsync = abfsHttpClient.updatePathAsync(
            "append",
            abfsHttpClient.getSession().getFileSystem(),
            getRelativePath(path),
            offset,
            false /* retainUncommittedData */,
            Integer.toString(body.readableBytes()),
            null /* xMsLeaseAction */,
            null /* xMsLeaseId */,
            null /* xMsCacheControl */,
            null /* xMsContentType */,
            null /* xMsContentDisposition */,
            null /* xMsContentEncoding */,
            null /* xMsContentLanguage */,
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

    return executeAsync("AbfsHttpServiceImpl.writeFileAsync", azureBlobFileSystem,
            abfsHttpClient, ThreadPoolExecutorType.WRITE, asyncCallable);
  }

  @Override
  public void flushFile(final AzureBlobFileSystem azureBlobFileSystem, final Path path, final long offset, final boolean retainUncommitedData)
      throws
      AzureBlobFileSystemException {
    execute(
        "AbfsHttpServiceImpl.flushFile",
        new Callable<Void>() {
          @Override
          public Void call() throws Exception {
            return flushFileAsync(azureBlobFileSystem, path, offset, retainUncommitedData).get();
          }
        });
  }

  @Override
  public Future<Void> flushFileAsync(final AzureBlobFileSystem azureBlobFileSystem, final Path path, final long offset, final boolean
      retainUncommitedData) throws AzureBlobFileSystemException {
    final AbfsHttpClient abfsHttpClient = this.getOrCreateFileSystemClient(azureBlobFileSystem);

    this.loggingService.debug(
        "flushFileAsync filesystem: {0} path: {1} offset: {2}",
        abfsHttpClient.getSession().getFileSystem(),
        path.toString(),
        offset);

    final Callable<Void> asyncCallable = new Callable<Void>() {
      @Override
      public Void call() throws Exception {
        return abfsHttpClient.updatePathAsync(
            "flush",
            abfsHttpClient.getSession().getFileSystem(),
            getRelativePath(path),
            offset,
            retainUncommitedData /* retainUncommitedData */,
            null /* contentLength */,
            null /* xMsLeaseAction */,
            null /* xMsLeaseId */,
            null /* xMsCacheControl */,
            null /* xMsContentType */,
            null /* xMsContentDisposition */,
            null /* xMsContentEncoding */,
            null /* xMsContentLanguage */,
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

    return executeAsync("AbfsHttpServiceImpl.flushFileAsync", azureBlobFileSystem,
            abfsHttpClient, ThreadPoolExecutorType.WRITE, asyncCallable);
  }

  @Override
  public void rename(final AzureBlobFileSystem azureBlobFileSystem, final Path source, final Path destination) throws
      AzureBlobFileSystemException {
    execute(
        "AbfsHttpServiceImpl.rename",
        new Callable<Void>() {
          @Override
          public Void call() throws Exception {
            renameAsync(azureBlobFileSystem, source, destination).get();
            return null;
          }
        });
  }

  @Override
  public Future<Void> renameAsync(final AzureBlobFileSystem azureBlobFileSystem, final Path source, final Path destination) throws
      AzureBlobFileSystemException {

    if(isAtomicRenameKey(source.getName())) {
      this.loggingService.warning("The atomic rename feature is not supported by the ABFS scheme; however rename,"
          +" create and delete operations are atomic if Namespace is enabled for your Azure Storage account.");
    }

    final AbfsHttpClient abfsHttpClient = this.getOrCreateFileSystemClient(azureBlobFileSystem);

    this.loggingService.debug(
        "renameAsync filesystem: {0} source: {1} destination: {2}",
        abfsHttpClient.getSession().getFileSystem(),
        source.toString(),
        destination.toString());

    final Callable<Void> asyncCallable = new Callable<Void>() {
      @Override
      public Void call() throws Exception {
        String continuation = null;
        boolean operationPending = true;

        long deadline = now() + RENAME_TIMEOUT_MILISECONDS;
        while (operationPending) {
          if (now() > deadline) {
            loggingService.debug(
                "Rename {0} to {1} timed out.",
                source,
                destination);

            throw new TimeoutException("Rename timed out.");
          }

          String encodedRenameSource = URLEncoder.encode(Path.SEPARATOR + abfsHttpClient.getSession().getFileSystem()
                  + Path.SEPARATOR + getRelativePath(source), StandardCharsets.UTF_8.toString());
          // This is to ensure it matches .NET and Azure Blob SDK's behavior
          encodedRenameSource = encodedRenameSource.replace("+", "%20").replace("%2F", "/");

          continuation = abfsHttpClient.createPathWithServiceResponseAsync(
              abfsHttpClient.getSession().getFileSystem(),
              getRelativePath(destination),
              null /* resource */,
              continuation,
              null /* contentLength */,
              null /* cacheControl */,
              null /* contentEncoding */,
              null /* contentLanguage */,
              null /* contentDisposition */,
              null /* xMsCacheControl */,
              null /* xMsContentType */,
              null /* xMsContentEncoding */,
              null /* xMsContentLanguage */,
              null /* xMsContentDisposition */,
              encodedRenameSource,
              null /* xMsLeaseAction */,
              null /* xMsLeaseId */,
              null /* xMsProposedLeaseId */,
              null /* xMsSourceLeaseId */,
              null /* xMsProperties */,
              null /* ifMatch */,
              CONDITIONAL_ALL,
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

          operationPending = continuation != null && !continuation.isEmpty();
        }

        return null;
      }
    };

    return executeAsync("AbfsHttpServiceImpl.renameAsync", azureBlobFileSystem,
            abfsHttpClient, ThreadPoolExecutorType.WRITE, asyncCallable);
  }

  @Override
  public void delete(final AzureBlobFileSystem azureBlobFileSystem, final Path path, final boolean recursive) throws
      AzureBlobFileSystemException {
    execute(
        "AbfsHttpServiceImpl.delete",
        new Callable<Void>() {
          @Override
          public Void call() throws Exception {
            deleteAsync(azureBlobFileSystem, path, recursive).get();
            return null;
          }
        });
  }

  @Override
  public Future<Void> deleteAsync(final AzureBlobFileSystem azureBlobFileSystem, final Path path, final boolean recursive) throws
      AzureBlobFileSystemException {
    final AbfsHttpClient abfsHttpClient = this.getOrCreateFileSystemClient(azureBlobFileSystem);

    this.loggingService.debug(
        "deleteAsync filesystem: {0} path: {1} recursive: {2}",
        abfsHttpClient.getSession().getFileSystem(),
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

          continuation = abfsHttpClient.deletePathWithServiceResponseAsync(
              abfsHttpClient.getSession().getFileSystem(),
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

          operationPending = continuation != null && !continuation.isEmpty();
        }

        return null;
      }
    };

    return executeAsync("AbfsHttpServiceImpl.deleteAsync", azureBlobFileSystem,
            abfsHttpClient, ThreadPoolExecutorType.WRITE, asyncCallable);
  }

  @Override
  public FileStatus getFileStatus(final AzureBlobFileSystem azureBlobFileSystem, final Path path) throws AzureBlobFileSystemException {
    return execute(
        "AbfsHttpServiceImpl.getFileStatus",
        new Callable<FileStatus>() {
          @Override
          public FileStatus call() throws Exception {
            return getFileStatusAsync(azureBlobFileSystem, path).get();
          }
        });
  }

  @Override
  public Future<FileStatus> getFileStatusAsync(final AzureBlobFileSystem azureBlobFileSystem, final Path path) throws
      AzureBlobFileSystemException {
    final AbfsHttpClient abfsHttpClient = this.getOrCreateFileSystemClient(azureBlobFileSystem);

    this.loggingService.debug(
        "getFileStatusAsync filesystem: {0} path: {1}",
        abfsHttpClient.getSession().getFileSystem(),
        path.toString());

    final Callable<FileStatus> asyncCallable = new Callable<FileStatus>() {
      @Override
      public FileStatus call() throws Exception {
        return getFileStatusInternal(azureBlobFileSystem, abfsHttpClient, path).toBlocking().single();
      }
    };

    return executeAsync("AbfsHttpServiceImpl.getFileStatusAsync", azureBlobFileSystem,
            abfsHttpClient, ThreadPoolExecutorType.READ, asyncCallable);
  }

  @Override
  public FileStatus[] listStatus(final AzureBlobFileSystem azureBlobFileSystem, final Path path) throws AzureBlobFileSystemException {
    return execute(
        "AbfsHttpServiceImpl.listStatus",
        new Callable<FileStatus[]>() {
          @Override
          public FileStatus[] call() throws Exception {
            return listStatusAsync(azureBlobFileSystem, path).get();
          }
        });
  }

  @Override
  public Future<FileStatus[]> listStatusAsync(final AzureBlobFileSystem azureBlobFileSystem, final Path path) throws
      AzureBlobFileSystemException {
    final AbfsHttpClient abfsHttpClient = this.getOrCreateFileSystemClient(azureBlobFileSystem);

    this.loggingService.debug(
        "listStatusAsync filesystem: {0} path: {1}",
        abfsHttpClient.getSession().getFileSystem(),
        path.toString());

    final Callable<FileStatus[]> asyncCallable = new Callable<FileStatus[]>() {
      @Override
      public FileStatus[] call() throws Exception {
        String relativePath = path.isRoot() ? null : getRelativePath(path);
        boolean remaining = true;
        String continuation = null;
        ArrayList<FileStatus> fileStatuses = new ArrayList<>();

        while (remaining) {
          ServiceResponseWithHeaders<ListSchema, ListPathsHeaders> serviceResponseWithHeaders = abfsHttpClient.listPathsWithServiceResponseAsync(
              false,
              abfsHttpClient.getSession().getFileSystem(),
              FILE_SYSTEM,
              relativePath,
              continuation,
              LIST_MAX_RESULTS,
              null /* xMsClientRequestId */,
              FileSystemConfigurations.FS_AZURE_DEFAULT_CONNECTION_TIMEOUT,
              null /* xMsDate */).toBlocking().single();

          ListSchema retrievedSchema = serviceResponseWithHeaders.body();
          if (retrievedSchema == null) {
            throw new AzureServiceErrorResponseException(
                AzureServiceErrorCode.PATH_NOT_FOUND.getStatusCode(),
                AzureServiceErrorCode.PATH_NOT_FOUND.getErrorCode(),
                "listStatusAsync path not found",
                null);
          }

          continuation = serviceResponseWithHeaders.headers().xMsContinuation();
          long blockSize = configurationService.getAzureBlockSize();

          for (ListEntrySchema entry : retrievedSchema.paths()) {
            long lastModifiedMillis = 0;
            long contentLength = entry.contentLength() == null ? 0 : entry.contentLength();
            boolean isDirectory = entry.isDirectory() == null ? false : entry.isDirectory();
            if (entry.lastModified() != null && !entry.lastModified().isEmpty()) {
              final DateTime dateTime = DateTime.parse(
                  entry.lastModified(),
                  DateTimeFormat.forPattern(DATE_TIME_PATTERN).withZoneUTC());
              lastModifiedMillis = dateTime.getMillis();
            }

            fileStatuses.add(
                new VersionedFileStatus(
                    azureBlobFileSystem.getOwnerUser(),
                    azureBlobFileSystem.getOwnerUserPrimaryGroup(),
                    contentLength,
                    isDirectory,
                    1,
                    blockSize,
                    lastModifiedMillis,
                    azureBlobFileSystem.makeQualified(new Path(File.separator + entry.name())),
                    entry.eTag()));
          }

          if (continuation == null || continuation.isEmpty()) {
            remaining = false;
          }
        }

        return fileStatuses.toArray(new FileStatus[0]);
      }
    };

    return executeAsync("AbfsHttpServiceImpl.listStatusAsync", azureBlobFileSystem,
            abfsHttpClient, ThreadPoolExecutorType.READ, asyncCallable);
  }

  @Override
  public synchronized void closeFileSystem(final AzureBlobFileSystem azureBlobFileSystem) throws AzureBlobFileSystemException {
    final AbfsHttpClient abfsHttpClient = this.abfsHttpClientCache.remove(azureBlobFileSystem);

    if (abfsHttpClient != null) {
      this.loggingService.debug(
          "closeFileSystem filesystem: {0}",
          abfsHttpClient.getSession().getFileSystem());

      execute(
          "AbfsHttpServiceImpl.closeFileSystem",
          new Callable<Void>() {
            @Override
            public Void call() throws Exception {
              abfsHttpClient.close();
              return null;
            }
          }
      );
    }

    final ThreadPoolExecutor readThreadPoolExecutor = this.abfsHttpClientReadExecutorServiceCache.get(azureBlobFileSystem);

    if (readThreadPoolExecutor != null) {
      this.abfsHttpClientCompletionServiceCache.remove(readThreadPoolExecutor);
      readThreadPoolExecutor.shutdownNow();
      this.abfsHttpClientReadExecutorServiceCache.remove(azureBlobFileSystem);
    }

    final ThreadPoolExecutor writeThreadPoolExecutor = this.abfsHttpClientWriteExecutorServiceCache.get(azureBlobFileSystem);

    if (writeThreadPoolExecutor != null) {
      this.abfsHttpClientCompletionServiceCache.remove(writeThreadPoolExecutor);
      writeThreadPoolExecutor.shutdownNow();
      this.abfsHttpClientWriteExecutorServiceCache.remove(azureBlobFileSystem);
    }
  }

  @VisibleForTesting
  synchronized boolean threadPoolsAreRunning(final AzureBlobFileSystem azureBlobFileSystem) {
    return this.abfsHttpClientReadExecutorServiceCache.get(azureBlobFileSystem) != null
        || this.abfsHttpClientWriteExecutorServiceCache.get(azureBlobFileSystem) != null;
  }

  @Override
  public boolean isAtomicRenameKey(String key) {
    return isKeyForDirectorySet(key, azureAtomicRenameDirSet);
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

  private synchronized AbfsHttpClient getOrCreateFileSystemClient(final AzureBlobFileSystem azureBlobFileSystem) throws
      AzureBlobFileSystemException {
    Preconditions.checkNotNull(azureBlobFileSystem, "azureBlobFileSystem");

    AbfsHttpClient abfsHttpClient = this.abfsHttpClientCache.get(azureBlobFileSystem);

    if (abfsHttpClient != null) {
      return abfsHttpClient;
    }

    abfsHttpClient = abfsHttpClientFactory.create(azureBlobFileSystem);
    this.abfsHttpClientCache.put(
        azureBlobFileSystem,
        abfsHttpClient);

    return abfsHttpClient;
  }

  private synchronized ThreadPoolExecutor getOrCreateFileSystemClientReadThreadPoolExecutor(
    final AbfsHttpClient abfsHttpClient,
    final AzureBlobFileSystem azureBlobFileSystem) throws AzureBlobFileSystemException {
    return getOrCreateFileSystemClientThreadPoolExecutor(
        abfsHttpClient,
        azureBlobFileSystem,
        ThreadPoolExecutorType.READ);
  }

  private synchronized ThreadPoolExecutor getOrCreateFileSystemClientWriteThreadPoolExecutor(
      final AbfsHttpClient abfsHttpClient,
      final AzureBlobFileSystem azureBlobFileSystem) throws AzureBlobFileSystemException {
    return getOrCreateFileSystemClientThreadPoolExecutor(
        abfsHttpClient,
        azureBlobFileSystem,
        ThreadPoolExecutorType.WRITE);
  }

  private synchronized ThreadPoolExecutor getOrCreateFileSystemClientThreadPoolExecutor(
      final AbfsHttpClient abfsHttpClient,
      final AzureBlobFileSystem azureBlobFileSystem,
      final ThreadPoolExecutorType threadPoolExecutorType) throws
      AzureBlobFileSystemException {
    Preconditions.checkNotNull(abfsHttpClient, "abfsHttpClient");
    Preconditions.checkNotNull(azureBlobFileSystem, "azureBlobFileSystem");

    final boolean isRead = threadPoolExecutorType == ThreadPoolExecutorType.READ;

    ThreadPoolExecutor threadPoolExecutor =
        isRead
            ? this.abfsHttpClientReadExecutorServiceCache.get(azureBlobFileSystem)
            : this.abfsHttpClientWriteExecutorServiceCache.get(azureBlobFileSystem);

    if (threadPoolExecutor != null) {
      return threadPoolExecutor;
    }

    int maxConcurrentThreads =
        isRead
            ? this.configurationService.getMaxConcurrentReadThreads()
            : this.configurationService.getMaxConcurrentWriteThreads();

    this.loggingService.debug(
        "Creating AbfsHttpServiceImpl with " + (isRead ? "read" : "write") + " pool capacity of {0}",
        maxConcurrentThreads);

    threadPoolExecutor = createThreadPoolExecutor((isRead ? "Rex-" : "Wex-") + abfsHttpClient.getSession().getFileSystem(), maxConcurrentThreads);

    final ConcurrentHashMap<AzureBlobFileSystem, ThreadPoolExecutor> cache =
        isRead
            ? this.abfsHttpClientReadExecutorServiceCache
            : this.abfsHttpClientWriteExecutorServiceCache;

    cache.put(azureBlobFileSystem, threadPoolExecutor);
    this.abfsHttpClientCompletionServiceCache.put(threadPoolExecutor, new ExecutorCompletionService(threadPoolExecutor));
    return threadPoolExecutor;
  }

  private <T> T execute(
      final String scopeDescription,
      final Callable<T> callableRestOperation) throws
      AzureBlobFileSystemException {
    return this.execute(scopeDescription, callableRestOperation, null);
  }

  private <T> T execute(
      final String scopeDescription,
      final Callable<T> callableRestOperation,
      @Nullable final SpanId parentTraceId) throws
      AzureServiceErrorResponseException {

    TraceScope traceScope;
    if (parentTraceId == null) {
      traceScope = this.tracingService.traceBegin(scopeDescription);
    } else {
      traceScope = this.tracingService.traceBegin(scopeDescription, parentTraceId);
    }

    try {
      return callableRestOperation.call();
    } catch (Exception exception) {
      final Throwable rootCause = ExceptionUtils.getRootCause(exception);
      AzureServiceErrorResponseException abfsFilesystemException;

      if (exception instanceof AzureServiceErrorResponseException) {
        abfsFilesystemException = (AzureServiceErrorResponseException) exception;
      } else if (rootCause instanceof AzureServiceErrorResponseException) {
        abfsFilesystemException = (AzureServiceErrorResponseException) rootCause;
      } else if (exception instanceof ErrorSchemaException) {
        abfsFilesystemException = parseErrorSchemaException((ErrorSchemaException) exception);
      } else if (rootCause instanceof ErrorSchemaException) {
        abfsFilesystemException = parseErrorSchemaException((ErrorSchemaException) rootCause);
      } else if (exception instanceof ExecutionException) {
        abfsFilesystemException = new InvalidAzureServiceErrorResponseException((Exception) rootCause);
      } else {
        abfsFilesystemException = new InvalidAzureServiceErrorResponseException(
            new AzureServiceNetworkException(scopeDescription, exception));
      }

      tracingService.traceException(traceScope, abfsFilesystemException);
      throw abfsFilesystemException;
    } finally {
      if (parentTraceId == null) {
        tracingService.traceEnd(traceScope);
      }
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
    return resourceType == null ? false : resourceType.equalsIgnoreCase(DIRECTORY);
  }

  private DateTime parseLastModifiedTime(final String lastModifiedTime) {
    return DateTime.parse(
        lastModifiedTime,
        DateTimeFormat.forPattern(DATE_TIME_PATTERN).withZoneUTC());
  }

  private <T> Future<T> executeAsync(
      final String scopeDescription,
      final AzureBlobFileSystem azureBlobFileSystem,
      final AbfsHttpClient abfsHttpClient,
      final ThreadPoolExecutorType threadPoolExecutorType,
      final Callable<T> task) throws AzureBlobFileSystemException {
    if (abfsHttpClient.getSession().getSessionState() != AbfsHttpClientSessionState.OPEN) {
      throw new IllegalStateException("Cannot execute task in a closed session");
    }

    final SpanId currentTraceScopeId = this.tracingService.getCurrentTraceScopeSpanId();


    Callable<T> taskWithObservable = new Callable<T>() {
      @Override
      public T call() throws Exception {
        return execute(scopeDescription, task, currentTraceScopeId);
      }
    };

    // check if need to wait
    int maxTaskSize = (threadPoolExecutorType == ThreadPoolExecutorType.READ
            ? this.configurationService.getMaxConcurrentReadThreads()
            : this.configurationService.getMaxConcurrentWriteThreads());

    final ThreadPoolExecutor threadPoolExecutor = getOrCreateFileSystemClientThreadPoolExecutor(abfsHttpClient, azureBlobFileSystem, threadPoolExecutorType);

    if (threadPoolExecutor.getQueue().size() >= maxTaskSize) {
      this.waitForCompleted(threadPoolExecutor);
    }

    CompletionService completionService = this.abfsHttpClientCompletionServiceCache.get(threadPoolExecutor);

    for (;;) {
      try {
        return completionService.submit(taskWithObservable);
      } catch (RejectedExecutionException ex) {
        // Ignore and retry
      }
    }
  }

  private ThreadPoolExecutor createThreadPoolExecutor(final String threadName, int maxConcurrentThreads) {
    // Do not use new ThreadPoolExecutor.AbortPolicy()
    // It will cause so many threads to be created and not be used.
    // AbortPolicy has some locking mechanisms underneath. Look at
    // ThreadPoolExecutor.java Line 1863.
    ThreadPoolExecutor threadPoolExecutor = new ThreadPoolExecutor(
        maxConcurrentThreads,
        maxConcurrentThreads,
        1L,
        TimeUnit.SECONDS,
        new LinkedBlockingQueue<Runnable>(),
        new ThreadFactoryBuilder().setNameFormat(threadName + "-%d").build(),
        new RejectedExecutionHandler() {
          @Override
          public void rejectedExecution(Runnable r, ThreadPoolExecutor executor) {
            throw new RejectedExecutionException("Task " + r.toString() + " rejected from: " + threadName);
          }
        });

    threadPoolExecutor.prestartCoreThread();
    threadPoolExecutor.prestartAllCoreThreads();

    return threadPoolExecutor;
  }

  // Internal calls are provided to prevent deadlocks in the thread executor pool
  private Observable<GetFilesystemPropertiesHeaders> getFileSystemPropertiesInternal(final AbfsHttpClient abfsHttpClient) {
    Observable<ServiceResponseWithHeaders<Void, GetFilesystemPropertiesHeaders>> response =
        abfsHttpClient.getFilesystemPropertiesWithServiceResponseAsync(
            abfsHttpClient.getSession().getFileSystem(),
            FILE_SYSTEM);

    return response.flatMap(new Func1<ServiceResponseWithHeaders<Void, GetFilesystemPropertiesHeaders>, Observable<GetFilesystemPropertiesHeaders>>() {
      @Override
      public Observable<GetFilesystemPropertiesHeaders> call(ServiceResponseWithHeaders<Void, GetFilesystemPropertiesHeaders>
          voidGetFilesystemPropertiesHeadersServiceResponseWithHeaders) {
        try {
          GetFilesystemPropertiesHeaders headers = voidGetFilesystemPropertiesHeadersServiceResponseWithHeaders.headers();
          return Observable.just(headers);
        } catch (Exception ex) {
          return Observable.error(ex);
        }
      }
    });
  }

  // Internal calls are provided to prevent deadlocks in the thread executor pool
  private Observable<GetPathPropertiesHeaders> getPathPropertiesInternal(final AbfsHttpClient abfsHttpClient, final Path path) {
    final String relativePath = getRelativePath(path);
    Observable<ServiceResponseWithHeaders<Void, GetPathPropertiesHeaders>> response =
        abfsHttpClient.getPathPropertiesWithServiceResponseAsync(
            abfsHttpClient.getSession().getFileSystem(),
            relativePath);

    return response.flatMap(new Func1<ServiceResponseWithHeaders<Void, GetPathPropertiesHeaders>, Observable<GetPathPropertiesHeaders>>() {
      @Override
      public Observable<GetPathPropertiesHeaders> call(ServiceResponseWithHeaders<Void, GetPathPropertiesHeaders>
          voidGetPathPropertiesHeadersServiceResponseWithHeaders) {
        try {
          GetPathPropertiesHeaders headers = voidGetPathPropertiesHeadersServiceResponseWithHeaders.headers();
          return Observable.just(headers);
        } catch (Exception ex) {
          return Observable.error(ex);
        }
      }
    });
  }

  // Internal calls are provided to prevent deadlocks in the thread executor pool
  private Observable<OutputStream> openFileForWriteInternal(
      final AzureBlobFileSystem azureBlobFileSystem,
      final AbfsHttpClient abfsHttpClient, final Path path, final boolean overwrite) throws AzureBlobFileSystemException {
    return getFileStatusInternal(azureBlobFileSystem, abfsHttpClient, path).flatMap(new Func1<FileStatus, Observable<OutputStream>>() {
      @Override
      public Observable<OutputStream> call(FileStatus fileStatus) {
        if (fileStatus.isDirectory()) {
          return Observable.error(new AzureServiceErrorResponseException(
              AzureServiceErrorCode.PATH_NOT_FOUND.getStatusCode(),
              AzureServiceErrorCode.PATH_NOT_FOUND.getErrorCode(),
              "openFileForWriteAsync must be used with files and not directories",
              null));
        }

        long offset = fileStatus.getLen();

        if (overwrite) {
          offset = 0;
        }

        return Observable.just(abfsStreamFactory.createWriteStream(
            azureBlobFileSystem,
            path,
            offset));
      }
    });
  }

  // Internal calls are provided to prevent deadlocks in the thread executor pool
  private Observable<VersionedFileStatus> getFileStatusInternal(
      final AzureBlobFileSystem azureBlobFileSystem,
      final AbfsHttpClient abfsHttpClient,
      final Path path)
      throws AzureBlobFileSystemException {
    if (path.isRoot()) {
      return getFileSystemPropertiesInternal(abfsHttpClient).flatMap(new Func1<GetFilesystemPropertiesHeaders, Observable<VersionedFileStatus>>() {
        @Override
        public Observable<VersionedFileStatus> call(GetFilesystemPropertiesHeaders getFilesystemPropertiesHeaders) {
          return Observable.just(getFileStatusFromFilesystemPropertiesHeaders(azureBlobFileSystem, path, getFilesystemPropertiesHeaders));
        }
      });
    }

    return getPathPropertiesInternal(abfsHttpClient, path).flatMap(new Func1<GetPathPropertiesHeaders, Observable<VersionedFileStatus>>() {
      @Override
      public Observable<VersionedFileStatus> call(GetPathPropertiesHeaders getPathPropertiesHeaders) {
        return Observable.just(getFileStatusFromPathPropertiesHeaders(azureBlobFileSystem, path, getPathPropertiesHeaders));
      }
    });
  }

  private VersionedFileStatus getFileStatusFromFilesystemPropertiesHeaders(
      final AzureBlobFileSystem azureBlobFileSystem,
      final Path path,
      final GetFilesystemPropertiesHeaders getFilesystemPropertiesHeaders) {
    final long blockSize = configurationService.getAzureBlockSize();
    return new VersionedFileStatus(
        azureBlobFileSystem.getOwnerUser(),
        azureBlobFileSystem.getOwnerUserPrimaryGroup(),
        0,
        true,
        1,
        blockSize,
        parseLastModifiedTime(getFilesystemPropertiesHeaders.lastModified()).getMillis(),
        path,
        getFilesystemPropertiesHeaders.eTag());
  }

  private VersionedFileStatus getFileStatusFromPathPropertiesHeaders(
      final AzureBlobFileSystem azureBlobFileSystem,
      final Path path,
      final GetPathPropertiesHeaders getPathPropertiesHeaders) {
    final long blockSize = configurationService.getAzureBlockSize();
    return new VersionedFileStatus(
        azureBlobFileSystem.getOwnerUser(),
        azureBlobFileSystem.getOwnerUserPrimaryGroup(),
        parseContentLength(getPathPropertiesHeaders.contentLength()),
        parseIsDirectory(getPathPropertiesHeaders.xMsResourceType()),
        1,
        blockSize,
        parseLastModifiedTime(getPathPropertiesHeaders.lastModified()).getMillis(),
        path,
        getPathPropertiesHeaders.eTag());
  }

  private String convertXmsPropertiesToCommaSeparatedString(final Hashtable<String, String> properties) throws UnsupportedEncodingException,
      CharacterCodingException {
    String commaSeparatedProperties = "";
    Set<String> keys = properties.keySet();
    Iterator<String> itr = keys.iterator();

    final CharsetEncoder encoder = Charset.forName(XMS_PROPERTIES_ENCODING).newEncoder();

    while (itr.hasNext()) {
      String key = itr.next();
      String value = properties.get(key);

      Boolean canEncodeValue = encoder.canEncode(value);
      if (!canEncodeValue) {
        throw new CharacterCodingException();
      }

      String encodedPropertyValue = DatatypeConverter.printBase64Binary(encoder.encode(CharBuffer.wrap(value)).array());
      commaSeparatedProperties += key + "=" + encodedPropertyValue;

      if (itr.hasNext()) {
        commaSeparatedProperties += ",";
      }
    }

    return commaSeparatedProperties;
  }

  private Hashtable<String, String> parseCommaSeparatedXmsProperties(String xMsProperties) throws
      InvalidFileSystemPropertyException, UnsupportedEncodingException, CharacterCodingException {
    Hashtable<String, String> properties = new Hashtable<>();

    final CharsetDecoder decoder = Charset.forName(XMS_PROPERTIES_ENCODING).newDecoder();

    if (xMsProperties != null && !xMsProperties.isEmpty()) {
      String[] userProperties = xMsProperties.split(",");

      if (userProperties.length == 0) {
        return properties;
      }

      for (String property : userProperties) {
        if (property.isEmpty()) {
          throw new InvalidFileSystemPropertyException(xMsProperties);
        }

        String[] nameValue = property.split("=", 2);
        if (nameValue.length != 2) {
          throw new InvalidFileSystemPropertyException(xMsProperties);
        }

        byte[] decodedValue = DatatypeConverter.parseBase64Binary(nameValue[1]);
        String value = decoder.decode(ByteBuffer.wrap(decodedValue)).toString();
        properties.put(nameValue[0], value);
      }
    }

    return properties;
  }

  private String getRangeHeader(long offset, long length) {
    return "bytes=" + offset + "-" + (offset + length - 1);
  }

  private boolean isKeyForDirectorySet(String key, Set<String> dirSet) {
    for (String dir : dirSet) {
      if (dir.isEmpty() || key.startsWith(dir + "/")) {
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

  private void waitForCompleted(ThreadPoolExecutor threadPoolExecutor) throws AzureBlobFileSystemException{
    CompletionService completionService = abfsHttpClientCompletionServiceCache.get(threadPoolExecutor);
    boolean completed;
    for(completed = false; completionService.poll() != null; completed = true) {}

    if (!completed) {
      try {
        completionService.take();
      } catch (InterruptedException e) {
        throw new FileSystemOperationUnhandledException(e);
      }
    }
  }

  private class VersionedFileStatus extends FileStatus {
    private final String version;

    VersionedFileStatus(
        final String owner, final String group,
        final long length, final boolean isdir, final int blockReplication,
        final long blocksize, final long modificationTime, final Path path,
        String version) {
      super(length, isdir, blockReplication, blocksize, modificationTime, 0,
          new FsPermission(FsAction.ALL, FsAction.ALL, FsAction.ALL),
          owner,
          group,
          path);

      this.version = version;
    }
  }

  private enum ThreadPoolExecutorType {
    READ,
    WRITE
  }
}