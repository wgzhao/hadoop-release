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

import javax.xml.bind.DatatypeConverter;
import java.io.File;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CharsetEncoder;
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
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.microsoft.azure.dfs.rest.client.generated.models.CreatePathHeaders;
import com.microsoft.azure.dfs.rest.client.generated.models.DeletePathHeaders;
import com.microsoft.azure.dfs.rest.client.generated.models.ErrorSchemaException;
import com.microsoft.azure.dfs.rest.client.generated.models.GetFilesystemPropertiesHeaders;
import com.microsoft.azure.dfs.rest.client.generated.models.GetPathPropertiesHeaders;
import com.microsoft.azure.dfs.rest.client.generated.models.ListEntrySchema;
import com.microsoft.azure.dfs.rest.client.generated.models.ListPathsHeaders;
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
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
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
  private static final String CONDITIONAL_ALL = "*";
  private static final String XMS_PROPERTIES_ENCODING = "ISO-8859-1";
  private static final int LIST_MAX_RESULTS = 5000;
  private static final int DELETE_DIRECTORY_TIMEOUT_MILISECONDS = 180000;
  private static final int RENAME_TIMEOUT_MILISECONDS = 180000;

  private final AdfsHttpClientFactory adfsHttpClientFactory;
  private final AdfsStreamFactory adfsStreamFactory;
  private final ConcurrentHashMap<AzureDistributedFileSystem, AdfsHttpClient> adfsHttpClientCache;
  private final ConcurrentHashMap<AzureDistributedFileSystem, ThreadPoolExecutor> adfsHttpClientWriteExecutorServiceCache;
  private final ConcurrentHashMap<AzureDistributedFileSystem, ThreadPoolExecutor> adfsHttpClientReadExecutorServiceCache;
  private final ConfigurationService configurationService;
  private final TracingService tracingService;
  private final LoggingService loggingService;
  private final Set<String> azureAtomicRenameDirSet;

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
    this.adfsHttpClientReadExecutorServiceCache = new ConcurrentHashMap<>();
    this.adfsHttpClientWriteExecutorServiceCache = new ConcurrentHashMap<>();
    this.adfsHttpClientFactory = adfsHttpClientFactory;
    this.tracingService = tracingService;
    this.loggingService = loggingService.get(AdfsHttpService.class);
    this.azureAtomicRenameDirSet = new HashSet<>(Arrays.asList(configurationService.getAzureAtomicRenameDirs().split(",")));
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
    final AdfsHttpClient adfsHttpClient = this.getOrCreateFileSystemClient(azureDistributedFileSystem);
    final ThreadPoolExecutor readExecutorService = this.getOrCreateFileSystemClientReadThreadPoolExecutor(adfsHttpClient, azureDistributedFileSystem);

    this.loggingService.debug(
        "getFilesystemPropertiesAsync for filesystem: {0}",
        adfsHttpClient.getSession().getFileSystem());

    final Callable<Hashtable<String, String>> asyncCallable = new Callable<Hashtable<String, String>>() {
      @Override
      public Hashtable<String, String> call() throws Exception {
        String xmsProperties = getFileSystemPropertiesInternal(adfsHttpClient).toBlocking().single().xMsProperties();
        Hashtable<String, String> parsedXmsProperties = parseCommaSeparatedXmsProperties(xmsProperties);

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

    final AdfsHttpClient adfsHttpClient = this.getOrCreateFileSystemClient(azureDistributedFileSystem);
    final ThreadPoolExecutor writeExecutorService = this.getOrCreateFileSystemClientWriteThreadPoolExecutor(adfsHttpClient, azureDistributedFileSystem);

    this.loggingService.debug(
        "setFilesystemPropertiesAsync for filesystem: {0} with properties: {1}",
        adfsHttpClient.getSession().getFileSystem(),
        properties);

    final Callable<Void> asyncCallable = new Callable<Void>() {
      @Override
      public Void call() throws Exception {
        String commaSeparatedProperties = convertXmsPropertiesToCommaSeparatedString(properties);

        return adfsHttpClient.setFilesystemPropertiesAsync(
            adfsHttpClient.getSession().getFileSystem(),
            FILE_SYSTEM,
            commaSeparatedProperties,
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
    final AdfsHttpClient adfsHttpClient = this.getOrCreateFileSystemClient(azureDistributedFileSystem);
    final ThreadPoolExecutor readExecutorService = this.getOrCreateFileSystemClientReadThreadPoolExecutor(adfsHttpClient, azureDistributedFileSystem);

    this.loggingService.debug(
        "getPathPropertiesAsync for filesystem: {0} path: {1}",
        adfsHttpClient.getSession().getFileSystem(),
        path.toString());

    final Callable<Hashtable<String, String>> asyncCallable = new Callable<Hashtable<String, String>>() {
      @Override
      public Hashtable<String, String> call() throws Exception {
        String xmsProperties = getPathPropertiesInternal(adfsHttpClient, path).toBlocking().single().xMsProperties();
        Hashtable<String, String> parsedXmsProperties = parseCommaSeparatedXmsProperties(xmsProperties);

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
  public void setPathProperties(final AzureDistributedFileSystem azureDistributedFileSystem, final Path path, final Hashtable<String,
      String> properties) throws
      AzureDistributedFileSystemException {
    execute(
        "AdfsHttpServiceImpl.setPathProperties",
        new Callable<Void>() {
          @Override
          public Void call() throws Exception {
            return setPathPropertiesAsync(azureDistributedFileSystem, path, properties).get();
          }
        });
  }

  @Override
  public Future<Void> setPathPropertiesAsync(final AzureDistributedFileSystem azureDistributedFileSystem, final Path path, final
  Hashtable<String, String> properties) throws
      AzureDistributedFileSystemException {
    final AdfsHttpClient adfsHttpClient = this.getOrCreateFileSystemClient(azureDistributedFileSystem);
    final ThreadPoolExecutor writeExecutorService = this.getOrCreateFileSystemClientWriteThreadPoolExecutor(adfsHttpClient, azureDistributedFileSystem);

    this.loggingService.debug(
        "setFilesystemPropertiesAsync for filesystem: {0} path: {1} with properties: {2}",
        adfsHttpClient.getSession().getFileSystem(),
        path.toString(),
        properties);

    final Callable<Void> asyncCallable = new Callable<Void>() {
      @Override
      public Void call() throws Exception {
        String commaSeparatedProperties = convertXmsPropertiesToCommaSeparatedString(properties);
        Observable<Void> updatePathAsync = adfsHttpClient.updatePathAsync(
            "setProperties",
            adfsHttpClient.getSession().getFileSystem(),
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

    return executeAsync("AdfsHttpServiceImpl.setPathPropertiesAsync", adfsHttpClient, writeExecutorService, asyncCallable);
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
    final AdfsHttpClient adfsHttpClient = this.getOrCreateFileSystemClient(azureDistributedFileSystem);
    final ThreadPoolExecutor writeExecutorService = this.getOrCreateFileSystemClientWriteThreadPoolExecutor(adfsHttpClient, azureDistributedFileSystem);

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
    final AdfsHttpClient adfsHttpClient = this.getOrCreateFileSystemClient(azureDistributedFileSystem);
    final ThreadPoolExecutor writeExecutorService = this.getOrCreateFileSystemClientWriteThreadPoolExecutor(adfsHttpClient, azureDistributedFileSystem);

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
    final AdfsHttpClient adfsHttpClient = this.getOrCreateFileSystemClient(azureDistributedFileSystem);
    final ThreadPoolExecutor writeExecutorService = this.getOrCreateFileSystemClientWriteThreadPoolExecutor(adfsHttpClient, azureDistributedFileSystem);

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
            adfsHttpClient.getSession().getFileSystem(),
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
    final AdfsHttpClient adfsHttpClient = this.getOrCreateFileSystemClient(azureDistributedFileSystem);
    final ThreadPoolExecutor writeExecutorService = this.getOrCreateFileSystemClientWriteThreadPoolExecutor(adfsHttpClient, azureDistributedFileSystem);

    this.loggingService.debug(
        "createDirectoryAsync filesystem: {0} path: {1}",
        adfsHttpClient.getSession().getFileSystem(),
        path.toString());

    final Callable<Void> asyncCallable = new Callable<Void>() {
      @Override
      public Void call() throws Exception {
        return adfsHttpClient.createPathAsync(
            adfsHttpClient.getSession().getFileSystem(),
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
    final AdfsHttpClient adfsHttpClient = getOrCreateFileSystemClient(azureDistributedFileSystem);
    final ThreadPoolExecutor readExecutorService = this.getOrCreateFileSystemClientReadThreadPoolExecutor(adfsHttpClient, azureDistributedFileSystem);

    this.loggingService.debug(
        "openFileForReadAsync filesystem: {0} path: {1}",
        adfsHttpClient.getSession().getFileSystem(),
        path.toString());

    final Callable<InputStream> asyncCallable = new Callable<InputStream>() {
      @Override
      public InputStream call() throws Exception {
        return getFileStatusInternal(azureDistributedFileSystem, adfsHttpClient, path)
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
    final AdfsHttpClient adfsHttpClient = getOrCreateFileSystemClient(azureDistributedFileSystem);
    final ThreadPoolExecutor writeExecutorService = this.getOrCreateFileSystemClientWriteThreadPoolExecutor(adfsHttpClient, azureDistributedFileSystem);

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

    return executeAsync("AdfsHttpServiceImpl.openFileForWriteAsync", adfsHttpClient, writeExecutorService, asyncCallable);
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
    final AdfsHttpClient adfsHttpClient = this.getOrCreateFileSystemClient(azureDistributedFileSystem);
    final ThreadPoolExecutor readExecutorService = this.getOrCreateFileSystemClientReadThreadPoolExecutor(adfsHttpClient, azureDistributedFileSystem);

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
    final AdfsHttpClient adfsHttpClient = this.getOrCreateFileSystemClient(azureDistributedFileSystem);
    final ThreadPoolExecutor writeExecutorService = this.getOrCreateFileSystemClientWriteThreadPoolExecutor(adfsHttpClient, azureDistributedFileSystem);

    this.loggingService.debug(
        "writeFileAsync filesystem: {0} path: {1} offset: {2}",
        adfsHttpClient.getSession().getFileSystem(),
        path.toString(),
        offset);

    final Callable<Void> asyncCallable = new Callable<Void>() {
      @Override
      public Void call() throws Exception {
        Observable<Void> updatePathAsync = adfsHttpClient.updatePathAsync(
            "append",
            adfsHttpClient.getSession().getFileSystem(),
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
    final AdfsHttpClient adfsHttpClient = this.getOrCreateFileSystemClient(azureDistributedFileSystem);
    final ThreadPoolExecutor writeExecutorService = this.getOrCreateFileSystemClientWriteThreadPoolExecutor(adfsHttpClient, azureDistributedFileSystem);

    this.loggingService.debug(
        "flushFileAsync filesystem: {0} path: {1} offset: {2}",
        adfsHttpClient.getSession().getFileSystem(),
        path.toString(),
        offset);

    final Callable<Void> asyncCallable = new Callable<Void>() {
      @Override
      public Void call() throws Exception {
        return adfsHttpClient.updatePathAsync(
            "flush",
            adfsHttpClient.getSession().getFileSystem(),
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

    return executeAsync("AdfsHttpServiceImpl.flushFileAsync", adfsHttpClient, writeExecutorService, asyncCallable);
  }

  @Override
  public void rename(final AzureDistributedFileSystem azureDistributedFileSystem, final Path source, final Path destination) throws
      AzureDistributedFileSystemException {
    execute(
        "AdfsHttpServiceImpl.rename",
        new Callable<Void>() {
          @Override
          public Void call() throws Exception {
            renameAsync(azureDistributedFileSystem, source, destination).get();
            return null;
          }
        });
  }

  @Override
  public Future<Void> renameAsync(final AzureDistributedFileSystem azureDistributedFileSystem, final Path source, final Path destination) throws
      AzureDistributedFileSystemException {

    if(isAtomicRenameKey(source.getName())) {
      this.loggingService.warning("The atomic rename feature is not supported by the ADFS scheme; however rename,"
          +" create and delete operations are atomic if Namespace is enabled for your Azure Storage account.");
    }

    final AdfsHttpClient adfsHttpClient = this.getOrCreateFileSystemClient(azureDistributedFileSystem);
    final ThreadPoolExecutor writeExecutorService = this.getOrCreateFileSystemClientWriteThreadPoolExecutor(adfsHttpClient, azureDistributedFileSystem);

    this.loggingService.debug(
        "renameAsync filesystem: {0} source: {1} destination: {2}",
        adfsHttpClient.getSession().getFileSystem(),
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

          continuation = adfsHttpClient.createPathWithServiceResponseAsync(
              adfsHttpClient.getSession().getFileSystem(),
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
              Path.SEPARATOR + adfsHttpClient.getSession().getFileSystem() + Path.SEPARATOR + getRelativePath(source),
              null /* xMsLeaseAction */,
              null /* xMsLeaseId */,
              null /* xMsProposedLeaseId */,
              null /* xMsSourceLeaseId */,
              null /* xMsProperties */,
              null /* ifMatch */,
              null /* ifNonMatch */,
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
    final AdfsHttpClient adfsHttpClient = this.getOrCreateFileSystemClient(azureDistributedFileSystem);
    final ThreadPoolExecutor writeExecutorService = this.getOrCreateFileSystemClientWriteThreadPoolExecutor(adfsHttpClient, azureDistributedFileSystem);

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
    final AdfsHttpClient adfsHttpClient = this.getOrCreateFileSystemClient(azureDistributedFileSystem);
    final ThreadPoolExecutor writeExecutorService = this.getOrCreateFileSystemClientWriteThreadPoolExecutor(adfsHttpClient, azureDistributedFileSystem);

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
              getResource(true),
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

          operationPending = continuation != null && !continuation.isEmpty();
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
    final AdfsHttpClient adfsHttpClient = this.getOrCreateFileSystemClient(azureDistributedFileSystem);
    final ThreadPoolExecutor readExecutorService = this.getOrCreateFileSystemClientReadThreadPoolExecutor(adfsHttpClient, azureDistributedFileSystem);

    this.loggingService.debug(
        "getFileStatusAsync filesystem: {0} path: {1}",
        adfsHttpClient.getSession().getFileSystem(),
        path.toString());

    final Callable<FileStatus> asyncCallable = new Callable<FileStatus>() {
      @Override
      public FileStatus call() throws Exception {
        return getFileStatusInternal(azureDistributedFileSystem, adfsHttpClient, path).toBlocking().single();
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
    final AdfsHttpClient adfsHttpClient = this.getOrCreateFileSystemClient(azureDistributedFileSystem);
    final ThreadPoolExecutor readExecutorService = this.getOrCreateFileSystemClientReadThreadPoolExecutor(adfsHttpClient, azureDistributedFileSystem);

    this.loggingService.debug(
        "listStatusAsync filesystem: {0} path: {1}",
        adfsHttpClient.getSession().getFileSystem(),
        path.toString());

    final Callable<FileStatus[]> asyncCallable = new Callable<FileStatus[]>() {
      @Override
      public FileStatus[] call() throws Exception {
        String relativePath = path.isRoot() ? null : getRelativePath(path);
        boolean remaining = true;
        String continuation = null;
        ArrayList<FileStatus> fileStatuses = new ArrayList<>();

        while (remaining) {
          ServiceResponseWithHeaders<ListSchema, ListPathsHeaders> serviceResponseWithHeaders = adfsHttpClient.listPathsWithServiceResponseAsync(
              false,
              adfsHttpClient.getSession().getFileSystem(),
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
                    azureDistributedFileSystem.getOwnerUser(),
                    azureDistributedFileSystem.getOwnerUserPrimaryGroup(),
                    contentLength,
                    isDirectory,
                    1,
                    blockSize,
                    lastModifiedMillis,
                    azureDistributedFileSystem.makeQualified(new Path(File.separator + entry.name())),
                    entry.eTag()));
          }

          if (continuation == null || continuation.isEmpty()) {
            remaining = false;
          }
        }

        return fileStatuses.toArray(new FileStatus[0]);
      }
    };

    return executeAsync("AdfsHttpServiceImpl.listStatusAsync", adfsHttpClient, readExecutorService, asyncCallable);
  }

  @Override
  public synchronized void closeFileSystem(final AzureDistributedFileSystem azureDistributedFileSystem) throws AzureDistributedFileSystemException {
    final AdfsHttpClient adfsHttpClient = this.adfsHttpClientCache.remove(azureDistributedFileSystem);

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

    final ThreadPoolExecutor readThreadPoolExecutor = this.adfsHttpClientReadExecutorServiceCache.get(azureDistributedFileSystem);

    if (readThreadPoolExecutor != null) {
      readThreadPoolExecutor.shutdownNow();
      this.adfsHttpClientReadExecutorServiceCache.remove(azureDistributedFileSystem);
    }

    final ThreadPoolExecutor writeThreadPoolExecutor = this.adfsHttpClientWriteExecutorServiceCache.get(azureDistributedFileSystem);

    if (writeThreadPoolExecutor != null) {
      writeThreadPoolExecutor.shutdownNow();
      this.adfsHttpClientWriteExecutorServiceCache.remove(azureDistributedFileSystem);
    }
  }

  @VisibleForTesting
  synchronized boolean threadPoolsAreRunning(final AzureDistributedFileSystem azureDistributedFileSystem) {
    return this.adfsHttpClientReadExecutorServiceCache.get(azureDistributedFileSystem) != null
        || this.adfsHttpClientWriteExecutorServiceCache.get(azureDistributedFileSystem) != null;
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

  private synchronized AdfsHttpClient getOrCreateFileSystemClient(final AzureDistributedFileSystem azureDistributedFileSystem) throws
      AzureDistributedFileSystemException {
    Preconditions.checkNotNull(azureDistributedFileSystem, "azureDistributedFileSystem");

    AdfsHttpClient adfsHttpClient = this.adfsHttpClientCache.get(azureDistributedFileSystem);

    if (adfsHttpClient != null) {
      return adfsHttpClient;
    }

    adfsHttpClient = adfsHttpClientFactory.create(azureDistributedFileSystem);
    this.adfsHttpClientCache.put(
        azureDistributedFileSystem,
        adfsHttpClient);

    return adfsHttpClient;
  }

  private synchronized ThreadPoolExecutor getOrCreateFileSystemClientReadThreadPoolExecutor(
    final AdfsHttpClient adfsHttpClient,
    final AzureDistributedFileSystem azureDistributedFileSystem) throws AzureDistributedFileSystemException {
    return getOrCreateFileSystemClientThreadPoolExecutor(
        adfsHttpClient,
        azureDistributedFileSystem,
        ThreadPoolExecutorType.READ);
  }

  private synchronized ThreadPoolExecutor getOrCreateFileSystemClientWriteThreadPoolExecutor(
      final AdfsHttpClient adfsHttpClient,
      final AzureDistributedFileSystem azureDistributedFileSystem) throws AzureDistributedFileSystemException {
    return getOrCreateFileSystemClientThreadPoolExecutor(
        adfsHttpClient,
        azureDistributedFileSystem,
        ThreadPoolExecutorType.WRITE);
  }

  private synchronized ThreadPoolExecutor getOrCreateFileSystemClientThreadPoolExecutor(
      final AdfsHttpClient adfsHttpClient,
      final AzureDistributedFileSystem azureDistributedFileSystem,
      final ThreadPoolExecutorType threadPoolExecutorType) throws
      AzureDistributedFileSystemException {
    Preconditions.checkNotNull(adfsHttpClient, "adfsHttpClient");
    Preconditions.checkNotNull(azureDistributedFileSystem, "azureDistributedFileSystem");

    final boolean isRead = threadPoolExecutorType == ThreadPoolExecutorType.READ;

    ThreadPoolExecutor threadPoolExecutor =
        isRead
            ? this.adfsHttpClientReadExecutorServiceCache.get(azureDistributedFileSystem)
            : this.adfsHttpClientWriteExecutorServiceCache.get(azureDistributedFileSystem);

    if (threadPoolExecutor != null) {
      return threadPoolExecutor;
    }

    int maxConcurrentThreads =
        isRead
            ? this.configurationService.getMaxConcurrentReadThreads()
            : this.configurationService.getMaxConcurrentWriteThreads();

    this.loggingService.debug(
        "Creating AdfsHttpServiceImpl with " + (isRead ? "read" : "write") + " pool capacity of {0}",
        maxConcurrentThreads);

    threadPoolExecutor = createThreadPoolExecutor((isRead ? "Rex-" : "Wex-") + adfsHttpClient.getSession().getFileSystem(), maxConcurrentThreads);

    final ConcurrentHashMap<AzureDistributedFileSystem, ThreadPoolExecutor> cache =
        isRead
            ? this.adfsHttpClientReadExecutorServiceCache
            : this.adfsHttpClientWriteExecutorServiceCache;

    cache.put(azureDistributedFileSystem, threadPoolExecutor);
    return threadPoolExecutor;
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
      AzureServiceErrorResponseException adfsFilesystemException;

      if (exception instanceof AzureServiceErrorResponseException) {
        adfsFilesystemException = (AzureServiceErrorResponseException) exception;
      } else if (rootCause instanceof AzureServiceErrorResponseException) {
        adfsFilesystemException = (AzureServiceErrorResponseException) rootCause;
      } else if (exception instanceof ErrorSchemaException) {
        adfsFilesystemException = parseErrorSchemaException((ErrorSchemaException) exception);
      } else if (rootCause instanceof ErrorSchemaException) {
        adfsFilesystemException = parseErrorSchemaException((ErrorSchemaException) rootCause);
      } else if (exception instanceof ExecutionException) {
        adfsFilesystemException = new InvalidAzureServiceErrorResponseException((Exception) rootCause);
      } else {
        adfsFilesystemException = new InvalidAzureServiceErrorResponseException(
            new AzureServiceNetworkException(scopeDescription, exception));
      }

      tracingService.traceException(traceScope, adfsFilesystemException);
      throw adfsFilesystemException;
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
      final AdfsHttpClient adfsHttpClient,
      final ThreadPoolExecutor threadPoolExecutor,
      final Callable<T> task) {
    if (adfsHttpClient.getSession().getSessionState() != AdfsHttpClientSessionState.OPEN) {
      throw new IllegalStateException("Cannot execute task in a closed session");
    }

    final SpanId currentTraceScopeId = this.tracingService.getCurrentTraceScopeSpanId();

    for (;;) {
      try {
        Callable<T> taskWithObservable = new Callable<T>() {
          @Override
          public T call() throws Exception {
            return execute(scopeDescription, task, currentTraceScopeId);
          }
        };

        return threadPoolExecutor.submit(taskWithObservable);
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
        new SynchronousQueue<Runnable>(),
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
        } catch (Exception ex) {
          return Observable.error(ex);
        }
      }
    });
  }

  // Internal calls are provided to prevent deadlocks in the thread executor pool
  private Observable<GetPathPropertiesHeaders> getPathPropertiesInternal(final AdfsHttpClient adfsHttpClient, final Path path) {
    final String relativePath = getRelativePath(path);
    Observable<ServiceResponseWithHeaders<Void, GetPathPropertiesHeaders>> response =
        adfsHttpClient.getPathPropertiesWithServiceResponseAsync(
            adfsHttpClient.getSession().getFileSystem(),
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
      final AzureDistributedFileSystem azureDistributedFileSystem,
      final AdfsHttpClient adfsHttpClient, final Path path, final boolean overwrite) throws AzureDistributedFileSystemException {
    return getFileStatusInternal(azureDistributedFileSystem, adfsHttpClient, path).flatMap(new Func1<FileStatus, Observable<OutputStream>>() {
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

        return Observable.just(adfsStreamFactory.createWriteStream(
            azureDistributedFileSystem,
            path,
            offset));
      }
    });
  }

  // Internal calls are provided to prevent deadlocks in the thread executor pool
  private Observable<VersionedFileStatus> getFileStatusInternal(
      final AzureDistributedFileSystem azureDistributedFileSystem,
      final AdfsHttpClient adfsHttpClient,
      final Path path)
      throws AzureDistributedFileSystemException {
    if (path.isRoot()) {
      return getFileSystemPropertiesInternal(adfsHttpClient).flatMap(new Func1<GetFilesystemPropertiesHeaders, Observable<VersionedFileStatus>>() {
        @Override
        public Observable<VersionedFileStatus> call(GetFilesystemPropertiesHeaders getFilesystemPropertiesHeaders) {
          return Observable.just(getFileStatusFromFilesystemPropertiesHeaders(azureDistributedFileSystem, path, getFilesystemPropertiesHeaders));
        }
      });
    }

    return getPathPropertiesInternal(adfsHttpClient, path).flatMap(new Func1<GetPathPropertiesHeaders, Observable<VersionedFileStatus>>() {
      @Override
      public Observable<VersionedFileStatus> call(GetPathPropertiesHeaders getPathPropertiesHeaders) {
        return Observable.just(getFileStatusFromPathPropertiesHeaders(azureDistributedFileSystem, path, getPathPropertiesHeaders));
      }
    });
  }

  private VersionedFileStatus getFileStatusFromFilesystemPropertiesHeaders(
      final AzureDistributedFileSystem azureDistributedFileSystem,
      final Path path,
      final GetFilesystemPropertiesHeaders getFilesystemPropertiesHeaders) {
    final long blockSize = configurationService.getAzureBlockSize();
    return new VersionedFileStatus(
        azureDistributedFileSystem.getOwnerUser(),
        azureDistributedFileSystem.getOwnerUserPrimaryGroup(),
        0,
        true,
        1,
        blockSize,
        parseLastModifiedTime(getFilesystemPropertiesHeaders.lastModified()).getMillis(),
        path,
        getFilesystemPropertiesHeaders.eTag());
  }

  private VersionedFileStatus getFileStatusFromPathPropertiesHeaders(
      final AzureDistributedFileSystem azureDistributedFileSystem,
      final Path path,
      final GetPathPropertiesHeaders getPathPropertiesHeaders) {
    final long blockSize = configurationService.getAzureBlockSize();
    return new VersionedFileStatus(
        azureDistributedFileSystem.getOwnerUser(),
        azureDistributedFileSystem.getOwnerUserPrimaryGroup(),
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