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

import java.io.EOFException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;

import com.google.common.base.Preconditions;
import io.netty.buffer.ByteBuf;
import rx.Observable;
import rx.functions.Func1;
import rx.functions.FuncN;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.FSExceptionMessages;
import org.apache.hadoop.fs.FSInputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.azurebfs.AzureBlobFileSystem;
import org.apache.hadoop.fs.azurebfs.constants.FileSystemConfigurations;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.AzureBlobFileSystemException;
import org.apache.hadoop.fs.azurebfs.contracts.services.AbfsBufferPool;
import org.apache.hadoop.fs.azurebfs.contracts.services.AbfsHttpService;
import org.apache.hadoop.fs.azurebfs.contracts.services.AbfsStatisticsService;
import org.apache.hadoop.fs.azurebfs.contracts.services.LoggingService;
import org.apache.hadoop.fs.azurebfs.contracts.services.TracingService;
import org.apache.htrace.core.TraceScope;

@InterfaceAudience.Private
@InterfaceStability.Evolving
final class AbfsInputStream extends FSInputStream {
  private final AzureBlobFileSystem azureBlobFileSystem;
  private final AbfsHttpService abfsHttpService;
  private final AbfsBufferPool abfsBufferPool;
  private final TracingService tracingService;
  private final LoggingService loggingService;
  private final AbfsStatisticsService abfsStatisticsService;
  private final Path path;

  private long offset;
  private long bufferOffsetInStream;
  private long fileLength = 0;
  private ByteBuf buffer;
  private boolean closed;
  private final int bufferSize;
  private final String version;

  AbfsInputStream(
      final AbfsBufferPool abfsBufferPool,
      final AbfsHttpService abfsHttpService,
      final AzureBlobFileSystem azureBlobFileSystem,
      final AbfsStatisticsService abfsStatisticsService,
      final TracingService tracingService,
      final LoggingService loggingService,
      final Path path,
      final long fileLength,
      final int bufferSize,
      final String version) {
    Preconditions.checkNotNull(abfsBufferPool, "abfsBufferPool");
    Preconditions.checkNotNull(azureBlobFileSystem, "azureBlobFileSystem");
    Preconditions.checkNotNull(abfsHttpService, "abfsHttpService");
    Preconditions.checkNotNull(tracingService, "tracingService");
    Preconditions.checkNotNull(loggingService, "loggingService");
    Preconditions.checkNotNull(abfsStatisticsService, "abfsStatisticsService");
    Preconditions.checkNotNull(path, "path");
    Preconditions.checkArgument(bufferSize >= FileSystemConfigurations.MIN_BUFFER_SIZE);
    Preconditions.checkNotNull(version, "version");
    Preconditions.checkArgument(version.length() > 0);

    this.azureBlobFileSystem = azureBlobFileSystem;
    this.abfsHttpService = abfsHttpService;
    this.tracingService = tracingService;
    this.loggingService = loggingService.get(AbfsInputStream.class);
    this.abfsStatisticsService = abfsStatisticsService;
    this.path = path;
    this.offset = 0;
    this.bufferOffsetInStream = -1;
    this.fileLength = fileLength;
    this.closed = false;
    this.bufferSize = bufferSize;
    this.abfsBufferPool = abfsBufferPool;
    this.buffer = this.abfsBufferPool.getByteBuffer(new byte[this.bufferSize]);
    this.buffer.clear();
    this.version = version;
  }

  /**
   * Return the size of the remaining available bytes
   * if the size is less than or equal to {@link Integer#MAX_VALUE},
   * otherwise, return {@link Integer#MAX_VALUE}.
   * <p>
   * This is to match the behavior of DFSInputStream.available(),
   * which some clients may rely on (HBase write-ahead log reading in
   * particular).
   */
  @Override
  public synchronized int available() throws IOException {
    if (closed) {
      throw new EOFException(FSExceptionMessages.STREAM_IS_CLOSED);
    }

    final long remaining = this.fileLength - this.offset;
    return remaining <= Integer.MAX_VALUE
        ? (int) remaining : Integer.MAX_VALUE;
  }

  /*
   * Reads the next byte of data from the input stream. The value byte is
   * returned as an integer in the range 0 to 255. If no byte is available
   * because the end of the stream has been reached, the value -1 is returned.
   * This method blocks until input data is available, the end of the stream
   * is detected, or an exception is thrown.
   *
   * @returns int An integer corresponding to the byte read.
   */
  @Override
  public synchronized int read() throws IOException {
    byte[] tmpBuf = new byte[1];
    if (read(tmpBuf, 0, 1) < 0) {
      return -1;
    }

    // byte values are in range of -128 to 128, with this we convert it to 0-256
    return tmpBuf[0] & 0xFF;
  }

  /*
   * Reads up to len bytes of data from the input stream into an array of
   * bytes. An attempt is made to read as many as len bytes, but a smaller
   * number may be read. The number of bytes actually read is returned as an
   * integer. This method blocks until input data is available, end of file is
   * detected, or an exception is thrown. If len is zero, then no bytes are
   * read and 0 is returned; otherwise, there is an attempt to read at least
   * one byte. If no byte is available because the stream is at end of file,
   * the value -1 is returned; otherwise, at least one byte is read and stored
   * into b.
   *
   * @param b -- the buffer into which data is read
   *
   * @param off -- the start offset in the array b at which data is written
   *
   * @param len -- the maximum number of bytes read
   *
   * @ returns int The total number of byes read into the buffer, or -1 if
   * there is no more data because the end of stream is reached.
   */
  @Override
  public synchronized int read(final byte[] b, final int off, final int len) throws IOException {
    this.loggingService.debug("AbfsInputStream.read byte array length: {0} offset: {1} len: {2}", b.length, off, len);
    TraceScope traceScope = this.tracingService.traceBegin("AbfsInputStream.read");

    if (closed) {
      throw new EOFException(FSExceptionMessages.STREAM_IS_CLOSED);
    }

    if (len == 0) {
      return 0;
    }

    if (this.available() == 0) {
      return -1;
    }

    if (off < 0 || len < 0 || len > b.length - off) {
      throw new IndexOutOfBoundsException();
    }

    int readLength = len > available() ? available() : len;

    // Check to see if buffer can be used.
    boolean canReadFromBuffer = this.offset >= bufferOffsetInStream
        && this.offset + readLength <= bufferOffsetInStream + this.buffer.writerIndex();

    if (canReadFromBuffer) {
      long bufferOffset = this.offset - bufferOffsetInStream;
      this.buffer.readerIndex((int) bufferOffset);

      this.offset += readLength;
      this.buffer.readBytes(b, off, readLength);
      this.tracingService.traceEnd(traceScope);
      return readLength;
    }

    int remainingBytesToRead = readLength;
    int bufferOffset = off;

    final ByteBuf targetBuffer = this.abfsBufferPool.getByteBuffer(b);

    List<Observable<Integer>> readObservables = new ArrayList<>();

    try {
      while (remainingBytesToRead != 0) {
        Observable<Integer> readObservable;
        int chunkLength;

        if (remainingBytesToRead > bufferSize) {
          readObservable = Observable.from(this.abfsHttpService.readFileAsync(
              this.azureBlobFileSystem,
              this.path,
              this.version,
              this.offset,
              this.bufferSize,
              targetBuffer,
              bufferOffset));

          chunkLength = bufferSize;
        }
        else {
          // Last download will be into the buffer
          this.buffer.setIndex(0, this.bufferSize);
          this.bufferOffsetInStream = this.offset;

          final long targetBufferOffset = bufferOffset;
          final long targetBufferBytesToWrite = remainingBytesToRead;

          readObservable = Observable.from(this.abfsHttpService.readFileAsync(
              this.azureBlobFileSystem,
              this.path,
              this.version,
              this.offset,
              this.bufferSize,
              this.buffer,
              0)).switchMap(new Func1<Integer, Observable<Integer>>() {
            @Override
            public Observable<Integer> call(Integer integer) {
              buffer.setIndex(0, integer);
              buffer.readBytes(targetBuffer, (int) targetBufferOffset, (int) targetBufferBytesToWrite);
              return Observable.just((int) targetBufferBytesToWrite);
            }
          });

          chunkLength = remainingBytesToRead;
        }

        bufferOffset += chunkLength;
        this.offset += chunkLength;
        remainingBytesToRead -= chunkLength;

        readObservables.add(readObservable);
      }

      // Wait synchronously on the list of read operations.
      Integer totalReadLength = Observable.zip(readObservables, new FuncN<Integer>() {
        @Override
        public Integer call(Object... results) {
          int totalLength = 0;
          for (Object result : results) {
            totalLength += (Integer) result;
          }

          return totalLength;
        }
      }).toBlocking().toFuture().get();

      Preconditions.checkArgument(totalReadLength == readLength);
    }
    catch (AzureBlobFileSystemException | ExecutionException ex) {
      throw new IOException(ex);
    }
    catch (InterruptedException ex) {
      Thread.currentThread().interrupt();
    }
    finally {
      this.tracingService.traceEnd(traceScope);
    }

    this.abfsStatisticsService.incrementReadOps(this.azureBlobFileSystem, 1);
    this.abfsStatisticsService.incrementBytesRead(this.azureBlobFileSystem, readLength);

    return readLength;
  }

  @Override
  public synchronized void close() throws IOException {
    if (closed) {
      return;
    }

    closed = true;
    this.abfsBufferPool.releaseByteBuffer(this.buffer);
    this.buffer = null;
  }

  @Override
  public synchronized void seek(final long pos) throws EOFException {
    if (closed) {
      throw new EOFException(FSExceptionMessages.STREAM_IS_CLOSED);
    }

    if (pos > this.fileLength || pos < 0) {
      throw new EOFException();
    }

    this.offset = pos;
  }

  @Override
  public synchronized long getPos() throws IOException {
    if (closed) {
      throw new EOFException(FSExceptionMessages.STREAM_IS_CLOSED);
    }

    return this.offset;
  }

  @Override
  public boolean seekToNewSource(final long targetPos) throws IOException {
    return false;
  }
}