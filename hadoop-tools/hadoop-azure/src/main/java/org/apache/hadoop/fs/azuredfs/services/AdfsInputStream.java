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

import java.io.EOFException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import com.google.common.base.Preconditions;
import io.netty.buffer.ByteBuf;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.FSExceptionMessages;
import org.apache.hadoop.fs.FSInputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.azuredfs.AzureDistributedFileSystem;
import org.apache.hadoop.fs.azuredfs.constants.FileSystemConfigurations;
import org.apache.hadoop.fs.azuredfs.contracts.exceptions.AzureDistributedFileSystemException;
import org.apache.hadoop.fs.azuredfs.contracts.services.AdfsBufferPool;
import org.apache.hadoop.fs.azuredfs.contracts.services.AdfsHttpService;
import org.apache.hadoop.fs.azuredfs.contracts.services.LoggingService;
import org.apache.hadoop.fs.azuredfs.contracts.services.TracingService;
import org.apache.htrace.core.TraceScope;

@InterfaceAudience.Private
@InterfaceStability.Evolving
final class AdfsInputStream extends FSInputStream {
  private final AzureDistributedFileSystem azureDistributedFileSystem;
  private final AdfsHttpService adfsHttpService;
  private final AdfsBufferPool adfsBufferPool;
  private final TracingService tracingService;
  private final LoggingService loggingService;
  private final Path path;

  private long offset;
  private long bufferOffsetInStream;
  private long fileLength = 0;
  private ByteBuf buffer;
  private boolean closed;
  private List<Future<Integer>> tasks;
  private final int bufferSize;
  private final String version;

  AdfsInputStream(
      final AdfsBufferPool adfsBufferPool,
      final AdfsHttpService adfsHttpService,
      final AzureDistributedFileSystem azureDistributedFileSystem,
      final TracingService tracingService,
      final LoggingService loggingService,
      final Path path,
      final long fileLength,
      final int bufferSize,
      final String version) {
    Preconditions.checkNotNull(adfsBufferPool, "adfsBufferPool");
    Preconditions.checkNotNull(azureDistributedFileSystem, "azureDistributedFileSystem");
    Preconditions.checkNotNull(adfsHttpService, "adfsHttpService");
    Preconditions.checkNotNull(tracingService, "tracingService");
    Preconditions.checkNotNull(loggingService, "loggingService");
    Preconditions.checkNotNull(path, "path");
    Preconditions.checkArgument(bufferSize >= FileSystemConfigurations.MIN_BUFFER_SIZE);
    Preconditions.checkNotNull(version, "version");
    Preconditions.checkArgument(version.length() > 0);

    this.azureDistributedFileSystem = azureDistributedFileSystem;
    this.adfsHttpService = adfsHttpService;
    this.tracingService = tracingService;
    this.loggingService = loggingService;
    this.path = path;
    this.offset = 0;
    this.bufferOffsetInStream = -1;
    this.fileLength = fileLength;
    this.closed = false;
    this.tasks = new ArrayList<>();
    this.bufferSize = bufferSize;
    this.adfsBufferPool = adfsBufferPool;
    this.buffer = this.adfsBufferPool.getByteBuffer(new byte[this.bufferSize]);
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
    this.loggingService.debug("AdfsInputStream.read byte array length: {0} offset: {1} len: {2}", b.length, off, len);
    TraceScope traceScope = this.tracingService.traceBegin("AdfsInputStream.read");

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

    try {
      if (len < FileSystemConfigurations.MIN_BUFFER_SIZE) {
        return readFromBuffer(b, off, len);
      }

      return readFromService(b, off, len);
    } catch (InterruptedException | ExecutionException ex) {
      for (Future task : tasks) {
        if (!task.isDone()) {
          task.cancel(true);
        }
      }

      throw new IOException(ex);
    } finally {
      this.tasks.clear();
      this.tracingService.traceEnd(traceScope);
    }
  }

  @Override
  public synchronized void close() throws IOException {
    if (closed) {
      return;
    }

    closed = true;
    this.adfsBufferPool.releaseByteBuffer(this.buffer);
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

  private synchronized int readFromService(final byte[] b, final int off, final int len) throws IOException, ExecutionException, InterruptedException {
    this.loggingService.debug("AdfsInputStream.readFromService byte array length: {0} offset: {1} len: {2}", b.length, off, len);
    TraceScope traceScope = this.tracingService.traceBegin("AdfsInputStream.readFromService");

    int readLength = available() > len ? len : available();
    int remainingBytesToRead = readLength;
    int bufferOffset = off;

    final ByteBuf bytesBuffer = this.adfsBufferPool.getByteBuffer(b);

    while (remainingBytesToRead != 0) {
      int bytesToRead = remainingBytesToRead > bufferSize ? bufferSize : remainingBytesToRead;

      sendReadRequest(
          this.offset,
          bytesToRead,
          bytesBuffer,
          bufferOffset);

      this.offset += bytesToRead;

      bufferOffset += bytesToRead;
      remainingBytesToRead -= bytesToRead;
    }

    int totalBytesRead = this.waitAllAndGetTotalReadBytes();
    Preconditions.checkArgument(readLength == totalBytesRead);

    this.adfsBufferPool.releaseByteBuffer(bytesBuffer);
    this.tracingService.traceEnd(traceScope);
    return readLength;
  }

  private synchronized int readFromBuffer(final byte[] b, final int off, final int len) throws IOException, ExecutionException, InterruptedException {
    this.loggingService.debug("AdfsInputStream.readFromBuffer byte array length: {0} offset: {1} len: {2}", b.length, off, len);
    TraceScope traceScope = this.tracingService.traceBegin("AdfsInputStream.readFromBuffer");

    // Check to see if we already buffered the request.
    if (this.offset >= bufferOffsetInStream
        && this.offset + len <= bufferOffsetInStream + this.buffer.writerIndex()) {
      long startOffset = this.offset - bufferOffsetInStream;
      this.buffer.readerIndex((int) startOffset);
    }
    else {
      int remainingSize = this.bufferSize > available() ? available() : this.bufferSize;
      this.buffer.setIndex(0, remainingSize);

      sendReadRequest(
          this.offset,
          remainingSize,
          this.buffer,
          0);

      int totalBytesRead = this.waitAllAndGetTotalReadBytes();
      Preconditions.checkArgument(totalBytesRead == remainingSize);

      this.bufferOffsetInStream = this.offset;
    }

    int readLength = len > available() ? available() : len;
    this.offset += readLength;
    this.buffer.readBytes(b, off, readLength);
    this.tracingService.traceEnd(traceScope);
    return readLength;
  }

  private synchronized int waitAllAndGetTotalReadBytes() throws InterruptedException, ExecutionException {
    int totalBytesRead = 0;
    for (Future<Integer> task : tasks) {
      totalBytesRead += task.get();
    }

    return totalBytesRead;
  }

  private synchronized void sendReadRequest(
      final long serviceOffset,
      final int serviceLength,
      final ByteBuf targetBuffer,
      final int bufferOffset) throws IOException {

    try {
      final Future<Integer> read = this.adfsHttpService.readFileAsync(
          this.azureDistributedFileSystem,
          path,
          version,
          serviceOffset,
          serviceLength,
          targetBuffer,
          bufferOffset);

      this.tasks.add(read);
    } catch (AzureDistributedFileSystemException exception) {
      throw new IOException(exception);
    }
  }
}