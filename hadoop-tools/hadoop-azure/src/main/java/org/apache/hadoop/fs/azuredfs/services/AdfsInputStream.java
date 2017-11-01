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

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.FSExceptionMessages;
import org.apache.hadoop.fs.FSInputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.azuredfs.AzureDistributedFileSystem;
import org.apache.hadoop.fs.azuredfs.contracts.exceptions.AzureDistributedFileSystemException;
import org.apache.hadoop.fs.azuredfs.contracts.services.AdfsBufferPool;
import org.apache.hadoop.fs.azuredfs.contracts.services.AdfsHttpService;

@InterfaceAudience.Private
@InterfaceStability.Evolving
final class AdfsInputStream extends FSInputStream {
  private static final int DEFAULT_DOWNLOAD_BLOCK_SIZE = 4 * 1024 * 1024;

  private final AzureDistributedFileSystem azureDistributedFileSystem;
  private final AdfsHttpService adfsHttpService;
  private final AdfsBufferPool adfsBufferPool;
  private final Path path;

  private long offset;
  private long fileLength = 0;
  private boolean closed;
  private List<Future<Void>> tasks;

  AdfsInputStream(
      final AdfsHttpService adfsHttpService,
      final AdfsBufferPool adfsBufferPool,
      final AzureDistributedFileSystem azureDistributedFileSystem,
      final Path path,
      final long fileLength) {
    this(adfsHttpService, adfsBufferPool, azureDistributedFileSystem, path, fileLength, 0);
  }

  AdfsInputStream(
      final AdfsHttpService adfsHttpService,
      final AdfsBufferPool adfsBufferPool,
      final AzureDistributedFileSystem azureDistributedFileSystem,
      final Path path,
      final long fileLength,
      final long offset) {
    Preconditions.checkNotNull(azureDistributedFileSystem, "azureDistributedFileSystem");
    Preconditions.checkNotNull(adfsHttpService, "adfsHttpService");
    Preconditions.checkNotNull(path, "path");
    Preconditions.checkNotNull(adfsBufferPool, "adfsBufferPool");

    this.adfsBufferPool = adfsBufferPool;
    this.azureDistributedFileSystem = azureDistributedFileSystem;
    this.adfsHttpService = adfsHttpService;
    this.path = path;
    this.offset = offset;
    this.fileLength = fileLength;
    this.closed = false;
    this.tasks = new ArrayList<>();
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
    if (this.closed) {
      throw new IOException("Stream is closed");
    }

    final long remaining = this.fileLength - offset;
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

    return tmpBuf[0];
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
    if (offset < 0 || len < 0 || len > b.length - offset) {
      throw new IndexOutOfBoundsException();
    }

    if (closed) {
      throw new IOException(FSExceptionMessages.STREAM_IS_CLOSED);
    }

    try {
      int remainingBytesToRead = len;
      int bufferOffset = off;
      long currentOffset = offset;

      while (remainingBytesToRead != 0) {
        if (remainingBytesToRead < DEFAULT_DOWNLOAD_BLOCK_SIZE) {
          readFromService(
              currentOffset,
              remainingBytesToRead,
              b,
              bufferOffset);

          remainingBytesToRead = 0;
          currentOffset += remainingBytesToRead;
          bufferOffset += remainingBytesToRead;
        } else {
          readFromService(
              currentOffset,
              DEFAULT_DOWNLOAD_BLOCK_SIZE,
              b,
              bufferOffset);

          remainingBytesToRead -= DEFAULT_DOWNLOAD_BLOCK_SIZE;
          currentOffset += DEFAULT_DOWNLOAD_BLOCK_SIZE;
          bufferOffset += DEFAULT_DOWNLOAD_BLOCK_SIZE;
        }
      }

      for (Future<Void> task : tasks) {
        task.get();
      }

      return len;
    } catch (InterruptedException | ExecutionException ex) {
      for (Future task : tasks) {
        if (!task.isDone()) {
          task.cancel(true);
        }
      }

      throw new IOException(ex);
    } finally {
      this.tasks.clear();
    }
  }

  @Override
  public synchronized void close() throws IOException {
    if (!closed) {
      closed = true;
    }
  }

  @Override
  public synchronized void seek(final long pos) throws EOFException {
    if (pos > this.fileLength) {
      throw new EOFException();
    }

    this.offset = pos;
  }

  @Override
  public synchronized long getPos() throws IOException {
    return this.offset;
  }

  @Override
  public boolean seekToNewSource(final long targetPos) throws IOException {
    return false;
  }

  private synchronized void readFromService(
      final long serviceOffset,
      final int serviceLength,
      final byte[] targetBuffer,
      final int bufferOffset) throws IOException {

    try {
      final Future<Void> read = this.adfsHttpService.readFileAsync(
          this.azureDistributedFileSystem,
          path,
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