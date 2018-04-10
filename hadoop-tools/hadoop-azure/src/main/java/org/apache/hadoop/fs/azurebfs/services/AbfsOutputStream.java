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

import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import com.google.common.base.Preconditions;
import io.netty.buffer.ByteBuf;
import org.threadly.concurrent.collections.ConcurrentArrayList;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.FSExceptionMessages;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.Syncable;
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
final class AbfsOutputStream extends OutputStream implements Syncable {
  private final AzureBlobFileSystem azureBlobFileSystem;
  private final AbfsHttpService abfsHttpService;
  private final AbfsBufferPool abfsBufferPool;
  private final TracingService tracingService;
  private final LoggingService loggingService;
  private final AbfsStatisticsService abfsStatisticsService;
  private final Path path;
  private final int bufferSize;
  private final ExecutorService taskCleanupJobExecutor;

  private ByteBuf buffer;
  private boolean closed;
  private ConcurrentArrayList<WriteOperation> writeOperations;
  private long offset;
  private long lastFlushOffset;

  AbfsOutputStream(
      final AbfsHttpService abfsHttpService,
      final AbfsBufferPool abfsBufferPool,
      final AzureBlobFileSystem azureBlobFileSystem,
      final AbfsStatisticsService abfsStatisticsService,
      final TracingService tracingService,
      final LoggingService loggingService,
      final Path path,
      final long offset,
      final int bufferSize) {
    Preconditions.checkNotNull(azureBlobFileSystem, "azureBlobFileSystem");
    Preconditions.checkNotNull(abfsStatisticsService, "abfsStatisticsService");
    Preconditions.checkNotNull(abfsHttpService, "abfsHttpService");
    Preconditions.checkNotNull(abfsBufferPool, "tracingService");
    Preconditions.checkNotNull(abfsBufferPool, "loggingService");
    Preconditions.checkNotNull(path, "path");
    Preconditions.checkNotNull(abfsBufferPool, "abfsBufferPool");
    Preconditions.checkArgument(offset >= 0);
    Preconditions.checkArgument(bufferSize >= FileSystemConfigurations.MIN_BUFFER_SIZE
        && bufferSize <= FileSystemConfigurations.MAX_BUFFER_SIZE);

    this.abfsBufferPool = abfsBufferPool;
    this.azureBlobFileSystem = azureBlobFileSystem;
    this.abfsHttpService = abfsHttpService;
    this.abfsStatisticsService = abfsStatisticsService;
    this.tracingService = tracingService;
    this.loggingService = loggingService;
    this.path = path;
    this.closed = false;
    this.bufferSize = bufferSize;
    this.buffer = this.abfsBufferPool.getDynamicByteBuffer(this.bufferSize);
    this.writeOperations = new ConcurrentArrayList<>();

    this.offset = offset;
    this.taskCleanupJobExecutor = Executors.newCachedThreadPool();
  }

  /**
   * Writes the specified byte to this output stream. The general contract for
   * write is that one byte is written to the output stream. The byte to be
   * written is the eight low-order bits of the argument b. The 24 high-order
   * bits of b are ignored.
   *
   * @param byteVal the byteValue to write.
   * @throws IOException if an I/O error occurs. In particular, an IOException may be
   *                     thrown if the output stream has been closed.
   */
  @Override
  public void write(final int byteVal) throws IOException {
    write(new byte[]{(byte) (byteVal & 0xFF)});
  }

  /**
   * Writes length bytes from the specified byte array starting at off to
   * this output stream.
   *
   * @param data   the byte array to write.
   * @param off the start off in the data.
   * @param length the number of bytes to write.
   * @throws IOException if an I/O error occurs. In particular, an IOException may be
   *                     thrown if the output stream has been closed.
   */
  @Override
  public synchronized void write(final byte[] data, final int off, final int length)
      throws IOException {
    if (closed) {
      throw new IOException(FSExceptionMessages.STREAM_IS_CLOSED);
    }

    Preconditions.checkArgument(data != null, "null data");

    if (off < 0 || length < 0 || length > data.length - off) {
      throw new IndexOutOfBoundsException();
    }

    this.loggingService.debug("AbfsOutputStream.write byte array length: {0} offset: {1} len: {2}", data.length, off, length);
    TraceScope traceScope = this.tracingService.traceBegin("AbfsOutputStream.write");

    int currentOffset = off;
    int writableBytes = this.buffer.maxWritableBytes();
    int numberOfBytesToWrite = length;

    while (numberOfBytesToWrite > 0) {
      if (writableBytes < numberOfBytesToWrite) {
        this.buffer.writeBytes(data, currentOffset, writableBytes);
        writeCurrentBufferToService();

        currentOffset = currentOffset + writableBytes;
        numberOfBytesToWrite = numberOfBytesToWrite - writableBytes;
      } else {
        this.buffer.writeBytes(data, currentOffset, numberOfBytesToWrite);
        numberOfBytesToWrite = 0;
      }

      writableBytes = this.buffer.maxWritableBytes();
    }

    this.abfsStatisticsService.incrementWriteOps(this.azureBlobFileSystem, 1);
    this.tracingService.traceEnd(traceScope);
  }

  /**
   * Flushes this output stream and forces any buffered output bytes to be
   * written out. If any data remains in the payload it is committed to the
   * service. Data is queued for writing and forced out to the service
   * before the call returns.
   */
  @Override
  public void flush() throws IOException {
    if (closed) {
      throw new IOException(FSExceptionMessages.STREAM_IS_CLOSED);
    }

    this.flushInternalAsync();
  }

  /**
   * @deprecated As of HADOOP 0.21.0, replaced by hflush
   * @see #hflush()
   */
  @Override
  public void sync() throws IOException {
    if (closed) {
      throw new IOException(FSExceptionMessages.STREAM_IS_CLOSED);
    }

    this.flushInternal();
  }

  /** Similar to posix fsync, flush out the data in client's user buffer
   * all the way to the disk device (but the disk may have it in its cache).
   * @throws IOException if error occurs
   */
  @Override
  public void hsync() throws IOException {
    if (closed) {
      throw new IOException(FSExceptionMessages.STREAM_IS_CLOSED);
    }

    this.flushInternal();
  }

  /** Flush out the data in client's user buffer. After the return of
   * this call, new readers will see the data.
   * @throws IOException if any error occurs
   */
  @Override
  public void hflush() throws IOException {
    if (closed) {
      throw new IOException(FSExceptionMessages.STREAM_IS_CLOSED);
    }

    this.flushInternal();
  }

  /**
   * Force all data in the output stream to be written to Azure storage.
   * Wait to return until this is complete. Close the access to the stream and
   * shutdown the upload thread pool.
   * If the blob was created, its lease will be released.
   * Any error encountered caught in threads and stored will be rethrown here
   * after cleanup.
   */
  @Override
  public synchronized void close() throws IOException {
    if (closed) {
      return;
    }

    this.flushInternal();
    writeOperations.clear();

    this.taskCleanupJobExecutor.shutdownNow();
    this.abfsBufferPool.releaseByteBuffer(this.buffer);
    this.closed = true;
  }

  private synchronized void flushInternal() throws IOException {
    this.writeCurrentBufferToService();
    this.flushWrittenBytesToService();
  }

  private synchronized void flushInternalAsync() throws IOException {
    this.writeCurrentBufferToService();
    this.flushWrittenBytesToServiceAsync();
  }

  private synchronized void writeCurrentBufferToService() throws IOException {
    if (this.buffer.readableBytes() == 0) {
      return;
    }

    this.loggingService.debug("AbfsOutputStream.writeCurrentBufferToService");
    TraceScope traceScope = this.tracingService.traceBegin("AbfsOutputStream.writeCurrentBufferToService");

    try {
      final ByteBuf bytes = this.abfsBufferPool.copy(this.buffer);
      final int readableBytes = bytes.readableBytes();

      this.abfsBufferPool.releaseByteBuffer(this.buffer);
      this.buffer = this.abfsBufferPool.getDynamicByteBuffer(bufferSize);
      final long offset = this.offset;
      this.offset += readableBytes;

      final Future<Void> append = abfsHttpService.writeFileAsync(
          azureBlobFileSystem,
          path,
          bytes,
          offset);

      final Future job = this.taskCleanupJobExecutor.submit(new Callable<Void>() {
        @Override
        public Void call() throws Exception {
          try {
            append.get();
          }
          finally {
            abfsBufferPool.releaseByteBuffer(bytes);
          }

          return null;
        }
      });

      this.abfsStatisticsService.incrementBytesWritten(this.azureBlobFileSystem, readableBytes);
      this.writeOperations.add(new WriteOperation(job, offset, readableBytes));
    } catch (AzureBlobFileSystemException exception) {
      throw new IOException(exception);
    }
    finally {
      this.tracingService.traceEnd(traceScope);
    }
  }

  private synchronized void flushWrittenBytesToService() throws IOException {
    for (WriteOperation writeOperation : this.writeOperations) {
      try {
        writeOperation.task.get();
      } catch (InterruptedException | ExecutionException ex) {
        throw new IOException(ex);
      }
    }

    flushWrittenBytesToServiceInternal(this.offset, false);
  }

  private synchronized void flushWrittenBytesToServiceAsync() throws IOException {
    ArrayList<WriteOperation> finishedWriteOperations = new ArrayList<>();
    for (WriteOperation writeOperation : this.writeOperations) {
      if (writeOperation.task.isDone()
          && (writeOperation.startOffset + writeOperation.length) >= this.lastFlushOffset) {
        finishedWriteOperations.add(writeOperation);
      }
    }

    if (finishedWriteOperations.size() == 0) {
      return;
    }

    Collections.sort(finishedWriteOperations, new Comparator<WriteOperation>() {
      @Override
      public int compare(WriteOperation o1, WriteOperation o2) {
        if (o1.startOffset == o2.startOffset
            && o1.task.hashCode() == o2.task.hashCode()
            && o1.length == o2.length) {
          return 0;
        }

        return (int) (o1.startOffset - o2.startOffset);
      }
    });

    long writtenOffset = 0;
    for (int i = 0; i < finishedWriteOperations.size() - 1; i++) {
      if (finishedWriteOperations.get(i).startOffset + finishedWriteOperations.get(i).length
          != finishedWriteOperations.get(i + 1).startOffset) {
        return;
      }

      if (finishedWriteOperations.get(i).startOffset == writtenOffset) {
        writtenOffset += finishedWriteOperations.get(i).length;
      }
    }

    if (writtenOffset > this.lastFlushOffset) {
      this.flushWrittenBytesToServiceInternal(writtenOffset, true);
    }
  }

  private synchronized void flushWrittenBytesToServiceInternal(final long offset, final boolean retainUncommitedData) throws IOException {
    this.loggingService.debug("AbfsOutputStream.flushWrittenBytesToService");
    TraceScope traceScope = this.tracingService.traceBegin("AbfsOutputStream.flushWrittenBytesToService");

    try {
      abfsHttpService.flushFile(azureBlobFileSystem, path, offset, retainUncommitedData);
      this.lastFlushOffset = offset;
    } catch (AzureBlobFileSystemException exception) {
      throw new IOException(exception);
    } finally {
      this.abfsBufferPool.releaseByteBuffer(this.buffer);
      this.buffer = this.abfsBufferPool.getDynamicByteBuffer(this.bufferSize);
      this.tracingService.traceEnd(traceScope);
    }
  }

  class WriteOperation {
    private final Future<Void> task;
    private final long startOffset;
    private final long length;

    WriteOperation(final Future<Void> task, final long startOffset, final long length) {
      Preconditions.checkNotNull(task, "task");
      Preconditions.checkArgument(startOffset >= 0, "startOffset");
      Preconditions.checkArgument(length >= 0, "length");

      this.task = task;
      this.startOffset = startOffset;
      this.length = length;
    }
  }
}