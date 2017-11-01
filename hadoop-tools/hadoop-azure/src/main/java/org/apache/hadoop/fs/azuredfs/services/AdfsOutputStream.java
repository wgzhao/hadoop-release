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

import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import com.google.common.base.Preconditions;
import io.netty.buffer.ByteBuf;
import rx.Observable;
import rx.functions.Action0;
import rx.schedulers.Schedulers;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.FSExceptionMessages;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.Syncable;
import org.apache.hadoop.fs.azuredfs.AzureDistributedFileSystem;
import org.apache.hadoop.fs.azuredfs.contracts.exceptions.AzureDistributedFileSystemException;
import org.apache.hadoop.fs.azuredfs.contracts.services.AdfsBufferPool;
import org.apache.hadoop.fs.azuredfs.contracts.services.AdfsHttpService;

@InterfaceAudience.Private
@InterfaceStability.Evolving
final class AdfsOutputStream extends OutputStream implements Syncable {
  private static final int DEFAULT_UPLOAD_BLOCK_SIZE = 4 * 1024 * 1024;

  private final AzureDistributedFileSystem azureDistributedFileSystem;
  private final AdfsHttpService adfsHttpService;
  private final AdfsBufferPool adfsBufferPool;
  private final Path path;

  private ByteBuf buffer;
  private long writtenBytesToService;
  private boolean closed;
  private List<Future<Void>> tasks;
  private final boolean flushable;

  AdfsOutputStream(
      final AdfsHttpService adfsHttpService,
      final AdfsBufferPool adfsBufferPool,
      final AzureDistributedFileSystem azureDistributedFileSystem,
      final Path path,
      final boolean flushable) {
    Preconditions.checkNotNull(azureDistributedFileSystem, "azureDistributedFileSystem");
    Preconditions.checkNotNull(adfsHttpService, "adfsHttpService");
    Preconditions.checkNotNull(path, "path");
    Preconditions.checkNotNull(adfsBufferPool, "adfsBufferPool");

    this.adfsBufferPool = adfsBufferPool;
    this.azureDistributedFileSystem = azureDistributedFileSystem;
    this.adfsHttpService = adfsHttpService;
    this.path = path;
    this.closed = false;
    this.buffer = this.adfsBufferPool.getByteBuffer(DEFAULT_UPLOAD_BLOCK_SIZE);
    this.tasks = new ArrayList<>();
    this.flushable = flushable;
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
   * Writes length bytes from the specified byte array starting at offset to
   * this output stream.
   *
   * @param data   the byte array to write.
   * @param offset the start offset in the data.
   * @param length the number of bytes to write.
   * @throws IOException if an I/O error occurs. In particular, an IOException may be
   *                     thrown if the output stream has been closed.
   */
  @Override
  public synchronized void write(final byte[] data, final int offset, final int length)
      throws IOException {
    Preconditions.checkArgument(data != null, "null data");

    if (offset < 0 || length < 0 || length > data.length - offset) {
      throw new IndexOutOfBoundsException();
    }

    if (closed) {
      throw new IOException(FSExceptionMessages.STREAM_IS_CLOSED);
    }

    int currentOffset = offset;
    int writableBytes = this.buffer.writableBytes();
    int numberOfBytesToWrite = length - currentOffset;

    while (numberOfBytesToWrite != 0) {
      if (writableBytes < numberOfBytesToWrite) {
        this.buffer.writeBytes(data, currentOffset, writableBytes);
        writeCurrentBufferToService();

        this.buffer = this.adfsBufferPool.getByteBuffer(DEFAULT_UPLOAD_BLOCK_SIZE);
        currentOffset = currentOffset + writableBytes;
        numberOfBytesToWrite = numberOfBytesToWrite - writableBytes;
      } else {
        this.buffer.writeBytes(data, currentOffset, numberOfBytesToWrite);
        numberOfBytesToWrite = 0;
      }

      writableBytes = this.buffer.writableBytes();
    }
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
      return;
    }

    if (!flushable) {
      return;
    }

    this.flushInternal();
  }

  /**
   * Force all data in the output stream to be written to Azure storage.
   * Wait to return until this is complete.
   */
  @Override
  public void sync() throws IOException {
  }

  /**
   * Force all data in the output stream to be written to Azure storage.
   * Wait to return until this is complete.
   */
  @Override
  public void hsync() throws IOException {
  }

  /**
   * Force all data in the output stream to be written to Azure storage.
   * Wait to return until this is complete.
   */
  @Override
  public void hflush() throws IOException {
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
  }

  private synchronized void flushInternal() throws IOException {
    this.writeCurrentBufferToService();
    this.flushWrittenBytesToService();
  }

  private synchronized void writeCurrentBufferToService() throws IOException {
    if (this.buffer.readableBytes() == 0) {
      return;
    }

    final byte[] bytes;
    final int length = this.buffer.readableBytes();
    bytes = new byte[length];
    this.buffer.getBytes(this.buffer.readerIndex(), bytes);
    adfsBufferPool.releaseByteBuffer(this.buffer);

    try {
      final Future<Void> append = adfsHttpService.appendFileAsync(azureDistributedFileSystem,
          path,
          bytes,
          writtenBytesToService);

      writtenBytesToService += length;

      final AdfsOutputStream adfsOutputStream = this;
      Observable.from(append).doOnCompleted(new Action0() {
        @Override
        public void call() {
          synchronized (adfsOutputStream) {
            adfsOutputStream.tasks.remove(adfsOutputStream.tasks.indexOf(append));
          }
        }
      }).subscribeOn(Schedulers.newThread());

      this.tasks.add(append);
    } catch (AzureDistributedFileSystemException exception) {
      throw new IOException(exception);
    }
  }

  private synchronized void flushWrittenBytesToService() throws IOException {
    final long currentWrittenBytes = writtenBytesToService;
    for (Future<Void> task : this.tasks) {
      try {
        task.get();
      } catch (InterruptedException | ExecutionException ex) {
        throw new IOException(ex);
      }
    }

    try {
      adfsHttpService.flushFile(azureDistributedFileSystem, path, currentWrittenBytes);
    } catch (AzureDistributedFileSystemException exception) {
      for (Future task : tasks) {
        if (!task.isDone()) {
          task.cancel(true);
        }
      }

      throw new IOException(exception);
    } finally {
      this.tasks.clear();
    }
  }
}