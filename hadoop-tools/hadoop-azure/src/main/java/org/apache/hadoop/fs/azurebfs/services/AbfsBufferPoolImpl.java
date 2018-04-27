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

import com.google.inject.Singleton;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.azurebfs.contracts.services.AbfsBufferPool;

/**
 * File System service to provider AzureBlobFileSystem client.
 */
@Singleton
@InterfaceAudience.Private
@InterfaceStability.Evolving
final class AbfsBufferPoolImpl implements AbfsBufferPool {
  AbfsBufferPoolImpl() {
  }

  @Override
  public ByteBuf getByteBuffer(final byte[] bytes) {
    ByteBuf buffer = Unpooled.wrappedBuffer(bytes);
    buffer.retain();
    return buffer;
  }

  @Override
  public ByteBuf copy(ByteBuf byteBuf) {
    ByteBuf buffer = byteBuf.copy();
    buffer.retain();
    return buffer;
  }

  @Override
  public ByteBuf getDynamicByteBuffer(int bufferSize) {
    ByteBuf buffer = Unpooled.buffer(0, bufferSize);
    buffer.retain();
    return buffer;
  }

  @Override
  public ByteBuf getFixedByteBuffer(int bufferSize) {
    ByteBuf buffer = Unpooled.buffer(bufferSize);
    buffer.retain();
    return buffer;
  }

  @Override
  public synchronized boolean releaseByteBuffer(final ByteBuf byteBuf) {
    while (byteBuf.refCnt() != 0) {
      byteBuf.release();
    }

    return true;
  }
}