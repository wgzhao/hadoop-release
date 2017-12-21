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

package org.apache.hadoop.fs.azuredfs.contracts.services;

import io.netty.buffer.ByteBuf;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

/**
 * AdfsBufferPool to create and release buffers.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public interface AdfsBufferPool extends InjectableService {
  /**
   * Gets bytes buffer object for an existing byte array
   * @param bytes to create ByteBuf from.
   * @return ByteBuf
   */
  ByteBuf getByteBuffer(byte[] bytes);

  /**
   * Copies a bytebuffer.
   * @param bytebuffer to copy.
   * @return ByteBuf copied bytebuffer
   */
  ByteBuf copy(ByteBuf byteBuf);

  /**
   * Gets an empty buffer with dynamic size with maximum capacity of bufferSize
   * @param bufferSize maximum capacity of the buffer
   * @return ByteBuf
   */
  ByteBuf getDynamicByteBuffer(int bufferSize);

  /**
   * Gets an empty buffer with fixed size with maximum capacity of bufferSize
   * @param bufferSize maximum capacity of the buffer
   * @return ByteBuf
   */
  ByteBuf getFixedByteBuffer(int bufferSize);

  /**
   * Releases an existing buffer
   * @return true if buffer is successfully released
   */
  boolean releaseByteBuffer(ByteBuf byteBuf);
}