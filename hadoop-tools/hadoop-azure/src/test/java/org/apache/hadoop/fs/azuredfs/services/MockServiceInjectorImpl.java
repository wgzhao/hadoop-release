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

import java.util.HashMap;
import java.util.Map;

import com.google.inject.AbstractModule;

public final class MockServiceInjectorImpl extends AbstractModule {
  private final Map<Class, Class> bindings;

  public MockServiceInjectorImpl() {
    this.bindings = new HashMap<>();
  }

  @Override
  @SuppressWarnings("unchecked")
  protected final void configure() {
    for (Map.Entry<Class, Class> entrySet : this.bindings.entrySet()) {
      bind(entrySet.getKey()).to(entrySet.getValue());
    }
  }

  public <T> void bind(Class<T> tInterface, Class<? extends T> tClazz) {
    this.bindings.put(tInterface, tClazz);
  }
}
