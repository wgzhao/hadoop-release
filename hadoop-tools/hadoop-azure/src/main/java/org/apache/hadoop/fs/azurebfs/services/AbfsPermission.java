/*
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
 *
 */

package org.apache.hadoop.fs.azurebfs.services;

import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;

/**
 * Hadoop shell command -getfacl does not invoke getAclStatus if FsPermission
 * from getFileStatus has not set ACL bit to true. By default getAclBit returns
 * false.
 *
 * Provision to make additional call to invoke getAclStatus would be redundant
 * when abfs is running as additional FS. To avoid this redundancy, provided
 * configuration to return true/false on getAclBit.
 */
class AbfsPermission extends FsPermission {
  private static final int STICKY_BIT_OCTAL_VALUE = 01000;
  private final boolean aclBit;

  public AbfsPermission(Short aShort, boolean aclBitStatus) {
    super(aShort);
    this.aclBit = aclBitStatus;
  }

  public AbfsPermission(FsAction u, FsAction g, FsAction o, boolean aclBitStatus) {
    super(u, g, o, false);
    this.aclBit = aclBitStatus;
  }

  /**
   * Returns true if "adl.feature.support.acl.bit" configuration is set to
   * true.
   *
   * If configuration is not set then default value is true.
   *
   * @return If configuration is not set then default value is true.
   */
  public boolean getAclBit() {
    return aclBit;
  }

  @Override
  public boolean equals(Object obj) {
    if (obj instanceof FsPermission) {
      FsPermission that = (FsPermission) obj;
      return this.getUserAction() == that.getUserAction()
          && this.getGroupAction() == that.getGroupAction()
          && this.getOtherAction() == that.getOtherAction()
          && this.getStickyBit() == that.getStickyBit();
    }
    return false;
  }

  /**
   * Create a AbfsPermission from a abfs symbolic permission string
   * @param abfsSymbolicPermission e.g. "rw-rw-rw-+" / "rw-rw-rw-"
   */
  public static AbfsPermission valueOf(final String abfsSymbolicPermission, final boolean enableAclBit) {
    if(abfsSymbolicPermission == null) {
      return null;
    }

    final boolean isExtendedAcl = abfsSymbolicPermission.charAt(abfsSymbolicPermission.length() - 1) == '+';

    final String abfsRawSymbolicPermission = isExtendedAcl ? abfsSymbolicPermission.substring(0, abfsSymbolicPermission.length() - 1)
        : abfsSymbolicPermission;

    int n = 0;
    for(int i = 0; i < abfsRawSymbolicPermission.length(); i++) {
      n = n << 1;
      char c = abfsRawSymbolicPermission.charAt(i);
      n += (c == '-' || c == 'T' || c == 'S') ? 0: 1;
    }

    // Add sticky bit value if set
    if(abfsRawSymbolicPermission.charAt(abfsRawSymbolicPermission.length() - 1) == 't'
        || abfsRawSymbolicPermission.charAt(abfsRawSymbolicPermission.length() - 1) == 'T') {
      n += STICKY_BIT_OCTAL_VALUE;
    }


    return new AbfsPermission((short) n, isExtendedAcl && enableAclBit);
  }

  @Override
  public int hashCode() {
    return toShort();
  }
}

