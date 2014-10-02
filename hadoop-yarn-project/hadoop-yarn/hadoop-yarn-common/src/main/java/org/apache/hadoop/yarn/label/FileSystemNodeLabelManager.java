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

package org.apache.hadoop.yarn.label;

import java.io.EOFException;
import java.io.IOException;
import java.util.Collection;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.proto.YarnServerResourceManagerServiceProtos.AddLabelsRequestProto;
import org.apache.hadoop.yarn.proto.YarnServerResourceManagerServiceProtos.RemoveLabelsRequestProto;
import org.apache.hadoop.yarn.proto.YarnServerResourceManagerServiceProtos.SetNodeToLabelsRequestProto;
import org.apache.hadoop.yarn.server.api.protocolrecords.AddLabelsRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.RemoveLabelsRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.SetNodeToLabelsRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.impl.pb.AddLabelsRequestPBImpl;
import org.apache.hadoop.yarn.server.api.protocolrecords.impl.pb.RemoveLabelsRequestPBImpl;
import org.apache.hadoop.yarn.server.api.protocolrecords.impl.pb.SetNodeToLabelsRequestPBImpl;

import com.google.common.collect.Sets;

public class FileSystemNodeLabelManager extends NodeLabelManager {
  protected static final String ROOT_DIR_NAME = "FSNodeLabelManagerRoot";
  protected static final String MIRROR_FILENAME = "nodelabel.mirror";
  protected static final String EDITLOG_FILENAME = "nodelabel.editlog";

  Path fsWorkingPath;
  Path rootDirPath;
  FileSystem fs;
  FSDataOutputStream editlogOs;
  Path editLogPath;

  @Override
  protected void serviceInit(Configuration conf) throws Exception {
    fsWorkingPath =
        new Path(conf.get(YarnConfiguration.FS_NODE_LABEL_STORE_URI, "file:///tmp/"));
    rootDirPath = new Path(fsWorkingPath, ROOT_DIR_NAME);

    setFileSystem(conf);

    // mkdir of root dir path
    fs.mkdirs(rootDirPath);

    super.serviceInit(conf);
  }

  @Override
  protected void serviceStop() throws Exception {
    try {
      fs.close();
      editlogOs.close();
    } catch (Exception e) {
      LOG.warn("Exception happened whiling shutting down,", e);
    }

    super.serviceStop();
  }

  private void setFileSystem(Configuration conf) throws IOException {
    Configuration confCopy = new Configuration(conf);
    confCopy.setBoolean("dfs.client.retry.policy.enabled", true);
    String retryPolicy =
        confCopy.get(YarnConfiguration.FS_NODE_LABEL_STORE_RETRY_POLICY_SPEC,
            YarnConfiguration.DEFAULT_FS_NODE_LABEL_STORE_RETRY_POLICY_SPEC);
    confCopy.set("dfs.client.retry.policy.spec", retryPolicy);
    fs = fsWorkingPath.getFileSystem(confCopy);
    
    // if it's local file system, use RawLocalFileSystem instead of
    // LocalFileSystem, the latter one doesn't support append.
    if (fs.getScheme().equals("file")) {
      fs = ((LocalFileSystem)fs).getRaw();
    }
  }
  
  private void ensureAppendEditlogFile() throws IOException {
    editlogOs = fs.append(editLogPath);
  }
  
  private void ensureCloseEditlogFile() throws IOException {
    editlogOs.close();
  }

  @Override
  public void persistNodeToLabelsChanges(
      Map<String, Set<String>> nodeToLabels) throws IOException {
    ensureAppendEditlogFile();
    editlogOs.writeInt(SerializedLogType.NODE_TO_LABELS.ordinal());
    ((SetNodeToLabelsRequestPBImpl) SetNodeToLabelsRequest
        .newInstance(nodeToLabels)).getProto().writeDelimitedTo(editlogOs);
    ensureCloseEditlogFile();
  }

  @Override
  public void persistAddingLabels(Set<String> labels)
      throws IOException {
    ensureAppendEditlogFile();
    editlogOs.writeInt(SerializedLogType.ADD_LABELS.ordinal());
    ((AddLabelsRequestPBImpl) AddLabelsRequest.newInstance(labels)).getProto()
        .writeDelimitedTo(editlogOs);
    ensureCloseEditlogFile();
  }

  @Override
  public void persistRemovingLabels(Collection<String> labels)
      throws IOException {
    ensureAppendEditlogFile();
    editlogOs.writeInt(SerializedLogType.REMOVE_LABELS.ordinal());
    ((RemoveLabelsRequestPBImpl) RemoveLabelsRequest.newInstance(Sets
        .newHashSet(labels.iterator()))).getProto().writeDelimitedTo(editlogOs);
    ensureCloseEditlogFile();
  }

  @Override
  public void recover() throws IOException {
    /*
     * Steps of recover
     * 1) Read from last mirror (from mirror or mirror.old)
     * 2) Read from last edit log, and apply such edit log
     * 3) Write new mirror to mirror.writing
     * 4) Rename mirror to mirror.old
     * 5) Move mirror.writing to mirror
     * 6) Remove mirror.old
     * 7) Remove edit log and create a new empty edit log 
     */
    
    // Open mirror from serialized file
    Path mirrorPath = new Path(rootDirPath, MIRROR_FILENAME);
    Path oldMirrorPath = new Path(rootDirPath, MIRROR_FILENAME + ".old");

    FSDataInputStream is = null;
    if (fs.exists(mirrorPath)) {
      is = fs.open(mirrorPath);
    } else if (fs.exists(oldMirrorPath)) {
      is = fs.open(oldMirrorPath);
    }

    if (null != is) {
      Set<String> labels =
          new AddLabelsRequestPBImpl(
              AddLabelsRequestProto.parseDelimitedFrom(is)).getLabels();
      Map<String, Set<String>> nodeToLabels =
          new SetNodeToLabelsRequestPBImpl(
              SetNodeToLabelsRequestProto.parseDelimitedFrom(is))
              .getNodeToLabels();
      addLabels(labels);
      setLabelsOnMultipleNodes(nodeToLabels);
      is.close();
    }

    // Open and process editlog
    editLogPath = new Path(rootDirPath, EDITLOG_FILENAME);
    if (fs.exists(editLogPath)) {
      is = fs.open(editLogPath);

      while (true) {
        try {
          // read edit log one by one
          SerializedLogType type = SerializedLogType.values()[is.readInt()];
          
          switch (type) {
          case ADD_LABELS: {
            Collection<String> partitions =
                AddLabelsRequestProto.parseDelimitedFrom(is)
                    .getLabelsList();
            addLabels(Sets.newHashSet(partitions.iterator()));
            break;
          }
          case REMOVE_LABELS: {
            Collection<String> partitions =
                RemoveLabelsRequestProto.parseDelimitedFrom(is)
                    .getLabelsList();
            removeLabels(partitions);
            break;
          }
          case NODE_TO_LABELS: {
            Map<String, Set<String>> map =
                new SetNodeToLabelsRequestPBImpl(
                    SetNodeToLabelsRequestProto.parseDelimitedFrom(is))
                    .getNodeToLabels();
            setLabelsOnMultipleNodes(map);
            break;
          }
          }
        } catch (EOFException e) {
          // EOF hit, break
          break;
        }
      }
    }

    // Serialize current mirror to mirror.writing
    Path writingMirrorPath = new Path(rootDirPath, MIRROR_FILENAME + ".writing");
    FSDataOutputStream os = fs.create(writingMirrorPath, true);
    ((AddLabelsRequestPBImpl) AddLabelsRequestPBImpl
        .newInstance(super.existingLabels)).getProto().writeDelimitedTo(os);
    ((SetNodeToLabelsRequestPBImpl) SetNodeToLabelsRequest
        .newInstance(super.nodeToLabels)).getProto().writeDelimitedTo(os);
    os.close();
    
    // Move mirror to mirror.old
    if (fs.exists(mirrorPath)) {
      fs.delete(oldMirrorPath, false);
      fs.rename(mirrorPath, oldMirrorPath);
    }
    
    // move mirror.writing to mirror
    fs.rename(writingMirrorPath, mirrorPath);
    fs.delete(writingMirrorPath, false);
    
    // remove mirror.old
    fs.delete(oldMirrorPath, false);
    
    // create a new editlog file
    editlogOs = fs.create(editLogPath, true);
    editlogOs.close();
    
    LOG.info("Finished write mirror at:" + mirrorPath.toString());
    LOG.info("Finished create editlog file at:" + editLogPath.toString());
  }
}
