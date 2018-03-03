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
package org.apache.hadoop.fs.azuredfs;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.azure.NativeAzureFileSystem;
import org.apache.hadoop.conf.Configuration;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.assertFalse;
import static junit.framework.TestCase.assertTrue;


public class ITestWasbAdfsCompatibility extends DependencyInjectedTest {
  private WasbComponentHelper wasbComponentHelper;

  // native FS and adfs point to same container
  private final NativeAzureFileSystem nativeFs;
  private final AzureDistributedFileSystem adFs;

  private static final String WASB_TEST_CONTEXT = "wasb test file";
  private static final String ADFS_TEST_CONTEXT = "adfs test file";
  private static final String TEST_CONTEXT = "THIS IS FOR TEST";

  public ITestWasbAdfsCompatibility() throws Exception {
    super();
    wasbComponentHelper = new WasbComponentHelper();
    // create file system using native FS
    nativeFs = wasbComponentHelper.nativeFs;
    // point ADFS to the fs created by native fs
    Configuration adFsConfig = this.getConfiguration();
    String adfsUrl = wasbUrlToAdfsUrl(nativeFs.getUri().toString());
    adFsConfig.set("fs.defaultFS", adfsUrl);
    adFs = (AzureDistributedFileSystem) FileSystem.get(adFsConfig);
  }

  @Test
  public void testListFileStatus() throws Exception {
    // crate file using adfs
    Path path1 = new Path("/testfiles/~12/!008/3/adFsTestfile");
    FSDataOutputStream adfsStream = adFs.create(path1, true);
    adfsStream.write(ADFS_TEST_CONTEXT.getBytes());
    adfsStream.flush();
    adfsStream.hsync();
    adfsStream.close();

    // create file using wasb
    Path path2 = new Path("/testfiles/~12/!008/3/nativeFsTestfile");
    System.out.println(nativeFs.getUri());
    FSDataOutputStream nativeFsStream = nativeFs.create(path2, true);
    nativeFsStream.write(WASB_TEST_CONTEXT.getBytes());
    nativeFsStream.flush();
    nativeFsStream.hsync();
    nativeFsStream.close();
    // list file using adfs and wasb
    FileStatus[] adfsFileStatus = adFs.listStatus(new Path("/testfiles/~12/!008/3/"));
    FileStatus[] nativeFsFileStatus = nativeFs.listStatus(new Path("/testfiles/~12/!008/3/"));

    assertEquals(2, adfsFileStatus.length);
    assertEquals(2, nativeFsFileStatus.length);
  }

  @Test
  @Ignore("3/2/2018: Can not use WASB to consume file created by ADFS, a bug has been created to track this issue")
  public void testReadFile() throws IOException {
    boolean[] createFileWithAdfs = new boolean[]{false, true, false, true};
    boolean[] readFileWithAdfs = new boolean[]{false, true, true, false};
    FileSystem fs;
    BufferedReader br = null;
    for (int i = 0; i< 4; i++) {
      try {
        Path path = new Path("/testfiles/~12/!008/testfile" + i);
        if (createFileWithAdfs[i]) {
          fs = adFs;
        } else {
          fs = nativeFs;
        }

        // Write
        FSDataOutputStream nativeFsStream = fs.create(path,true);
        nativeFsStream.write(TEST_CONTEXT.getBytes());
        nativeFsStream.flush();
        nativeFsStream.hsync();
        nativeFsStream.close();

        // Check file status
        assertEquals(true, fs.exists(path));
        assertEquals(false, fs.getFileStatus(path).isDirectory());

        // Read
        if (readFileWithAdfs[i]) {
          fs = adFs;
        } else {
          fs = nativeFs;
        }
        FSDataInputStream inputStream = fs.open(path);
        br = new BufferedReader(new InputStreamReader(fs.open(path)));
        String line = br.readLine();
        assertEquals(TEST_CONTEXT, line);

        // Remove file
        fs.delete(path, true);
        assertFalse(fs.exists(path));
      } catch (Exception e) {
        e.printStackTrace();
      } finally {
        if (br != null) {
          br.close();
        }
      }
    }
  }

  @Test
  public void testDir() throws IOException {
    boolean[] createDirWithAdfs = new boolean[]{false, true, false, true};
    boolean[] readDirWithAdfs = new boolean[]{false, true, true, false};
    FileSystem fs;
    for (int i = 0; i < 4; i++) {
      Path path = new Path("/testDir/t" + i);
      //create
      if (createDirWithAdfs[i]) {
        fs = adFs;
      } else {
        fs = nativeFs;
      }
      assertTrue(fs.mkdirs(path));
      //check
      assertTrue(fs.exists(path));
      //read
      if (readDirWithAdfs[i]) {
        fs = adFs;
      } else {
        fs = nativeFs;
      }
      assertTrue(fs.exists(path));
      FileStatus dirStatus = fs.getFileStatus(path);
      assertTrue(dirStatus.isDirectory());
      fs.delete(path, true);
      assertFalse(fs.exists(path));
    }
  }


  @Test
  public void testUrlConversion(){
    String adfsUrl = "adfs://abcde-1111-1111-1111-1111@xxxx.dfs.xxx.xxx.xxxx.xxxx";
    String wabsUrl = "wasb://abcde-1111-1111-1111-1111@xxxx.blob.xxx.xxx.xxxx.xxxx";
    Assert.assertEquals(adfsUrl, wasbUrlToAdfsUrl(wabsUrl));
    Assert.assertEquals(wabsUrl, adfsUrlToWasbUrl(adfsUrl));
  }

  @Test
  public void testSetWorkingDirectory() throws IOException {
    //create folders
    assertTrue(adFs.mkdirs(new Path("/d1/d2/d3/d4")));

    //set working directory to path1
    Path path1 = new Path("/d1/d2");
    nativeFs.setWorkingDirectory(path1);
    adFs.setWorkingDirectory(path1);
    assertEquals(path1, nativeFs.getWorkingDirectory());
    assertEquals(path1, adFs.getWorkingDirectory());

    //set working directory to path2
    Path path2 = new Path("d3/d4");
    nativeFs.setWorkingDirectory(path2);
    adFs.setWorkingDirectory(path2);

    Path path3 = new Path("/d1/d2/d3/d4");
    assertEquals(path3, nativeFs.getWorkingDirectory());
    assertEquals(path3, adFs.getWorkingDirectory());
  }

  private static String wasbUrlToAdfsUrl(String wasbUrl) {
    String data = wasbUrl.replace("wasb://", "adfs://");
    data = data.replace(".blob.", ".dfs.");
    return data;
  }

  private static String adfsUrlToWasbUrl(String adfsUrl) {
    String data = adfsUrl.replace("adfs://", "wasb://");
    data = data.replace(".dfs.", ".blob.");
    return data;
  }
}