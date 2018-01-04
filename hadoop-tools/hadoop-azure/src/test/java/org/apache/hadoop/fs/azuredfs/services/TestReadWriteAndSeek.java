package org.apache.hadoop.fs.azuredfs.services;

import java.util.Random;

import org.junit.Test;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.azuredfs.AzureDistributedFileSystem;
import org.apache.hadoop.fs.azuredfs.DependencyInjectedTest;
import org.apache.hadoop.fs.azuredfs.contracts.services.ConfigurationService;

import static org.apache.hadoop.fs.azuredfs.constants.FileSystemConfigurations.*;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertNotEquals;

public class TestReadWriteAndSeek extends DependencyInjectedTest {
  public TestReadWriteAndSeek() throws Exception {
    super();
    this.mockServiceInjector.replaceProvider(ConfigurationService.class, ConfigurationServiceImpl.class);
  }

  @Test
  public void testReadAndWriteWithDifferentBufferSizesAndSeek() throws Exception {
    testReadWriteAndSeek(MIN_BUFFER_SIZE);
    testReadWriteAndSeek(DEFAULT_READ_BUFFER_SIZE);
    testReadWriteAndSeek(MAX_BUFFER_SIZE);
  }

  private void testReadWriteAndSeek(int bufferSize) throws Exception {
    final AzureDistributedFileSystem fs = (AzureDistributedFileSystem) FileSystem.get(this.getConfiguration());
    fs.create(new Path("testfile"));
    ConfigurationServiceImpl configurationservice = (ConfigurationServiceImpl) ServiceProviderImpl.instance().get(ConfigurationService.class);
    configurationservice.setWriteBufferSize(bufferSize);
    configurationservice.setReadBufferSize(bufferSize);

    final FSDataOutputStream stream = fs.create(new Path("testfile"));

    final byte[] b = new byte[2 * bufferSize];
    new Random().nextBytes(b);
    stream.write(b);
    stream.close();

    final byte[] r = new byte[2 * bufferSize];
    final FSDataInputStream inputStream = fs.open(new Path("testfile"));
    inputStream.seek(bufferSize);
    int result = inputStream.read(r, bufferSize, bufferSize);
    assertNotEquals(-1, result);

    inputStream.seek(0);
    result = inputStream.read(r, 0, bufferSize);
    assertNotEquals(-1, result);
    assertArrayEquals(r, b);
  }
}
