package org.apache.hadoop.fs.azuredfs.contracts.services;

import com.microsoft.rest.retry.RetryStrategy;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.azuredfs.contracts.exceptions.AzureDistributedFileSystemException;

/**
 * ExponentialBackoffRetryStrategy factory.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public interface AdfsRetryStrategyFactory extends InjectableService {
  /**
   * Creates and configures an instance of ExponentialBackoffRetryStrategy
   * @return ExponentialBackoffRetryStrategy instance
   */
  RetryStrategy create() throws AzureDistributedFileSystemException;
}

