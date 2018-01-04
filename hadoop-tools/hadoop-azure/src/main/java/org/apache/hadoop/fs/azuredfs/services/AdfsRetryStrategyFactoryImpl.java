package org.apache.hadoop.fs.azuredfs.services;

import javax.inject.Inject;
import javax.inject.Singleton;

import com.microsoft.rest.retry.ExponentialBackoffRetryStrategy;
import com.microsoft.rest.retry.RetryStrategy;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.azuredfs.contracts.exceptions.AzureDistributedFileSystemException;
import org.apache.hadoop.fs.azuredfs.contracts.services.ConfigurationService;
import org.apache.hadoop.fs.azuredfs.contracts.services.AdfsRetryStrategyFactory;

@Singleton
@InterfaceAudience.Public
@InterfaceStability.Evolving
class AdfsRetryStrategyFactoryImpl implements AdfsRetryStrategyFactory{
  private final ConfigurationService configurationService;

  @Inject
  public AdfsRetryStrategyFactoryImpl(final ConfigurationService configurationService) {
    this.configurationService = configurationService;
  }

  @Override
  public RetryStrategy create() throws AzureDistributedFileSystemException {
    return new ExponentialBackoffRetryStrategy(
        configurationService.getMaxIoRetries(),
        configurationService.getMinBackoffIntervalMilliseconds(),
        configurationService.getMaxBackoffIntervalMilliseconds(),
        configurationService.getBackoffIntervalMilliseconds());
  }
}
