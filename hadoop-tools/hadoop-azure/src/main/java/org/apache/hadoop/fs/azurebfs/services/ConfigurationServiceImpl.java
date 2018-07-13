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

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.Map;

import com.google.common.annotations.VisibleForTesting;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys;
import org.apache.hadoop.fs.azurebfs.constants.FileSystemConfigurations;
import org.apache.hadoop.fs.azurebfs.contracts.annotations.ConfigurationValidationAnnotations.IntegerConfigurationValidatorAnnotation;
import org.apache.hadoop.fs.azurebfs.contracts.annotations.ConfigurationValidationAnnotations.LongConfigurationValidatorAnnotation;
import org.apache.hadoop.fs.azurebfs.contracts.annotations.ConfigurationValidationAnnotations.StringConfigurationValidatorAnnotation;
import org.apache.hadoop.fs.azurebfs.contracts.annotations.ConfigurationValidationAnnotations.Base64StringConfigurationValidatorAnnotation;
import org.apache.hadoop.fs.azurebfs.contracts.annotations.ConfigurationValidationAnnotations.BooleanConfigurationValidatorAnnotation;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.AzureBlobFileSystemException;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.ConfigurationPropertyNotFoundException;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.InvalidConfigurationValueException;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.KeyProviderException;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.TokenAccessProviderException;
import org.apache.hadoop.fs.azurebfs.contracts.services.ConfigurationService;
import org.apache.hadoop.fs.azurebfs.contracts.services.LoggingService;
import org.apache.hadoop.fs.azurebfs.diagnostics.Base64StringConfigurationBasicValidator;
import org.apache.hadoop.fs.azurebfs.diagnostics.BooleanConfigurationBasicValidator;
import org.apache.hadoop.fs.azurebfs.diagnostics.IntegerConfigurationBasicValidator;
import org.apache.hadoop.fs.azurebfs.diagnostics.LongConfigurationBasicValidator;
import org.apache.hadoop.fs.azurebfs.diagnostics.StringConfigurationBasicValidator;
import org.apache.hadoop.fs.azurebfs.oauth2.AccessTokenProvider;
import org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider;
import org.apache.hadoop.fs.azurebfs.oauth2.MsiTokenProvider;
import org.apache.hadoop.fs.azurebfs.oauth2.UserPasswordTokenProvider;

/**
 * This class is responsible to store and validate configuration values.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class ConfigurationServiceImpl implements ConfigurationService {
  private final Configuration configuration;
  private final LoggingService loggingService;
  private final boolean isSecure;

  @IntegerConfigurationValidatorAnnotation(ConfigurationKey = ConfigurationKeys.AZURE_WRITE_BUFFER_SIZE,
      MinValue = FileSystemConfigurations.MIN_BUFFER_SIZE,
      MaxValue = FileSystemConfigurations.MAX_BUFFER_SIZE,
      DefaultValue = FileSystemConfigurations.DEFAULT_WRITE_BUFFER_SIZE)
  private int writeBufferSize;

  @IntegerConfigurationValidatorAnnotation(ConfigurationKey = ConfigurationKeys.AZURE_READ_BUFFER_SIZE,
      MinValue = FileSystemConfigurations.MIN_BUFFER_SIZE,
      MaxValue = FileSystemConfigurations.MAX_BUFFER_SIZE,
      DefaultValue = FileSystemConfigurations.DEFAULT_READ_BUFFER_SIZE)
  private int readBufferSize;

  @IntegerConfigurationValidatorAnnotation(ConfigurationKey = ConfigurationKeys.AZURE_MIN_BACKOFF_INTERVAL,
      DefaultValue = FileSystemConfigurations.DEFAULT_MIN_BACKOFF_INTERVAL)
  private int minBackoffInterval;

  @IntegerConfigurationValidatorAnnotation(ConfigurationKey = ConfigurationKeys.AZURE_MAX_BACKOFF_INTERVAL,
      DefaultValue = FileSystemConfigurations.DEFAULT_MAX_BACKOFF_INTERVAL)
  private int maxBackoffInterval;

  @IntegerConfigurationValidatorAnnotation(ConfigurationKey = ConfigurationKeys.AZURE_BACKOFF_INTERVAL,
      DefaultValue = FileSystemConfigurations.DEFAULT_BACKOFF_INTERVAL)
  private int backoffInterval;

  @IntegerConfigurationValidatorAnnotation(ConfigurationKey = ConfigurationKeys.AZURE_MAX_IO_RETRIES,
      MinValue = 0,
      DefaultValue = FileSystemConfigurations.DEFAULT_MAX_RETRY_ATTEMPTS)
  private int maxIoRetries;

  @LongConfigurationValidatorAnnotation(ConfigurationKey = ConfigurationKeys.AZURE_BLOCK_SIZE_PROPERTY_NAME,
      MinValue = 0,
      MaxValue = FileSystemConfigurations.MAX_AZURE_BLOCK_SIZE,
      DefaultValue = FileSystemConfigurations.MAX_AZURE_BLOCK_SIZE)
  private long azureBlockSize;

  @StringConfigurationValidatorAnnotation(ConfigurationKey = ConfigurationKeys.AZURE_BLOCK_LOCATION_HOST_PROPERTY_NAME,
      DefaultValue = FileSystemConfigurations.AZURE_BLOCK_LOCATION_HOST_DEFAULT)
  private String azureBlockLocationHost;

  @IntegerConfigurationValidatorAnnotation(ConfigurationKey = ConfigurationKeys.AZURE_CONCURRENT_CONNECTION_VALUE_OUT,
      MinValue = 1,
      DefaultValue = FileSystemConfigurations.MAX_CONCURRENT_WRITE_THREADS)
  private int maxConcurrentWriteThreads;

  @IntegerConfigurationValidatorAnnotation(ConfigurationKey = ConfigurationKeys.AZURE_CONCURRENT_CONNECTION_VALUE_IN,
      MinValue = 1,
      DefaultValue = FileSystemConfigurations.MAX_CONCURRENT_READ_THREADS)
  private int maxConcurrentReadThreads;

  @BooleanConfigurationValidatorAnnotation(ConfigurationKey = ConfigurationKeys.AZURE_TOLERATE_CONCURRENT_APPEND,
      DefaultValue = FileSystemConfigurations.DEFAULT_READ_TOLERATE_CONCURRENT_APPEND)
  private boolean tolerateOobAppends;

  @StringConfigurationValidatorAnnotation(ConfigurationKey = ConfigurationKeys.FS_AZURE_ATOMIC_RENAME_KEY,
          DefaultValue = FileSystemConfigurations.DEFAULT_FS_AZURE_ATOMIC_RENAME_DIRECTORIES)
  private String azureAtomicDirs;

  @BooleanConfigurationValidatorAnnotation(ConfigurationKey = ConfigurationKeys.AZURE_CREATE_REMOTE_FILESYSTEM_DURING_INITIALIZATION,
      DefaultValue = FileSystemConfigurations.DEFAULT_AZURE_CREATE_REMOTE_FILESYSTEM_DURING_INITIALIZATION)
  private boolean createRemoteFileSystemDuringInitialization;

  @IntegerConfigurationValidatorAnnotation(ConfigurationKey = ConfigurationKeys.FS_AZURE_READ_AHEAD_QUEUE_DEPTH,
      DefaultValue = FileSystemConfigurations.DEFAULT_READ_AHEAD_QUEUE_DEPTH)
  private int readAheadQueueDepth;

  @BooleanConfigurationValidatorAnnotation(ConfigurationKey = ConfigurationKeys.FS_AZURE_ENABLE_FLUSH,
          DefaultValue = FileSystemConfigurations.DEFAULT_ENABLE_FLUSH)
  private boolean enableFlush;

  @StringConfigurationValidatorAnnotation(ConfigurationKey = ConfigurationKeys.FS_AZURE_USER_AGENT_PREFIX_KEY,
          DefaultValue = "")
  private String userAgentId;


  private Map<String, String> storageAccountKeys;

  public ConfigurationServiceImpl(final Configuration configuration, final LoggingService loggingService) throws IllegalAccessException,
      InvalidConfigurationValueException {
    this.configuration = configuration;
    this.loggingService = loggingService.get(ConfigurationService.class);
    this.isSecure = this.configuration.getBoolean(ConfigurationKeys.FS_AZURE_SECURE_MODE, false);

    validateStorageAccountKeys();

    Field[] fields = this.getClass().getDeclaredFields();
    for (Field field : fields) {
      field.setAccessible(true);
      if (field.isAnnotationPresent(IntegerConfigurationValidatorAnnotation.class)) {
        field.set(this, validateInt(field));
      } else if (field.isAnnotationPresent(LongConfigurationValidatorAnnotation.class)) {
        field.set(this, validateLong(field));
      } else if (field.isAnnotationPresent(StringConfigurationValidatorAnnotation.class)) {
        field.set(this, validateString(field));
      } else if (field.isAnnotationPresent(Base64StringConfigurationValidatorAnnotation.class)) {
        field.set(this, validateBase64String(field));
      } else if (field.isAnnotationPresent(BooleanConfigurationValidatorAnnotation.class)) {
        field.set(this, validateBoolean(field));
      }
    }
  }

  @Override
  public boolean isEmulator() {
    return this.getConfiguration().getBoolean(ConfigurationKeys.FS_AZURE_EMULATOR_ENABLED, false);
  }

  @Override
  public boolean isSecureMode() {
    return this.isSecure;
  }

  @Override
  public String getStorageAccountKey(final String accountName) throws AzureBlobFileSystemException {
    String key;
    String keyProviderClass =
        configuration.get(ConfigurationKeys.AZURE_KEY_ACCOUNT_KEYPROVIDER_PREFIX + accountName);
    KeyProvider keyProvider;

    if (keyProviderClass == null) {
      // No key provider was provided so use the provided key as is.
      keyProvider = new SimpleKeyProvider(loggingService);
    } else {
      // create an instance of the key provider class and verify it
      // implements KeyProvider
      Object keyProviderObject;
      try {
        Class<?> clazz = configuration.getClassByName(keyProviderClass);
        keyProviderObject = clazz.getDeclaredConstructor(LoggingService.class).newInstance(loggingService);
      } catch (Exception e) {
        throw new KeyProviderException("Unable to load key provider class.", e);
      }
      if (!(keyProviderObject instanceof KeyProvider)) {
        throw new KeyProviderException(keyProviderClass
            + " specified in config is not a valid KeyProvider class.");
      }
      keyProvider = (KeyProvider) keyProviderObject;
    }
    key = keyProvider.getStorageAccountKey(accountName, configuration);

    if (key == null) {
      throw new ConfigurationPropertyNotFoundException(accountName);
    }

    return key;
  }

  @Override
  public Configuration getConfiguration() {
    return this.configuration;
  }

  @Override
  public int getWriteBufferSize() {
    return this.writeBufferSize;
  }

  @Override
  public int getReadBufferSize() {
    return this.readBufferSize;
  }

  @Override
  public int getMinBackoffIntervalMilliseconds() {
    return this.minBackoffInterval;
  }

  @Override
  public int getMaxBackoffIntervalMilliseconds() {
    return this.maxBackoffInterval;
  }

  @Override
  public int getBackoffIntervalMilliseconds() {
    return this.backoffInterval;
  }

  @Override
  public int getMaxIoRetries() {
    return this.maxIoRetries;
  }

  @Override
  public long getAzureBlockSize() {
    return this.azureBlockSize;
  }

  @Override
  public String getAzureBlockLocationHost() {
    return this.azureBlockLocationHost;
  }

  @Override
  public int getMaxConcurrentWriteThreads() {
    return this.maxConcurrentWriteThreads;
  }

  @Override
  public int getMaxConcurrentReadThreads() {
    return this.maxConcurrentReadThreads;
  }

  @Override
  public boolean getTolerateOobAppends() {
    return this.tolerateOobAppends;
  }

  @Override
  public String getAzureAtomicRenameDirs() {
    return this.azureAtomicDirs;
  }

  @Override
  public boolean getCreateRemoteFileSystemDuringInitialization() {
    return this.createRemoteFileSystemDuringInitialization;
  }

  @Override
  public int getReadAheadQueueDepth() { return this.readAheadQueueDepth; }

  @Override
  public boolean isFlushEnabled() {
    return this.enableFlush;
  }

  @Override
  public String getCustomUserAgentPrefix() {
    return userAgentId;
  }

  @Override
    public AuthType getAuthType(final String accountName) {
        return configuration.getEnum(ConfigurationKeys.FS_AZURE_ACCOUNT_AUTH_TYPE_PROPERTY_NAME + accountName, AuthType.SharedKey);
    }

  @Override
  public AccessTokenProvider getTokenProvider(final String accountName) throws TokenAccessProviderException {

    AuthType authType = configuration.getEnum(ConfigurationKeys.FS_AZURE_ACCOUNT_AUTH_TYPE_PROPERTY_NAME + accountName, AuthType.SharedKey);
    if (authType != AuthType.OAuth) {
      throw new TokenAccessProviderException("invalid auth type");
    }

    try {
        Class<? extends AccessTokenProvider> tokenProviderClass =
                configuration.getClass(ConfigurationKeys.FS_AZURE_ACCOUNT_TOKEN_PROVIDER_TYPE_PROPERTY_NAME + accountName, null,
                        AccessTokenProvider.class);

        AccessTokenProvider tokenProvider = null;
        if(tokenProviderClass == ClientCredsTokenProvider.class) {
          String authEndpoint = getPasswordString(ConfigurationKeys.FS_AZURE_ACCOUNT_OAUTH_CLIENT_ENDPOINT + accountName);
          String clientId = getPasswordString(ConfigurationKeys.FS_AZURE_ACCOUNT_OAUTH_CLIENT_ID + accountName);
          String clientSecret = getPasswordString(ConfigurationKeys.FS_AZURE_ACCOUNT_OAUTH_CLIENT_SECRET + accountName);
          tokenProvider = new ClientCredsTokenProvider(loggingService, authEndpoint, clientId, clientSecret);
        }
        else if(tokenProviderClass == UserPasswordTokenProvider.class) {
          String clientId = getPasswordString(ConfigurationKeys.FS_AZURE_ACCOUNT_OAUTH_CLIENT_ENDPOINT + accountName);
          String username = getPasswordString(ConfigurationKeys.FS_AZURE_ACCOUNT_OAUTH_USER_NAME + accountName);
          String password = getPasswordString(ConfigurationKeys.FS_AZURE_ACCOUNT_OAUTH_USER_PASSWORD + accountName);
          tokenProvider = new UserPasswordTokenProvider(loggingService, clientId, username, password);
        }
        else if(tokenProviderClass == MsiTokenProvider.class) {
          String tenantGuid = getPasswordString(ConfigurationKeys.FS_AZURE_ACCOUNT_OAUTH_MSI_TENANT + accountName);
          String clientId = getPasswordString(ConfigurationKeys.FS_AZURE_ACCOUNT_OAUTH_CLIENT_ID + accountName);
          tokenProvider = new MsiTokenProvider(loggingService, tenantGuid, clientId);
        }
        else {
          throw new IllegalArgumentException("Failed to initialize " + tokenProviderClass);
        }

        return tokenProvider;
    } catch (Exception e) {
      throw new TokenAccessProviderException("Unable to load key provider class.", e);
    }
  }

  void validateStorageAccountKeys() throws InvalidConfigurationValueException {
    Base64StringConfigurationBasicValidator validator = new Base64StringConfigurationBasicValidator(
        ConfigurationKeys.FS_AZURE_ACCOUNT_KEY_PROPERTY_NAME, "", true);
    this.storageAccountKeys = this.configuration.getValByRegex(ConfigurationKeys.FS_AZURE_ACCOUNT_KEY_PROPERTY_NAME_REGX);

    for (String key : this.storageAccountKeys.keySet()) {
      validator.validate(storageAccountKeys.get(key));
    }
  }

  int validateInt(Field field) throws IllegalAccessException, InvalidConfigurationValueException {
    IntegerConfigurationValidatorAnnotation validator = field.getAnnotation(IntegerConfigurationValidatorAnnotation.class);
    String value = this.configuration.get(validator.ConfigurationKey());

    // validate
    return new IntegerConfigurationBasicValidator(
        validator.MinValue(),
        validator.MaxValue(),
        validator.DefaultValue(),
        validator.ConfigurationKey(),
        validator.ThrowIfInvalid()).validate(value);
  }

  long validateLong(Field field) throws IllegalAccessException, InvalidConfigurationValueException {
    LongConfigurationValidatorAnnotation validator = field.getAnnotation(LongConfigurationValidatorAnnotation.class);
    String value = this.configuration.get(validator.ConfigurationKey());

    // validate
    return new LongConfigurationBasicValidator(
        validator.MinValue(),
        validator.MaxValue(),
        validator.DefaultValue(),
        validator.ConfigurationKey(),
        validator.ThrowIfInvalid()).validate(value);
  }

  String validateString(Field field) throws IllegalAccessException, InvalidConfigurationValueException {
    StringConfigurationValidatorAnnotation validator = field.getAnnotation(StringConfigurationValidatorAnnotation.class);
    String value = this.configuration.get(validator.ConfigurationKey());

    // validate
    return new StringConfigurationBasicValidator(
        validator.ConfigurationKey(),
        validator.DefaultValue(),
        validator.ThrowIfInvalid()).validate(value);
  }

  String validateBase64String(Field field) throws IllegalAccessException, InvalidConfigurationValueException {
    Base64StringConfigurationValidatorAnnotation validator = field.getAnnotation((Base64StringConfigurationValidatorAnnotation.class));
    String value = this.configuration.get(validator.ConfigurationKey());

    // validate
    return new Base64StringConfigurationBasicValidator(
        validator.ConfigurationKey(),
        validator.DefaultValue(),
        validator.ThrowIfInvalid()).validate(value);
  }

  boolean validateBoolean(Field field) throws IllegalAccessException, InvalidConfigurationValueException {
    BooleanConfigurationValidatorAnnotation validator = field.getAnnotation(BooleanConfigurationValidatorAnnotation.class);
    String value = this.configuration.get(validator.ConfigurationKey());

    // validate
    return new BooleanConfigurationBasicValidator(
        validator.ConfigurationKey(),
        validator.DefaultValue(),
        validator.ThrowIfInvalid()).validate(value);
  }

  @VisibleForTesting
  void setReadBufferSize(int bufferSize) {
    this.readBufferSize = bufferSize;
  }

  @VisibleForTesting
  void setWriteBufferSize(int bufferSize) {
    this.writeBufferSize = bufferSize;
  }

  String getPasswordString(String key)
          throws IOException {
    char[] passchars = this.configuration.getPassword(key);
    if (passchars != null) {
      return new String(passchars);
    }

    return null;
  }
}