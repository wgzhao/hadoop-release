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

import java.lang.reflect.Field;

import static org.junit.Assert.*;
import org.junit.Test;

import org.apache.commons.net.util.Base64;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.azuredfs.DependencyInjectedTest;
import org.apache.hadoop.fs.azuredfs.contracts.annotations.ConfigurationValidationAnnotations.*;
import static org.apache.hadoop.fs.azuredfs.constants.FileSystemConfigurations.*;

public class TestConfigurationServiceFieldsValidation extends DependencyInjectedTest  {
  private ConfigurationServiceImpl configService;

  private static final String INT_KEY= "intKey";
  private static final String LONG_KEY= "longKey";
  private static final String STRING_KEY= "stringKey";
  private static final String BASE64_KEY= "base64Key";
  private static final String BOOLEAN_KEY= "booleanKey";
  private static final int DEFAULT_INT = 4194304;
  private static final int DEFAULT_LONG = 4194304;
  private final String encodedString;

  @IntegerConfigurationValidatorAnnotation(ConfigurationKey = INT_KEY,
      MinValue = Integer.MIN_VALUE,
      MaxValue = Integer.MAX_VALUE,
      DefaultValue = DEFAULT_INT)
  private int intField;

  @LongConfigurationValidatorAnnotation(ConfigurationKey = LONG_KEY,
      MinValue = Long.MIN_VALUE,
      MaxValue = Long.MAX_VALUE,
      DefaultValue = DEFAULT_LONG)
  private int longField;

  @StringConfigurationValidatorAnnotation(ConfigurationKey = STRING_KEY,
  DefaultValue = "default")
  private String stringField;

  @Base64StringConfigurationValidatorAnnotation(ConfigurationKey = BASE64_KEY,
  DefaultValue = "base64")
  private String base64Field;

  @BooleanConfigurationValidatorAnnotation(ConfigurationKey = BOOLEAN_KEY,
  DefaultValue = false)
  private boolean boolField;

  public TestConfigurationServiceFieldsValidation() throws Exception {
    super();
    this.encodedString = Base64.encodeBase64String("base64Value".getBytes());
    Configuration configuration = new Configuration();
    configuration.set(INT_KEY, "1234565");
    configuration.set(LONG_KEY, "4194304");
    configuration.set(STRING_KEY, "stringValue");
    configuration.set(BASE64_KEY, encodedString);
    configuration.set(BOOLEAN_KEY, "true");
    configService = new ConfigurationServiceImpl(configuration);
  }

  @Test
  public void testValidateFunctionsInConfigServiceImpl() throws Exception {
    Field[] fields = this.getClass().getDeclaredFields();
    for (Field field : fields) {
      field.setAccessible(true);
      if (field.isAnnotationPresent(IntegerConfigurationValidatorAnnotation.class)) {
        assertEquals(1234565, configService.validateInt(field));
      } else if (field.isAnnotationPresent(LongConfigurationValidatorAnnotation.class)) {
        assertEquals(DEFAULT_LONG, configService.validateLong(field));
      } else if (field.isAnnotationPresent(StringConfigurationValidatorAnnotation.class)) {
        assertEquals("stringValue", configService.validateString(field));
      } else if (field.isAnnotationPresent(Base64StringConfigurationValidatorAnnotation.class)) {
        assertEquals(this.encodedString , configService.validateBase64String(field));
      } else if (field.isAnnotationPresent(BooleanConfigurationValidatorAnnotation.class)) {
        assertEquals(true, configService.validateBoolean(field));
      }
    }
  }

  @Test
  public void testConfigServiceImplAnnotatedFieldsInitialized() throws Exception {
    // test that all the ConfigurationServiceImpl annotated fields have been initialized in the constructor
    assertEquals(DEFAULT_WRITE_BUFFER_SIZE, configService.getWriteBufferSize());
    assertEquals(DEFAULT_READ_BUFFER_SIZE, configService.getReadBufferSize());
    assertEquals(DEFAULT_MIN_BACKOFF_INTERVAL, configService.getMinBackoffInterval());
    assertEquals(DEFAULT_MAX_BACKOFF_INTERVAL, configService.getMaxBackoffInterval());
    assertEquals(DEFAULT_BACKOFF_INTERVAL, configService.getBackoffInterval());
    assertEquals(DEFAULT_MAX_RETRY_ATTEMPTS, configService.getMaxIoRetries());
    assertEquals(MAX_AZURE_BLOCK_SIZE, configService.getAzureBlockSize());
    assertEquals(AZURE_BLOCK_LOCATION_HOST_DEFAULT, configService.getAzureBlockLocationHost());
  }
}
