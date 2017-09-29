package org.apache.hadoop.fs.azuredfs.services.mocks;

import java.io.IOException;
import java.security.InvalidKeyException;

import com.microsoft.azure.storage.StorageException;
import okhttp3.Request;

import org.apache.hadoop.fs.azuredfs.contracts.services.AzureAuthorizationService;

public class MockAzureAuthorizationServiceImpl implements AzureAuthorizationService {
  @Override
  public Request updateRequestWithAuthorizationHeader(Request request) throws IOException, InvalidKeyException, StorageException {
    return null;
  }
}
