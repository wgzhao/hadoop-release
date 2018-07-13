package org.apache.hadoop.fs.azurebfs.contracts.exceptions;

import org.apache.hadoop.classification.InterfaceAudience;

/**
 * Thrown if there is a problem instantiating a TokenAccessProvider or retrieving a configuration
 * using a TokenAccessProvider object.
 */
@InterfaceAudience.Private
public class TokenAccessProviderException extends AzureBlobFileSystemException {

    public TokenAccessProviderException(String message) {
        super(message);
    }

    public TokenAccessProviderException(String message, Throwable cause) {
        super(message);
    }
}