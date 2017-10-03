
package org.apache.hadoop.fs.azuredfs.contracts.exceptions;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

/**
 * Base exception for any Azure Distributed Filesystem driver exceptions. All the exceptions must inherit this class.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public abstract class AzureDistributedFileSystemException extends Exception {
  public AzureDistributedFileSystemException(final String message) { super(message); }

  public AzureDistributedFileSystemException(final String message, final Exception innerException) { super(message, innerException); }

  @Override
  public String toString() {
    if (this.getMessage() == null && this.getCause() == null) {
      return null;
    }

    if (this.getCause() == null) {
      return this.getMessage();
    }

    if (this.getMessage() == null) {
      return this.getCause().toString();
    }

    return this.getMessage() + this.getCause().toString();
  }
}