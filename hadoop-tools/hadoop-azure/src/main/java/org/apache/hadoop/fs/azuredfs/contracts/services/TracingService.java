
package org.apache.hadoop.fs.azuredfs.contracts.services;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.azuredfs.contracts.exceptions.AzureDistributedFileSystemException;
import org.apache.htrace.core.SpanId;
import org.apache.htrace.core.TraceScope;

/**
 * Azure Distributed Filesystem tracing service.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public interface TracingService extends InjectableService {
  /**
   * Creates a {@link TraceScope} object with the provided description.
   * @param description the trace description.
   * @return created traceScope.
   */
  TraceScope traceBegin(final String description);

  /**
   * Creates a {@link TraceScope} object with the provided description.
   * @param description the trace description.
   * @param parentSpanId the span id of the parent trace scope.
   * @return create traceScope
   */
  TraceScope traceBegin(final String description, SpanId parentSpanId);

  /**
   * Appends the provided exception to the trace scope.
   * @param traceScope the scope which exception needs to be attached to.
   * @param azureDistributedFileSystemException the exception to be attached to the scope.
   */
  void traceException(TraceScope traceScope, AzureDistributedFileSystemException azureDistributedFileSystemException);

  /**
   * Ends the provided traceScope.
   * @param traceScope the scope that needs to be ended.
   */
  void traceEnd(TraceScope traceScope);
}