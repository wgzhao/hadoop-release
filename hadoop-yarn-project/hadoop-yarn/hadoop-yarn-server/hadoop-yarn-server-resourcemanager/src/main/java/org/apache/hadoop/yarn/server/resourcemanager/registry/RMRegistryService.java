/*
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

package org.apache.hadoop.yarn.server.resourcemanager.registry;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.io.DataInputByteBuffer;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.service.CompositeService;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.registry.server.integration.RMRegistryOperationsService;
import org.apache.hadoop.yarn.server.resourcemanager.RMAppManagerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.RMAppManagerEventType;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.RMStateStore;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.RMStateStoreAppEvent;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.RMStateStoreEvent;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.RMStateStoreEventType;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptEventType;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.event.RMAppAttemptContainerFinishedEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.event.RMAppAttemptRegistrationEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainerEventType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * This is the RM service which translates from RM events
 * to registry actions
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class RMRegistryService extends CompositeService {
  private static final Logger LOG =
      LoggerFactory.getLogger(RMRegistryService.class);

  private final RMContext rmContext;

  /**
   * Registry service
   */
  private final RMRegistryOperationsService registryOperations;

  public RMRegistryService(RMContext rmContext) {
    super(RMRegistryService.class.getName());
    this.rmContext = rmContext;

    registryOperations =
        new RMRegistryOperationsService("Registry");
    addService(registryOperations);
  }


  @Override
  protected void serviceStart() throws Exception {
    super.serviceStart();

    LOG.info("RM registry service started : {}",
        registryOperations.bindingDiagnosticDetails());
    // Register self as event handler for RM Events
    register(RMAppAttemptEventType.class, new AppEventHandler());
    register(RMAppManagerEventType.class, new AppManagerEventHandler());
    register(RMStateStoreEventType.class, new StateStoreEventHandler());
    register(RMContainerEventType.class,  new ContainerEventHandler());
  }

  /**
   * register a handler
   * @param eventType event type
   * @param handler handler
   */
  private void register(Class<? extends Enum> eventType,
      EventHandler handler) {
    rmContext.getDispatcher().register(eventType, handler);
  }

  @SuppressWarnings(
      {"EnumSwitchStatementWhichMissesCases", "UnnecessaryDefault"})
  protected void handleAppManagerEvent(RMAppManagerEvent event) throws
      IOException {
    RMAppManagerEventType eventType = event.getType();
    ApplicationId appId =
        event.getApplicationId();
    switch (eventType) {
      case APP_COMPLETED:
        registryOperations.onApplicationCompleted(appId);
        break;
      default:
        // this isn't in the enum today...just making sure for the
        // future
        break;
    }
  }

  @SuppressWarnings("EnumSwitchStatementWhichMissesCases")
  private void handleStateStoreEvent(RMStateStoreEvent event)
      throws IOException {
    RMStateStoreEventType eventType = event.getType();
    switch (eventType) {
      case STORE_APP:
        RMStateStoreAppEvent storeAppEvent = (RMStateStoreAppEvent) event;
        RMStateStore.ApplicationState appState = storeAppEvent.getAppState();
        ApplicationId appId = appState.getAppId();
        registryOperations.onStateStoreEvent(appId, appState.getUser());
        break;

      default:
        break;
    }
  }


  @SuppressWarnings("EnumSwitchStatementWhichMissesCases")
  protected void handleAppAttemptEvent(RMAppAttemptEvent event) throws
      IOException {
    RMAppAttemptEventType eventType = event.getType();
    ApplicationAttemptId appAttemptId =
        event.getApplicationAttemptId();

    ApplicationId appId = appAttemptId.getApplicationId();
    switch (eventType) {

      case LAUNCHED:
        registryOperations.onApplicationLaunched(appId);
        break;

      case REGISTERED:
        RMAppAttemptRegistrationEvent evt =
            (RMAppAttemptRegistrationEvent) event;
        registryOperations.onApplicationAttemptRegistered(appAttemptId,
            evt.getHost(),
            evt.getRpcport(),
            evt.getTrackingurl());
        break;

      case UNREGISTERED:
        registryOperations.onApplicationAttemptUnregistered(appAttemptId);
        break;

      case ATTEMPT_ADDED:
        registryOperations.onApplicationAttemptAdded(appAttemptId);
        break;

      // container has finished
      case CONTAINER_FINISHED:
        RMAppAttemptContainerFinishedEvent cfe =
            (RMAppAttemptContainerFinishedEvent) event;
        ContainerId containerId = cfe.getContainerStatus().getContainerId();
        registryOperations.onAMContainerFinished(containerId);
        break;


      default:
        // do nothing
    }
  }

  /**
   * Lifted from RMAppManager
   * @param application app submission
   * @return credentials
   * @throws IOException
   */
  private Credentials parseCredentials(ApplicationSubmissionContext application)
      throws IOException {
    Credentials credentials = new Credentials();
    DataInputByteBuffer dibb = new DataInputByteBuffer();
    ByteBuffer tokens = application.getAMContainerSpec().getTokens();
    if (tokens != null) {
      dibb.reset(tokens);
      credentials.readTokenStorageStream(dibb);
      tokens.rewind();
    }
    return credentials;
  }

  /**
   * Extract the information from the submission to set up the
   * registry permissions for a user
   * @param applicationId app in question
   * @return the credentials in the submission
   * @throws IOException problems parsing the credential
   */
  private Credentials extractCredentials(ApplicationId applicationId) throws
      IOException {
    RMApp rmApp = rmContext.getRMApps().get(applicationId);
    ApplicationSubmissionContext applicationSubmissionContext =
        rmApp.getApplicationSubmissionContext();

    if (UserGroupInformation.isSecurityEnabled()) {
      return parseCredentials(applicationSubmissionContext);
    } else {
      return null;
    }
  }

  @SuppressWarnings("EnumSwitchStatementWhichMissesCases")
  private void handleContainerEvent(RMContainerEvent event)
      throws IOException {
    RMContainerEventType eventType = event.getType();
    switch (eventType) {
      case FINISHED:
        ContainerId containerId = event.getContainerId();
        registryOperations.onContainerFinished(containerId);
        break;

      default:
        break;
    }
  }


  /**
   * Handler for app events
   */
  private class AppEventHandler implements
      EventHandler<RMAppAttemptEvent> {

    @Override
    public void handle(RMAppAttemptEvent event) {
      try {
        handleAppAttemptEvent(event);
      } catch (IOException e) {
        LOG.warn("handling {}: {}", event, e, e);
      }
    }
  }

  /**
   * Handler for RM-side App manager events
   */

  private class AppManagerEventHandler
      implements EventHandler<RMAppManagerEvent> {
    @Override
    public void handle(RMAppManagerEvent event) {
      try {
        handleAppManagerEvent(event);
      } catch (IOException e) {
        LOG.warn("handling {}: {}", event, e, e);
      }
    }
  }

  /**
   * Handler for RM-side state store events.
   * This happens early on, and as the data contains the user details,
   * it is where paths can be set up in advance of being used.
   */

  private class StateStoreEventHandler implements EventHandler<RMStateStoreEvent> {
    @Override
    public void handle(RMStateStoreEvent event) {
      try {
        handleStateStoreEvent(event);
      } catch (IOException e) {
        LOG.warn("handling {}: {}", event, e, e);
      }
    }
  }

  /**
   * Handler for RM-side container events
   */
  private class ContainerEventHandler implements EventHandler<RMContainerEvent> {

    @Override
    public void handle(RMContainerEvent event) {
      try {
        handleContainerEvent(event);
      } catch (IOException e) {
        LOG.warn("handling {}: {}", event, e, e);
      }
    }
  }


}
