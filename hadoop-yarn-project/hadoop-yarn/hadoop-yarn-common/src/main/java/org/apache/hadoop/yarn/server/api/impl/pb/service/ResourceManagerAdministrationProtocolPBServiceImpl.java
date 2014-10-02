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

package org.apache.hadoop.yarn.server.api.impl.pb.service;

import java.io.IOException;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.proto.YarnServerResourceManagerServiceProtos.AddLabelsRequestProto;
import org.apache.hadoop.yarn.proto.YarnServerResourceManagerServiceProtos.AddLabelsResponseProto;
import org.apache.hadoop.yarn.proto.YarnServerResourceManagerServiceProtos.ClearAllLabelsRequestProto;
import org.apache.hadoop.yarn.proto.YarnServerResourceManagerServiceProtos.ClearAllLabelsResponseProto;
import org.apache.hadoop.yarn.proto.YarnServerResourceManagerServiceProtos.GetGroupsForUserRequestProto;
import org.apache.hadoop.yarn.proto.YarnServerResourceManagerServiceProtos.GetGroupsForUserResponseProto;
import org.apache.hadoop.yarn.proto.YarnServerResourceManagerServiceProtos.GetLabelsRequestProto;
import org.apache.hadoop.yarn.proto.YarnServerResourceManagerServiceProtos.GetLabelsResponseProto;
import org.apache.hadoop.yarn.proto.YarnServerResourceManagerServiceProtos.GetNodeToLabelsRequestProto;
import org.apache.hadoop.yarn.proto.YarnServerResourceManagerServiceProtos.GetNodeToLabelsResponseProto;
import org.apache.hadoop.yarn.proto.YarnServerResourceManagerServiceProtos.RefreshAdminAclsRequestProto;
import org.apache.hadoop.yarn.proto.YarnServerResourceManagerServiceProtos.RefreshAdminAclsResponseProto;
import org.apache.hadoop.yarn.proto.YarnServerResourceManagerServiceProtos.RefreshNodesRequestProto;
import org.apache.hadoop.yarn.proto.YarnServerResourceManagerServiceProtos.RefreshNodesResponseProto;
import org.apache.hadoop.yarn.proto.YarnServerResourceManagerServiceProtos.RefreshQueuesRequestProto;
import org.apache.hadoop.yarn.proto.YarnServerResourceManagerServiceProtos.RefreshQueuesResponseProto;
import org.apache.hadoop.yarn.proto.YarnServerResourceManagerServiceProtos.RefreshServiceAclsRequestProto;
import org.apache.hadoop.yarn.proto.YarnServerResourceManagerServiceProtos.RefreshServiceAclsResponseProto;
import org.apache.hadoop.yarn.proto.YarnServerResourceManagerServiceProtos.RefreshSuperUserGroupsConfigurationRequestProto;
import org.apache.hadoop.yarn.proto.YarnServerResourceManagerServiceProtos.RefreshSuperUserGroupsConfigurationResponseProto;
import org.apache.hadoop.yarn.proto.YarnServerResourceManagerServiceProtos.RefreshUserToGroupsMappingsRequestProto;
import org.apache.hadoop.yarn.proto.YarnServerResourceManagerServiceProtos.RefreshUserToGroupsMappingsResponseProto;
import org.apache.hadoop.yarn.proto.YarnServerResourceManagerServiceProtos.RemoveLabelsRequestProto;
import org.apache.hadoop.yarn.proto.YarnServerResourceManagerServiceProtos.RemoveLabelsResponseProto;
import org.apache.hadoop.yarn.proto.YarnServerResourceManagerServiceProtos.SetNodeToLabelsRequestProto;
import org.apache.hadoop.yarn.proto.YarnServerResourceManagerServiceProtos.SetNodeToLabelsResponseProto;
import org.apache.hadoop.yarn.proto.YarnServerResourceManagerServiceProtos.UpdateNodeResourceRequestProto;
import org.apache.hadoop.yarn.proto.YarnServerResourceManagerServiceProtos.UpdateNodeResourceResponseProto;
import org.apache.hadoop.yarn.server.api.ResourceManagerAdministrationProtocol;
import org.apache.hadoop.yarn.server.api.ResourceManagerAdministrationProtocolPB;
import org.apache.hadoop.yarn.server.api.protocolrecords.AddLabelsResponse;
import org.apache.hadoop.yarn.server.api.protocolrecords.ClearAllLabelsResponse;
import org.apache.hadoop.yarn.server.api.protocolrecords.GetLabelsResponse;
import org.apache.hadoop.yarn.server.api.protocolrecords.GetNodeToLabelsResponse;
import org.apache.hadoop.yarn.server.api.protocolrecords.RefreshAdminAclsResponse;
import org.apache.hadoop.yarn.server.api.protocolrecords.RefreshNodesResponse;
import org.apache.hadoop.yarn.server.api.protocolrecords.RefreshQueuesResponse;
import org.apache.hadoop.yarn.server.api.protocolrecords.RefreshServiceAclsResponse;
import org.apache.hadoop.yarn.server.api.protocolrecords.RefreshSuperUserGroupsConfigurationResponse;
import org.apache.hadoop.yarn.server.api.protocolrecords.RefreshUserToGroupsMappingsResponse;
import org.apache.hadoop.yarn.server.api.protocolrecords.RemoveLabelsResponse;
import org.apache.hadoop.yarn.server.api.protocolrecords.SetNodeToLabelsResponse;
import org.apache.hadoop.yarn.server.api.protocolrecords.UpdateNodeResourceResponse;
import org.apache.hadoop.yarn.server.api.protocolrecords.impl.pb.AddLabelsRequestPBImpl;
import org.apache.hadoop.yarn.server.api.protocolrecords.impl.pb.AddLabelsResponsePBImpl;
import org.apache.hadoop.yarn.server.api.protocolrecords.impl.pb.ClearAllLabelsRequestPBImpl;
import org.apache.hadoop.yarn.server.api.protocolrecords.impl.pb.ClearAllLabelsResponsePBImpl;
import org.apache.hadoop.yarn.server.api.protocolrecords.impl.pb.GetLabelsRequestPBImpl;
import org.apache.hadoop.yarn.server.api.protocolrecords.impl.pb.GetLabelsResponsePBImpl;
import org.apache.hadoop.yarn.server.api.protocolrecords.impl.pb.GetNodeToLabelsRequestPBImpl;
import org.apache.hadoop.yarn.server.api.protocolrecords.impl.pb.GetNodeToLabelsResponsePBImpl;
import org.apache.hadoop.yarn.server.api.protocolrecords.impl.pb.RefreshAdminAclsRequestPBImpl;
import org.apache.hadoop.yarn.server.api.protocolrecords.impl.pb.RefreshAdminAclsResponsePBImpl;
import org.apache.hadoop.yarn.server.api.protocolrecords.impl.pb.RefreshNodesRequestPBImpl;
import org.apache.hadoop.yarn.server.api.protocolrecords.impl.pb.RefreshNodesResponsePBImpl;
import org.apache.hadoop.yarn.server.api.protocolrecords.impl.pb.RefreshQueuesRequestPBImpl;
import org.apache.hadoop.yarn.server.api.protocolrecords.impl.pb.RefreshQueuesResponsePBImpl;
import org.apache.hadoop.yarn.server.api.protocolrecords.impl.pb.RefreshServiceAclsRequestPBImpl;
import org.apache.hadoop.yarn.server.api.protocolrecords.impl.pb.RefreshServiceAclsResponsePBImpl;
import org.apache.hadoop.yarn.server.api.protocolrecords.impl.pb.RefreshSuperUserGroupsConfigurationRequestPBImpl;
import org.apache.hadoop.yarn.server.api.protocolrecords.impl.pb.RefreshSuperUserGroupsConfigurationResponsePBImpl;
import org.apache.hadoop.yarn.server.api.protocolrecords.impl.pb.RefreshUserToGroupsMappingsRequestPBImpl;
import org.apache.hadoop.yarn.server.api.protocolrecords.impl.pb.RefreshUserToGroupsMappingsResponsePBImpl;
import org.apache.hadoop.yarn.server.api.protocolrecords.impl.pb.RemoveLabelsRequestPBImpl;
import org.apache.hadoop.yarn.server.api.protocolrecords.impl.pb.RemoveLabelsResponsePBImpl;
import org.apache.hadoop.yarn.server.api.protocolrecords.impl.pb.SetNodeToLabelsRequestPBImpl;
import org.apache.hadoop.yarn.server.api.protocolrecords.impl.pb.SetNodeToLabelsResponsePBImpl;
import org.apache.hadoop.yarn.server.api.protocolrecords.impl.pb.UpdateNodeResourceRequestPBImpl;
import org.apache.hadoop.yarn.server.api.protocolrecords.impl.pb.UpdateNodeResourceResponsePBImpl;

import com.google.protobuf.RpcController;
import com.google.protobuf.ServiceException;

@Private
public class ResourceManagerAdministrationProtocolPBServiceImpl implements ResourceManagerAdministrationProtocolPB {

  private ResourceManagerAdministrationProtocol real;
  
  public ResourceManagerAdministrationProtocolPBServiceImpl(ResourceManagerAdministrationProtocol impl) {
    this.real = impl;
  }
  
  @Override
  public RefreshQueuesResponseProto refreshQueues(RpcController controller,
      RefreshQueuesRequestProto proto) throws ServiceException {
    RefreshQueuesRequestPBImpl request = new RefreshQueuesRequestPBImpl(proto);
    try {
      RefreshQueuesResponse response = real.refreshQueues(request);
      return ((RefreshQueuesResponsePBImpl)response).getProto();
    } catch (YarnException e) {
      throw new ServiceException(e);
    } catch (IOException e) {
      throw new ServiceException(e);
    }
  }

  @Override
  public RefreshAdminAclsResponseProto refreshAdminAcls(
      RpcController controller, RefreshAdminAclsRequestProto proto)
      throws ServiceException {
    RefreshAdminAclsRequestPBImpl request = 
      new RefreshAdminAclsRequestPBImpl(proto);
    try {
      RefreshAdminAclsResponse response = real.refreshAdminAcls(request);
      return ((RefreshAdminAclsResponsePBImpl)response).getProto();
    } catch (YarnException e) {
      throw new ServiceException(e);
    } catch (IOException e) {
      throw new ServiceException(e);
    }
  }

  @Override
  public RefreshNodesResponseProto refreshNodes(RpcController controller,
      RefreshNodesRequestProto proto) throws ServiceException {
    RefreshNodesRequestPBImpl request = new RefreshNodesRequestPBImpl(proto);
    try {
      RefreshNodesResponse response = real.refreshNodes(request);
      return ((RefreshNodesResponsePBImpl)response).getProto();
    } catch (YarnException e) {
      throw new ServiceException(e);
    } catch (IOException e) {
      throw new ServiceException(e);
    }
  }

  @Override
  public RefreshSuperUserGroupsConfigurationResponseProto 
  refreshSuperUserGroupsConfiguration(
      RpcController controller,
      RefreshSuperUserGroupsConfigurationRequestProto proto)
      throws ServiceException {
    RefreshSuperUserGroupsConfigurationRequestPBImpl request = 
      new RefreshSuperUserGroupsConfigurationRequestPBImpl(proto);
    try {
      RefreshSuperUserGroupsConfigurationResponse response = 
        real.refreshSuperUserGroupsConfiguration(request);
      return ((RefreshSuperUserGroupsConfigurationResponsePBImpl)response).getProto();
    } catch (YarnException e) {
      throw new ServiceException(e);
    } catch (IOException e) {
      throw new ServiceException(e);
    }
  }

  @Override
  public RefreshUserToGroupsMappingsResponseProto refreshUserToGroupsMappings(
      RpcController controller, RefreshUserToGroupsMappingsRequestProto proto)
      throws ServiceException {
    RefreshUserToGroupsMappingsRequestPBImpl request = 
      new RefreshUserToGroupsMappingsRequestPBImpl(proto);
    try {
      RefreshUserToGroupsMappingsResponse response = 
        real.refreshUserToGroupsMappings(request);
      return ((RefreshUserToGroupsMappingsResponsePBImpl)response).getProto();
    } catch (YarnException e) {
      throw new ServiceException(e);
    } catch (IOException e) {
      throw new ServiceException(e);
    }
  }

  @Override
  public RefreshServiceAclsResponseProto refreshServiceAcls(
      RpcController controller, RefreshServiceAclsRequestProto proto)
      throws ServiceException {
    RefreshServiceAclsRequestPBImpl request = 
        new RefreshServiceAclsRequestPBImpl(proto);
      try {
        RefreshServiceAclsResponse response = 
          real.refreshServiceAcls(request);
        return ((RefreshServiceAclsResponsePBImpl)response).getProto();
      } catch (YarnException e) {
        throw new ServiceException(e);
      } catch (IOException e) {
        throw new ServiceException(e);
      }
  }

  @Override
  public GetGroupsForUserResponseProto getGroupsForUser(
      RpcController controller, GetGroupsForUserRequestProto request)
      throws ServiceException {
    String user = request.getUser();
    try {
      String[] groups = real.getGroupsForUser(user);
      GetGroupsForUserResponseProto.Builder responseBuilder =
          GetGroupsForUserResponseProto.newBuilder();
      for (String group : groups) {
        responseBuilder.addGroups(group);
      }
      return responseBuilder.build();
    } catch (IOException e) {
      throw new ServiceException(e);
    }
  }
  
  @Override
  public UpdateNodeResourceResponseProto updateNodeResource(RpcController controller,
      UpdateNodeResourceRequestProto proto) throws ServiceException {
    UpdateNodeResourceRequestPBImpl request = 
        new UpdateNodeResourceRequestPBImpl(proto);
    try {
      UpdateNodeResourceResponse response = real.updateNodeResource(request);
      return ((UpdateNodeResourceResponsePBImpl)response).getProto();
    } catch (YarnException e) {
      throw new ServiceException(e);
    } catch (IOException e) {
      throw new ServiceException(e);
    }
  }

  @Override
  public AddLabelsResponseProto addLabels(RpcController controller,
      AddLabelsRequestProto proto) throws ServiceException {
    AddLabelsRequestPBImpl request = new AddLabelsRequestPBImpl(proto);
    try {
      AddLabelsResponse response = real.addLabels(request);
      return ((AddLabelsResponsePBImpl) response).getProto();
    } catch (YarnException e) {
      throw new ServiceException(e);
    } catch (IOException e) {
      throw new ServiceException(e);
    }
  }

  @Override
  public RemoveLabelsResponseProto removeLabels(
      RpcController controller, RemoveLabelsRequestProto proto)
      throws ServiceException {
    RemoveLabelsRequestPBImpl request =
        new RemoveLabelsRequestPBImpl(proto);
    try {
      RemoveLabelsResponse response = real.removeLabels(request);
      return ((RemoveLabelsResponsePBImpl) response).getProto();
    } catch (YarnException e) {
      throw new ServiceException(e);
    } catch (IOException e) {
      throw new ServiceException(e);
    }
  }

  @Override
  public SetNodeToLabelsResponseProto setNodeToLabels(
      RpcController controller, SetNodeToLabelsRequestProto proto)
      throws ServiceException {
    SetNodeToLabelsRequestPBImpl request =
        new SetNodeToLabelsRequestPBImpl(proto);
    try {
      SetNodeToLabelsResponse response = real.setNodeToLabels(request);
      return ((SetNodeToLabelsResponsePBImpl) response).getProto();
    } catch (YarnException e) {
      throw new ServiceException(e);
    } catch (IOException e) {
      throw new ServiceException(e);
    }
  }

  @Override
  public GetNodeToLabelsResponseProto getNodeToLabels(RpcController controller,
      GetNodeToLabelsRequestProto proto) throws ServiceException {
    GetNodeToLabelsRequestPBImpl request =
        new GetNodeToLabelsRequestPBImpl(proto);
    try {
      GetNodeToLabelsResponse response = real.getNodeToLabels(request);
      return ((GetNodeToLabelsResponsePBImpl) response).getProto();
    } catch (YarnException e) {
      throw new ServiceException(e);
    } catch (IOException e) {
      throw new ServiceException(e);
    }
  }

  @Override
  public GetLabelsResponseProto getLabels(RpcController controller,
      GetLabelsRequestProto proto) throws ServiceException {
    GetLabelsRequestPBImpl request =
        new GetLabelsRequestPBImpl(proto);
    try {
      GetLabelsResponse response = real.getLabels(request);
      return ((GetLabelsResponsePBImpl) response).getProto();
    } catch (YarnException e) {
      throw new ServiceException(e);
    } catch (IOException e) {
      throw new ServiceException(e);
    }
  }
  
  @Override
  public ClearAllLabelsResponseProto clearAllLabels(RpcController controller,
      ClearAllLabelsRequestProto proto) throws ServiceException {
    ClearAllLabelsRequestPBImpl request =
        new ClearAllLabelsRequestPBImpl(proto);
    try {
      ClearAllLabelsResponse response = real.clearAllLabels(request);
      return ((ClearAllLabelsResponsePBImpl) response).getProto();
    } catch (YarnException e) {
      throw new ServiceException(e);
    } catch (IOException e) {
      throw new ServiceException(e);
    }
  }
}
