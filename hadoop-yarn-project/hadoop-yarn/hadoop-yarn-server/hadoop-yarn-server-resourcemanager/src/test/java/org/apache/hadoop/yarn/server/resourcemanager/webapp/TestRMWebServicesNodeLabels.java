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

package org.apache.hadoop.yarn.server.resourcemanager.webapp;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.io.StringReader;
import java.io.StringWriter;

import javax.ws.rs.core.MediaType;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.MockRM;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.ResourceManager;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.NodeToLabelsInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.NodesToLabelsInfo;
import org.apache.hadoop.yarn.webapp.GenericExceptionHandler;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.junit.Test;

import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.servlet.GuiceServletContextListener;
import com.google.inject.servlet.ServletModule;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.WebResource;
import com.sun.jersey.api.json.JSONJAXBContext;
import com.sun.jersey.api.json.JSONMarshaller;
import com.sun.jersey.api.json.JSONUnmarshaller;
import com.sun.jersey.guice.spi.container.servlet.GuiceContainer;
import com.sun.jersey.test.framework.JerseyTest;
import com.sun.jersey.test.framework.WebAppDescriptor;

public class TestRMWebServicesNodeLabels extends JerseyTest {

  private static final Log LOG = LogFactory
      .getLog(TestRMWebServicesNodeLabels.class);

  private static MockRM rm;
  private YarnConfiguration conf;

  private String userName;
  private String notUserName;

  private Injector injector = Guice.createInjector(new ServletModule() {
    @Override
    protected void configureServlets() {
      bind(JAXBContextResolver.class);
      bind(RMWebServices.class);
      bind(GenericExceptionHandler.class);
      try {
        userName = UserGroupInformation.getCurrentUser().getShortUserName();
      } catch (IOException ioe) {
        throw new RuntimeException("Unable to get current user name "
            + ioe.getMessage(), ioe);
      }
      notUserName = userName + "abc123";
      conf = new YarnConfiguration();
      conf.set(YarnConfiguration.YARN_ADMIN_ACL, userName);
      rm = new MockRM(conf);
      bind(ResourceManager.class).toInstance(rm);
      bind(RMContext.class).toInstance(rm.getRMContext());
      filter("/*").through(
          TestRMWebServicesAppsModification.TestRMCustomAuthFilter.class);
      serve("/*").with(GuiceContainer.class);
    }
  });

  public class GuiceServletConfig extends GuiceServletContextListener {

    @Override
    protected Injector getInjector() {
      return injector;
    }
  }

  public TestRMWebServicesNodeLabels() {
    super(new WebAppDescriptor.Builder(
        "org.apache.hadoop.yarn.server.resourcemanager.webapp")
        .contextListenerClass(GuiceServletConfig.class)
        .filterClass(com.google.inject.servlet.GuiceFilter.class)
        .contextPath("jersey-guice-filter").servletPath("/").build());
  }

  @Test
  public void testNodeLabels() throws JSONException, Exception {
    WebResource r = resource();

    ClientResponse response;
    JSONObject json;
    JSONArray jarr;
    String responseString;

    // Add a label
    response =
        r.path("ws").path("v1").path("cluster").path("labels")
            .path("add-labels").queryParam("user.name", userName)
            .accept(MediaType.APPLICATION_JSON)
            .entity("{\"label\":\"a\"}", MediaType.APPLICATION_JSON)
            .post(ClientResponse.class);

    // Verify it is present
    response =
        r.path("ws").path("v1").path("cluster").path("labels")
            .path("all-labels").queryParam("user.name", userName)
            .accept(MediaType.APPLICATION_JSON).get(ClientResponse.class);
    assertEquals(MediaType.APPLICATION_JSON_TYPE, response.getType());
    json = response.getEntity(JSONObject.class);
    assertEquals("a", json.getString("label"));

    // Add another
    response =
        r.path("ws").path("v1").path("cluster").path("labels")
            .path("add-labels").queryParam("user.name", userName)
            .accept(MediaType.APPLICATION_JSON)
            .entity("{\"label\":\"b\"}", MediaType.APPLICATION_JSON)
            .post(ClientResponse.class);

    response =
        r.path("ws").path("v1").path("cluster").path("labels")
            .path("all-labels").queryParam("user.name", userName)
            .accept(MediaType.APPLICATION_JSON).get(ClientResponse.class);
    assertEquals(MediaType.APPLICATION_JSON_TYPE, response.getType());
    json = response.getEntity(JSONObject.class);

    // Verify
    jarr = json.getJSONArray("label");
    assertEquals(2, jarr.length());

    // Remove one
    response =
        r.path("ws").path("v1").path("cluster").path("labels")
            .path("remove-labels").queryParam("user.name", userName)
            .accept(MediaType.APPLICATION_JSON)
            .entity("{\"label\":\"a\"}", MediaType.APPLICATION_JSON)
            .post(ClientResponse.class);

    response =
        r.path("ws").path("v1").path("cluster").path("labels")
            .path("all-labels").queryParam("user.name", userName)
            .accept(MediaType.APPLICATION_JSON).get(ClientResponse.class);
    assertEquals(MediaType.APPLICATION_JSON_TYPE, response.getType());
    json = response.getEntity(JSONObject.class);
    // Verify
    assertEquals("b", json.getString("label"));

    // Add a node->label mapping
    NodesToLabelsInfo nsli = new NodesToLabelsInfo();
    NodeToLabelsInfo nli = new NodeToLabelsInfo("node1");
    nli.getLabels().add("b");
    nsli.add(nli);

    response =
        r.path("ws")
            .path("v1")
            .path("cluster")
            .path("labels")
            .path("set-node-to-labels")
            .queryParam("user.name", userName)
            .accept(MediaType.APPLICATION_JSON)
            .entity(toJson(nsli, NodesToLabelsInfo.class),
                MediaType.APPLICATION_JSON).post(ClientResponse.class);

    response =
        r.path("ws").path("v1").path("cluster").path("labels")
            .path("all-nodes-to-labels").queryParam("user.name", userName)
            .accept(MediaType.APPLICATION_JSON).get(ClientResponse.class);
    assertEquals(MediaType.APPLICATION_JSON_TYPE, response.getType());
    responseString = response.getEntity(String.class);
    LOG.info(responseString);
    // Verify
    nsli =
        (NodesToLabelsInfo) fromJson(responseString, NodesToLabelsInfo.class);
    assertEquals(1, nsli.getNodeToLabelsInfos().size());
    nli = nsli.getNodeToLabelsInfos().get(0);
    assertEquals("node1", nli.getNode());
    assertEquals(1, nli.getLabels().size());
    assertTrue(nli.getLabels().contains("b"));

    // Get with filter which should suppress results
    response =
        r.path("ws").path("v1").path("cluster").path("labels")
            .path("all-nodes-to-labels").queryParam("labels", "a")
            .queryParam("user.name", userName)
            .accept(MediaType.APPLICATION_JSON).get(ClientResponse.class);
    assertEquals(MediaType.APPLICATION_JSON_TYPE, response.getType());
    responseString = response.getEntity(String.class);
    LOG.info(responseString);
    nsli =
        (NodesToLabelsInfo) fromJson(responseString, NodesToLabelsInfo.class);
    assertEquals(0, nsli.getNodeToLabelsInfos().size());

    // Get with filter which should include results
    response =
        r.path("ws").path("v1").path("cluster").path("labels")
            .path("all-nodes-to-labels").queryParam("labels", "b")
            .queryParam("user.name", userName)
            .accept(MediaType.APPLICATION_JSON).get(ClientResponse.class);
    assertEquals(MediaType.APPLICATION_JSON_TYPE, response.getType());
    responseString = response.getEntity(String.class);
    LOG.info(responseString);
    nsli =
        (NodesToLabelsInfo) fromJson(responseString, NodesToLabelsInfo.class);
    assertEquals(1, nsli.getNodeToLabelsInfos().size());

    // "Remove" by setting with an empty label set
    nli = nsli.getNodeToLabelsInfos().get(0);
    nli.getLabels().remove("b");

    response =
        r.path("ws")
            .path("v1")
            .path("cluster")
            .path("labels")
            .path("set-node-to-labels")
            .queryParam("user.name", userName)
            .accept(MediaType.APPLICATION_JSON)
            .entity(toJson(nsli, NodesToLabelsInfo.class),
                MediaType.APPLICATION_JSON).post(ClientResponse.class);

    response =
        r.path("ws").path("v1").path("cluster").path("labels")
            .path("all-nodes-to-labels").queryParam("user.name", userName)
            .accept(MediaType.APPLICATION_JSON).get(ClientResponse.class);
    assertEquals(MediaType.APPLICATION_JSON_TYPE, response.getType());
    responseString = response.getEntity(String.class);
    LOG.info(responseString);
    nsli =
        (NodesToLabelsInfo) fromJson(responseString, NodesToLabelsInfo.class);
    assertEquals(1, nsli.getNodeToLabelsInfos().size());
    nli = nsli.getNodeToLabelsInfos().get(0);
    assertTrue(nli.getLabels().isEmpty());

  }

  @Test
  public void testNodeLabelsAuthFail() throws JSONException, Exception {

    WebResource r = resource();

    ClientResponse response;
    JSONObject json;
    String responseString;

    // Add a label
    response =
        r.path("ws").path("v1").path("cluster").path("labels")
            .path("add-labels").queryParam("user.name", userName)
            .accept(MediaType.APPLICATION_JSON)
            .entity("{\"label\":\"a\"}", MediaType.APPLICATION_JSON)
            .post(ClientResponse.class);

    // Verify it is present
    response =
        r.path("ws").path("v1").path("cluster").path("labels")
            .path("all-labels").queryParam("user.name", userName)
            .accept(MediaType.APPLICATION_JSON).get(ClientResponse.class);
    assertEquals(MediaType.APPLICATION_JSON_TYPE, response.getType());
    json = response.getEntity(JSONObject.class);
    assertEquals("a", json.getString("label"));

    // Fail adding another
    response =
        r.path("ws").path("v1").path("cluster").path("labels")
            .path("add-labels").queryParam("user.name", notUserName)
            .accept(MediaType.APPLICATION_JSON)
            .entity("{\"label\":\"b\"}", MediaType.APPLICATION_JSON)
            .post(ClientResponse.class);

    response =
        r.path("ws").path("v1").path("cluster").path("labels")
            .path("all-labels").queryParam("user.name", userName)
            .accept(MediaType.APPLICATION_JSON).get(ClientResponse.class);
    assertEquals(MediaType.APPLICATION_JSON_TYPE, response.getType());
    json = response.getEntity(JSONObject.class);

    // Verify
    assertEquals("a", json.getString("label"));

    // Faile to remove one
    response =
        r.path("ws").path("v1").path("cluster").path("labels")
            .path("remove-labels").queryParam("user.name", notUserName)
            .accept(MediaType.APPLICATION_JSON)
            .entity("{\"label\":\"a\"}", MediaType.APPLICATION_JSON)
            .post(ClientResponse.class);

    response =
        r.path("ws").path("v1").path("cluster").path("labels")
            .path("all-labels").queryParam("user.name", userName)
            .accept(MediaType.APPLICATION_JSON).get(ClientResponse.class);
    assertEquals(MediaType.APPLICATION_JSON_TYPE, response.getType());
    json = response.getEntity(JSONObject.class);
    // Verify
    assertEquals("a", json.getString("label"));

    // Add a node->label mapping
    NodesToLabelsInfo nsli = new NodesToLabelsInfo();
    NodeToLabelsInfo nli = new NodeToLabelsInfo("node1");
    nli.getLabels().add("a");
    nsli.add(nli);

    response =
        r.path("ws")
            .path("v1")
            .path("cluster")
            .path("labels")
            .path("set-node-to-labels")
            .queryParam("user.name", userName)
            .accept(MediaType.APPLICATION_JSON)
            .entity(toJson(nsli, NodesToLabelsInfo.class),
                MediaType.APPLICATION_JSON).post(ClientResponse.class);

    response =
        r.path("ws").path("v1").path("cluster").path("labels")
            .path("all-nodes-to-labels").queryParam("user.name", userName)
            .accept(MediaType.APPLICATION_JSON).get(ClientResponse.class);
    assertEquals(MediaType.APPLICATION_JSON_TYPE, response.getType());
    responseString = response.getEntity(String.class);
    LOG.info(responseString);
    // Verify
    nsli =
        (NodesToLabelsInfo) fromJson(responseString, NodesToLabelsInfo.class);
    assertEquals(1, nsli.getNodeToLabelsInfos().size());
    nli = nsli.getNodeToLabelsInfos().get(0);
    assertEquals("node1", nli.getNode());
    assertEquals(1, nli.getLabels().size());
    assertTrue(nli.getLabels().contains("a"));

    // Fail "Remove" by setting with an empty label set
    nli = nsli.getNodeToLabelsInfos().get(0);
    nli.getLabels().remove("a");

    response =
        r.path("ws")
            .path("v1")
            .path("cluster")
            .path("labels")
            .path("set-node-to-labels")
            .queryParam("user.name", notUserName)
            .accept(MediaType.APPLICATION_JSON)
            .entity(toJson(nsli, NodesToLabelsInfo.class),
                MediaType.APPLICATION_JSON).post(ClientResponse.class);

    response =
        r.path("ws").path("v1").path("cluster").path("labels")
            .path("all-nodes-to-labels").queryParam("user.name", userName)
            .accept(MediaType.APPLICATION_JSON).get(ClientResponse.class);
    assertEquals(MediaType.APPLICATION_JSON_TYPE, response.getType());
    responseString = response.getEntity(String.class);
    LOG.info(responseString);
    nsli =
        (NodesToLabelsInfo) fromJson(responseString, NodesToLabelsInfo.class);
    assertEquals(1, nsli.getNodeToLabelsInfos().size());
    nli = nsli.getNodeToLabelsInfos().get(0);
    assertFalse(nli.getLabels().isEmpty());

  }

  @SuppressWarnings("rawtypes")
  private String toJson(Object nsli, Class klass) throws Exception {
    StringWriter sw = new StringWriter();
    JSONJAXBContext ctx = new JSONJAXBContext(klass);
    JSONMarshaller jm = ctx.createJSONMarshaller();
    jm.marshallToJSON(nsli, sw);
    return sw.toString();
  }

  @SuppressWarnings({ "rawtypes", "unchecked" })
  private Object fromJson(String json, Class klass) throws Exception {
    StringReader sr = new StringReader(json);
    JSONJAXBContext ctx = new JSONJAXBContext(klass);
    JSONUnmarshaller jm = ctx.createJSONUnmarshaller();
    return jm.unmarshalFromJSON(sr, klass);
  }

}
