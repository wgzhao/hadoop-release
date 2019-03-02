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
package org.apache.hadoop.hdfs.server.common;

import javax.servlet.ServletException;
import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletContext;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import java.io.IOException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.security.token.delegation.DelegationTokenIdentifier;
import org.apache.hadoop.hdfs.web.AuthFilter;
import org.apache.hadoop.hdfs.web.WebHdfsFileSystem;
import org.apache.hadoop.security.authentication.server.AuthenticationFilter;
import org.apache.hadoop.security.token.Token;
import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.lang.Iterable;
import java.util.Arrays;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import org.apache.commons.net.util.SubnetUtils;
import org.apache.commons.io.FilenameUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HostRestrictingAuthorizationFilter implements Filter {
  private Logger LOG = LoggerFactory.getLogger(HostRestrictingAuthorizationFilter.class);
  private HashMap<String, ArrayList<Rule>> RULEMAP = null;
  public static final String HDFS_CONFIG_PREFIX = "dfs.web.authentication.";
  public static final String RESTRICTION_CONFIG = "host.allow.rules";

  private class Rule {
    private final SubnetUtils.SubnetInfo subnet;
    private final String path;

    /*
     * A class for holding dropbox filter rules
     *
     * @param subnet - the IPv4 subnet for which this rule is valid (pass null for
     * any network location)
     *
     * @param path - the HDFS path for which this rule is valid
     */
    Rule(SubnetUtils.SubnetInfo subnet, String path) {
      this.subnet = subnet;
      this.path = path;
    }

    public SubnetUtils.SubnetInfo getSubnet() {
      return(subnet);
    }

    public String getPath() {
      return(path);
    }
  }

  /*
   * Check all rules for this user to see if one matches for this host/path pair
   *
   * @Param: user - user to check rules for
   *
   * @Param: host - IP address (e.g. "192.168.0.1")
   *
   * @Param: path - file path with no scheme (e.g. /path/foo)
   *
   * @Returns: true if a rule matches this user, host, path tuple false if an
   * error occurs or no match
   */
  private boolean matchRule(String user, String remoteIp, String path) {
    // allow lookups for blank in the rules for user and path
    user = (user == null ? "" : user);
    path = (path == null ? "" : path);

    LOG.trace("Got user: {}, remoteIp: {}, path: {}", user, remoteIp, path);

    ArrayList<Rule> userRules = RULEMAP.get(user);
    ArrayList<Rule> anyRules = RULEMAP.get("*");
    if(anyRules != null) {
      if(userRules != null) {
        userRules.addAll(RULEMAP.get("*"));
      } else {
        userRules = anyRules;
      }
    }

    // isInRange fails for null/blank IPs, require an IP to approve
    if(remoteIp == null) {
      LOG.trace("Returned false due to null rempteIp");
      return false;
    }

    if(userRules != null) {
      for(Rule rule : userRules) {
        LOG.trace("Evaluating rule, subnet: {}, path: {}",
                  rule.getSubnet() != null ? rule.getSubnet().getCidrSignature() : null, rule.getPath());
        try {
          if((rule.getSubnet() == null || rule.getSubnet().isInRange(remoteIp))
              && FilenameUtils.directoryContains(rule.getPath(), path)) {
            LOG.debug("Found matching rule, subnet: {}, path: {}; returned true",
                      rule.getSubnet() != null ? rule.getSubnet().getCidrSignature() : null, rule.getPath());
            return true;
          }
        } catch(IOException e) {
          LOG.warn("Got IOException {}; returned false", e);
          return false;
        }
      }
    }
    LOG.trace("Found no rules for user");
    return false;
  }

  @Override
  public void destroy() {
    RULEMAP = null;
  }

  @Override
  public void init(FilterConfig config) throws ServletException {
    HashMap<String, String> overrideConfigs = new HashMap<String, String>();

    // Process dropbox rules
    String dropboxRules = config.getInitParameter(RESTRICTION_CONFIG);
    if(dropboxRules != null) {
      // name: dropbox.allow.rules
      // value: user1,network/bits1,path_glob1|user2,network/bints2,path_glob2...
      String[] rules = dropboxRules.split("\\||\n");
      RULEMAP = new HashMap<String, ArrayList<Rule>>(rules.length);
      for(String line : rules) {
        String[] parts = line.split(",", 3);
        // ensure we got a valid configuration record
        if (parts.length != 3) {
          LOG.debug("Failed to parse rule entry: {}", line);
          continue;
        }
        LOG.debug("Loaded rule: user: {}, network/bits: {} path: {}", parts[0], parts[1], parts[2]);
        String globPattern = parts[2];
        // Map is {"user": [subnet, path]}
        Rule rule = null;
        if(parts[1].trim().equals("*")) {
          rule = new Rule(null, globPattern);
        } else {
          rule = new Rule(new SubnetUtils(parts[1]).getInfo(), globPattern);
        }
        // Update the rule map with this rule

        ArrayList<Rule> ruleList = null;
        if(RULEMAP.containsKey(parts[0])){
          ruleList = RULEMAP.get(parts[0]);
        }else{
          ruleList = new ArrayList<Rule>() {};
        }
        ruleList.add(rule);
        RULEMAP.put(parts[0], ruleList);
      }
    } else {
      // make an empty hash since we have no rules
      RULEMAP = new HashMap(0);
    }
  }

  /**
   * doFilter() is a shim to create an HttpInteraction object and pass that to
   * the actual processing logic
   */
  @Override
  public void doFilter(ServletRequest request, ServletResponse response, FilterChain filterChain)
      throws IOException, ServletException {
    final HttpServletRequest httpRequest = (HttpServletRequest) request;
    HttpServletResponse httpResponse = (HttpServletResponse) response;

    handleInteraction(new ServletFilterHttpInteraction(httpRequest, httpResponse, filterChain));
  }


  /**
   * The actual processing logic of the Filter
   * Uses our {@HttpInteraction} shim which can be called from a variety of incoming request sources
   * @param interaction - An HttpInteraction object from any of our callers
   */
  public void handleInteraction(HttpInteraction interaction)
      throws IOException, ServletException {
    final String address = interaction.getRemoteAddr();
    final String query = interaction.getQueryString();
    final String path = interaction.getRequestURI().substring(WebHdfsFileSystem.PATH_PREFIX.length());
    String user = interaction.getRemoteUser();

    LOG.trace("Got request user: {}, remoteIp: {}, query: {}", user, address, query);
    if(!interaction.isCommitted() && "GET".equalsIgnoreCase(interaction.getMethod())) {
      LOG.trace("Got GET query and not committed");
      // loop over all query parts
      String[] queryParts = query.split("&");

      if(user == null) {
        LOG.trace("Looking for delegation token to identify user");
        for(String part : queryParts) {
          if (part.trim().startsWith("delegation=")){
            Token t = new Token();
            t.decodeFromUrlString(part.split("=", 2)[1]);
            ByteArrayInputStream buf = new ByteArrayInputStream(t.getIdentifier());
            DelegationTokenIdentifier identifier = new DelegationTokenIdentifier();
            identifier.readFields(new DataInputStream(buf));
            user = identifier.getUser().getUserName();
            LOG.trace("Updated request user: {}, remoteIp: {}, query: {}", user, address, query);
          }
        }
      }

      for(String part : query.split("&")) {
        if ((part.trim().equalsIgnoreCase("op=OPEN") || part.trim().equalsIgnoreCase("op=GETDELEGATIONTOKEN")) && !(matchRule("*", address, path) || matchRule(user, address, path))) {
          LOG.trace("Rejecting interaction; no rule found");
          interaction.sendError(HttpServletResponse.SC_FORBIDDEN,
            "WebHDFS is configured write-only for " + user + "@" + address + "for file: " + path);
          return;
        }
      }
    }

    LOG.trace("Proceeding with interaction");
    interaction.proceed();
  }

  /**
   * Constructs a mapping of configuration properties to be used for filter
   * initialization.  The mapping includes all properties that start with the
   * specified configuration prefix.  Property names in the mapping are trimmed
   * to remove the configuration prefix.
   *
   * @param conf configuration to read
   * @param confPrefix configuration prefix
   * @return mapping of configuration properties to be used for filter
   *     initialization
   */
  public static Map<String, String> getFilterParams(Configuration conf,
      String confPrefix) {
    return conf.getPropsWithPrefix(confPrefix);
  }

  /**
   * {@link HttpInteraction} implementation for use in the servlet filter.
   */
  private static final class ServletFilterHttpInteraction
      implements HttpInteraction {

    private final FilterChain chain;
    private final HttpServletRequest httpRequest;
    private final HttpServletResponse httpResponse;

    /**
     * Creates a new ServletFilterHttpInteraction.
     *
     * @param httpRequest request to process
     * @param httpResponse response to process
     * @param chain filter chain to forward to if HTTP interaction is allowed
     */
    public ServletFilterHttpInteraction(HttpServletRequest httpRequest,
        HttpServletResponse httpResponse, FilterChain chain) {
      this.httpRequest = httpRequest;
      this.httpResponse = httpResponse;
      this.chain = chain;
    }

    @Override
    public boolean isCommitted() {
      return(httpResponse.isCommitted());
    }

    @Override
    public String getRemoteAddr() {
      return(httpRequest.getRemoteAddr());
    }

    @Override
    public String getRemoteUser() {
      return(httpRequest.getRemoteUser());
    }

    @Override
    public String getRequestURI() {
      return(httpRequest.getRequestURI());
    }

    @Override
    public String getQueryString() {
      return(httpRequest.getQueryString());
    }

    @Override
    public String getMethod() {
      return httpRequest.getMethod();
    }

    @Override
    public void proceed() throws IOException, ServletException {
      chain.doFilter(httpRequest, httpResponse);
    }

    @Override
    public void sendError(int code, String message) throws IOException {
      httpResponse.sendError(code, message);
    }

  }

  /**
   * Defines the minimal API requirements for the filter to execute its
   * filtering logic.  This interface exists to facilitate integration in
   * components that do not run within a servlet container and therefore cannot
   * rely on a servlet container to dispatch to the {@link #doFilter} method.
   * Applications that do run inside a servlet container will not need to write
   * code that uses this interface.  Instead, they can use typical servlet
   * container configuration mechanisms to insert the filter.
   */
  public interface HttpInteraction {

    /**
     * Returns if the request has been committed.
     *
     * @return boolean
     */
    boolean isCommitted();

    /**
     * Returns the value of the requesting client address.
     *
     * @return the remote address
     */
    String getRemoteAddr();

    /**
     * Returns the user ID making the request.
     *
     * @return the user
     */
    String getRemoteUser();

    /**
     * Returns the value of the request URI.
     *
     * @return the request URI
     */
    String getRequestURI();

    /**
     * Returns the value of the query string.
     *
     * @return an optional contianing the URL query string
     */
    String getQueryString();

    /**
     * Returns the method.
     *
     * @return method
     */
    String getMethod();

    /**
     * Called by the filter after it decides that the request may proceed.
     *
     * @throws IOException if there is an I/O error
     * @throws ServletException if the implementation relies on the servlet API
     *     and a servlet API call has failed
     */
    void proceed() throws IOException, ServletException;

    /**
     * Called by the filter after it decides that the request is an
     * unauthorized request and therefore must be rejected.
     *
     * @param code status code to send
     * @param message response message
     * @throws IOException if there is an I/O error
     */
    void sendError(int code, String message) throws IOException;
  }
}
