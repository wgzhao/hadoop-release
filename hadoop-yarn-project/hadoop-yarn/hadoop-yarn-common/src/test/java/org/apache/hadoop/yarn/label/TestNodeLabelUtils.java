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

package org.apache.hadoop.yarn.label;

import java.io.IOException;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.yarn.label.NodeLabelUtils;
import org.junit.Assert;
import org.junit.Test;

import com.google.common.collect.ImmutableMap;

public class TestNodeLabelUtils extends NodeLabelTestBase {
  private void assertParseShouldFail(String json, boolean shouldFail) {
    try {
      NodeLabelUtils.getNodeToLabelsFromJson(json);
      if (shouldFail) {
        Assert.fail("should fail:" + json == null ? "" : json);
      }
    } catch (IOException e) {
      if (!shouldFail) {
        Assert.fail("shouldn't fail:" + json == null ? "" : json);
      }
    }
  }

  private void assertParseFailed(String json) {
    assertParseShouldFail(json, true);
  }

  @Test
  public void testParseNodeToLabelsFromJson() throws IOException {
    // empty and null
    assertParseShouldFail(null, false);
    assertParseShouldFail("", false);

    // empty host
    String json =
        "{\"host1\":{\"labels\":[\"x\",\"y\"]}, \"\":{\"labels\":[\"x\",\"y\"]}}";
    assertParseFailed(json);

    // not json object
    json =
        "[\"host1\":{\"labels\":[\"x\",\"y\"]}, \"\":{\"labels\":[\"x\",\"y\"]}]";
    assertParseFailed(json);

    // don't have labels
    json =
        "[\"host1\":{\"labels\":[\"x\",\"y\"]}, \"\":{\"tag\":[\"x\",\"y\"]}]";
    assertParseFailed(json);

    // labels is not array
    json = "{\"host1\":{\"labels\":{\"x\":\"y\"}}}";
    assertParseFailed(json);

    // not a valid json
    json = "[ }";
    assertParseFailed(json);

    // normal case #1
    json =
        "{\"host1\":{\"labels\":[\"x\",\"y\"]}, \"host2\":{\"labels\":[\"x\",\"y\"]}}";
    Map<String, Set<String>> nodeToLabels =
        NodeLabelUtils.getNodeToLabelsFromJson(json);
    assertMapEquals(nodeToLabels,
        ImmutableMap.of("host1", toSet("x", "y"), "host2", toSet("x", "y")));

    // normal case #2
    json =
        "{\"host1\":{\"labels\":[\"x\",\"y\"]}, \"host2\":{\"labels\":[\"a\",\"b\"]}}";
    nodeToLabels = NodeLabelUtils.getNodeToLabelsFromJson(json);
    assertMapEquals(nodeToLabels,
        ImmutableMap.of("host1", toSet("x", "y"), "host2", toSet("a", "b")));

    // label is empty #1
    json = "{\"host1\":{\"labels\":[\"x\",\"y\"]}, \"host2\":{\"labels\":[]}}";
    nodeToLabels = NodeLabelUtils.getNodeToLabelsFromJson(json);
    assertMapEquals(nodeToLabels, ImmutableMap.of("host1", toSet("x", "y"),
        "host2", new HashSet<String>()));
    
    // label is empty #2
    json = "{\"host1\":{\"labels\":[\"x\",\"y\"]}, \"host2\":{}}";
    nodeToLabels = NodeLabelUtils.getNodeToLabelsFromJson(json);
    assertMapEquals(nodeToLabels, ImmutableMap.of("host1", toSet("x", "y"),
        "host2", new HashSet<String>()));
  }
}
