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
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import com.google.gson.JsonParser;

public class NodeLabelUtils {
  private static final String PARSE_FAILED_MSG =
      "Failed to parse node-> labels json";
  private static final String LABELS_KEY =
      "labels";
  
  /**
   * Get node to labels from JSON like:
   *
   * {
   * "host1": {
   *     "labels": [
   *         "x",
   *         "y",
   *         "z"
   *     ]
   * },
   * "host2": {
   *     "labels": [
   *         "a",
   *         "b",
   *         "c"
   *     ]
   * },
   * "host3": {
   *     "labels": []
   * }
   * }
   * 
   * @param json
   * @return node to labels map
   */
  public static Map<String, Set<String>> getNodeToLabelsFromJson(String json)
      throws IOException {
    Map<String, Set<String>> nodeToLabels = new HashMap<String, Set<String>>();
    
    if (json == null || json.trim().isEmpty()) {
      return nodeToLabels;
    }
    
    JsonParser parser = new JsonParser();
    JsonElement node;
    try {
      node = parser.parse(json);
    } catch (JsonParseException e) {
      throw new IOException(e);
    }
    
    if (node.isJsonObject()) {
      JsonObject obj = node.getAsJsonObject();
      for (Map.Entry<String, JsonElement> entry : obj.entrySet()) {
         String nodeName = entry.getKey().trim();
         if (nodeName.isEmpty()) {
           throw new IOException(PARSE_FAILED_MSG);
         }
         nodeToLabels.put(nodeName, new HashSet<String>());
         
         if (entry.getValue().isJsonObject()) {
           JsonObject labelObj = entry.getValue().getAsJsonObject();
           if (labelObj.entrySet().size() > 0) {
             JsonElement labelsElement = labelObj.get(LABELS_KEY);
             if (labelsElement == null || !labelsElement.isJsonArray()) {
               throw new IOException(PARSE_FAILED_MSG);
             }
             JsonArray labelsArray = labelsElement.getAsJsonArray();
             for (JsonElement item : labelsArray) {
               nodeToLabels.get(nodeName).add(item.getAsString());
             }
           }
         } else {
           throw new IOException(PARSE_FAILED_MSG);
         }
      }
    } else {
      throw new IOException(PARSE_FAILED_MSG);
    }
    
    return nodeToLabels;
  }
}
