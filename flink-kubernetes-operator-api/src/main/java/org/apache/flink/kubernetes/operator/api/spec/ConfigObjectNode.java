/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.kubernetes.operator.api.spec;

import org.apache.flink.configuration.Configuration;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/** Allows parsing configurations as YAML, and adds related utility methods. */
public class ConfigObjectNode extends ObjectNode {

    public ConfigObjectNode() {
        this(JsonNodeFactory.instance);
    }

    public ConfigObjectNode(JsonNodeFactory nc, Map<String, JsonNode> kids) {
        super(nc, kids);
    }

    public ConfigObjectNode(JsonNodeFactory nc) {
        super(nc);
    }

    public void remove(String... names) {
        remove(Arrays.asList(names));
    }

    public void putAllFrom(Map<String, String> value) {
        value.forEach(this::put);
    }

    public void setAllFrom(Map<String, String> value) {
        removeAll();
        putAllFrom(value);
    }

    public Map<String, String> asFlatMap() {
        Map<String, String> flatMap = new HashMap<>();
        flattenHelper(this, "", flatMap);
        return flatMap;
    }

    public Configuration asConfiguration() {
        return Configuration.fromMap(asFlatMap());
    }

    private static void flattenHelper(
            JsonNode node, String parentKey, Map<String, String> flatMap) {
        if (node.isObject()) {
            Iterator<Map.Entry<String, JsonNode>> fields = node.fields();
            while (fields.hasNext()) {
                Map.Entry<String, JsonNode> field = fields.next();
                String newKey =
                        parentKey.isEmpty() ? field.getKey() : parentKey + "." + field.getKey();
                flattenHelper(field.getValue(), newKey, flatMap);
            }
        } else if (node.isArray()) {
            for (int i = 0; i < node.size(); i++) {
                if (node instanceof ArrayNode) {
                    flatMap.put(parentKey, arrayNodeToSemicolonSepratedString((ArrayNode) node));
                } else {
                    String newKey = parentKey + "[" + i + "]";
                    flattenHelper(node.get(i), newKey, flatMap);
                }
            }
        } else {
            flatMap.put(parentKey, node.asText());
        }
    }

    private static String arrayNodeToSemicolonSepratedString(ArrayNode arrayNode) {
        if (arrayNode == null || arrayNode.isEmpty()) {
            return "";
        }
        List<String> stringValues = new ArrayList<>();
        for (JsonNode node : arrayNode) {
            stringValues.add(node.asText());
        }
        return String.join(";", stringValues);
    }
}
