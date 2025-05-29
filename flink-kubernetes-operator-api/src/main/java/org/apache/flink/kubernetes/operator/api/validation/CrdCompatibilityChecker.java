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

package org.apache.flink.kubernetes.operator.api.validation;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;

/**
 * Utility for checking backward CRD compatibility.
 *
 * <p>The aim is to provide a very strict compatibility check logic initially that can be improved
 * over time with compatible type changes if necessary by CRD updates
 *
 * <p>It performs the following checks:
 *
 * <ul>
 *   <li>No property removed from any object
 *   <li>No enum value removed from enums (changing enum to string is allowed)
 *   <li>No type changes for fields
 *   <li>No type changes for array/map items
 *   <li>No change in extra properties of field schema definitions
 * </ul>
 */
public class CrdCompatibilityChecker {

    private static final Logger logger = LoggerFactory.getLogger(CrdCompatibilityChecker.class);
    private static final ObjectMapper objectMapper = new ObjectMapper(new YAMLFactory());

    public static void main(String[] args) throws IOException {

        for (int i = 1; i < args.length; i++) {
            String actualSchema = args[0];
            String oldSchema = args[i];
            logger.info("New schema: {}", actualSchema);
            logger.info("Old schema: {}", oldSchema);
            checkObjectCompatibility("", getSchema(oldSchema), getSchema(actualSchema));
        }
        System.out.println("Successful validation!");
    }

    private static JsonNode getSchema(String url) throws IOException {
        var target = new URL(url);
        var protocol = target.getProtocol();
        JsonNode crd;
        if ("file".equals(protocol)) {
            crd = objectMapper.readTree(new File(url.substring(7)));
        } else {
            crd = objectMapper.readTree(target);
        }
        return crd.get("spec").get("versions").get(0).get("schema").get("openAPIV3Schema");
    }

    protected static void checkObjectCompatibility(
            String path, JsonNode oldNode, JsonNode newNode) {
        checkTypeCompatibility(path, oldNode, newNode);

        if (oldNode.has("type")
                && oldNode.get("type").asText().equals("object")
                && oldNode.has("properties")) {
            var oldProps = oldNode.get("properties");
            var newProps = newNode.get("properties");

            var fieldNamesIt = oldProps.fieldNames();
            while (fieldNamesIt.hasNext()) {
                var field = fieldNamesIt.next();
                var fieldPath = path + "." + field;
                if (!newProps.has(field)) {
                    // This field was removed from Kubernetes ObjectMeta v1 in 1.25 as it was unused
                    // for a long time. If set for any reason (very unlikely as it does nothing),
                    // the property will be dropped / ignored by the api server.
                    if (!fieldPath.endsWith(".metadata.clusterName")
                            // This claims field was removed in Kubernetes 1.28 as it was mistakenly
                            // added in the first place. For more context please refer to
                            // https://github.com/kubernetes/api/commit/8b14183
                            && !fieldPath.contains(".volumeClaimTemplate.spec.resources.claims")
                            && !fieldPath.contains(
                                    ".spec.taskManager.podTemplate.spec.resourceClaims.items.source")
                            && !fieldPath.contains(
                                    ".spec.jobManager.podTemplate.spec.resourceClaims.items.source")
                            && !fieldPath.contains(
                                    ".spec.podTemplate.spec.resourceClaims.items.source")) {
                        err(fieldPath + " has been removed");
                    }
                } else {
                    checkObjectCompatibility(fieldPath, oldProps.get(field), newProps.get(field));
                }
            }
            logger.debug("Successfully validated property names for {}", path);
        } else {
            logger.debug("Successfully validated type for {}", path);
        }
    }

    protected static void checkTypeCompatibility(String path, JsonNode oldNode, JsonNode newNode) {
        if (!oldNode.has("type") && oldNode.has("anyOf")) {
            if (!oldNode.equals(newNode)) {
                err("AnyOf type mismatch for" + path);
            } else {
                return;
            }
        }

        String oldType = oldNode.get("type").asText();

        if (!oldType.equals(newNode.get("type").asText())) {
            err("Type mismatch for " + path);
        }

        verifyOtherPropsMatch(path, oldNode, newNode);

        if (oldType.equals("string")) {
            checkStringTypeCompatibility(path, oldNode, newNode);
        }

        if (oldType.equals("object") && oldNode.has("additionalProperties")) {
            checkTypeCompatibility(
                    path + ".additionalProperties",
                    oldNode.get("additionalProperties"),
                    newNode.get("additionalProperties"));
        }

        if (oldType.equals("array")) {
            checkObjectCompatibility(path + ".items", oldNode.get("items"), newNode.get("items"));
        }
    }

    protected static void verifyOtherPropsMatch(String path, JsonNode oldNode, JsonNode newNode) {
        var oldCopy = (ObjectNode) oldNode.deepCopy();
        var newCopy = (ObjectNode) newNode.deepCopy();
        List.of("items", "additionalProperties", "properties", "enum", "type")
                .forEach(
                        k -> {
                            oldCopy.remove(k);
                            newCopy.remove(k);
                        });
        if (!oldCopy.equals(newCopy)) {
            err("Other property mismatch for " + path);
        }
    }

    protected static void checkStringTypeCompatibility(
            String path, JsonNode oldNode, JsonNode newNode) {
        if (!oldNode.has("enum") && newNode.has("enum")) {
            // We make an exception here for jobstatus.state, this is a backward compatible change
            if (!path.equals(".status.jobStatus.state")) {
                err("Cannot turn string into enum for " + path);
            }
        }

        if (oldNode.has("enum")) {
            if (!newNode.has("enum")) {
                return;
            }

            List<String> newElements = new ArrayList<>();
            newNode.get("enum").elements().forEachRemaining(jn -> newElements.add(jn.asText()));

            oldNode.get("enum")
                    .elements()
                    .forEachRemaining(
                            jn -> {
                                if (!newElements.contains(jn.asText())) {
                                    err(
                                            "Enum value "
                                                    + jn.asText()
                                                    + " has been removed for "
                                                    + path);
                                }
                            });
        }
    }

    private static void err(String err) {
        throw new CompatibilityError(err);
    }

    /** Compatibility Error class. */
    public static class CompatibilityError extends RuntimeException {
        public CompatibilityError(String msg) {
            super(msg);
        }
    }
}
