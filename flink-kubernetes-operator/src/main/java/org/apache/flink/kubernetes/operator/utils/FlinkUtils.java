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

package org.apache.flink.kubernetes.operator.utils;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.GlobalConfiguration;
import org.apache.flink.kubernetes.operator.config.DefaultConfig;
import org.apache.flink.kubernetes.operator.crd.FlinkDeployment;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.client.KubernetesClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;

/** Flink Utility methods used by the operator. */
public class FlinkUtils {

    private static final Logger LOG = LoggerFactory.getLogger(FlinkUtils.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();

    public static DefaultConfig loadDefaultConfig() {
        Configuration operatorConfig =
                FlinkUtils.loadConfiguration(EnvUtils.get(EnvUtils.ENV_FLINK_OPERATOR_CONF_DIR));
        Configuration flinkConfig =
                FlinkUtils.loadConfiguration(EnvUtils.get(EnvUtils.ENV_FLINK_CONF_DIR));
        return new DefaultConfig(operatorConfig, flinkConfig);
    }

    public static Configuration getEffectiveConfig(
            FlinkDeployment flinkApp, Configuration flinkConfig) {
        try {
            final Configuration effectiveConfig =
                    FlinkConfigBuilder.buildFrom(flinkApp, flinkConfig);
            LOG.debug("Effective config: {}", effectiveConfig);
            return effectiveConfig;
        } catch (Exception e) {
            throw new RuntimeException("Failed to load configuration", e);
        }
    }

    public static Configuration loadConfiguration(String confDir) {
        return confDir != null
                ? GlobalConfiguration.loadConfiguration(confDir)
                : new Configuration();
    }

    public static Pod mergePodTemplates(Pod toPod, Pod fromPod) {
        if (fromPod == null) {
            return toPod;
        } else if (toPod == null) {
            return fromPod;
        }
        JsonNode node1 = MAPPER.valueToTree(toPod);
        JsonNode node2 = MAPPER.valueToTree(fromPod);
        mergeInto(node1, node2);
        try {
            return MAPPER.treeToValue(node1, Pod.class);
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    private static void mergeInto(JsonNode toNode, JsonNode fromNode) {
        Iterator<String> fieldNames = fromNode.fieldNames();
        while (fieldNames.hasNext()) {
            String fieldName = fieldNames.next();
            JsonNode toChildNode = toNode.get(fieldName);
            JsonNode fromChildNode = fromNode.get(fieldName);

            if (toChildNode != null && toChildNode.isArray() && fromChildNode.isArray()) {
                // TODO: does merging arrays even make sense or should it just override?
                for (int i = 0; i < fromChildNode.size(); i++) {
                    JsonNode updatedChildNode = fromChildNode.get(i);
                    if (toChildNode.size() <= i) {
                        // append new node
                        ((ArrayNode) toChildNode).add(updatedChildNode);
                    }
                    mergeInto(toChildNode.get(i), updatedChildNode);
                }
            } else if (toChildNode != null && toChildNode.isObject()) {
                mergeInto(toChildNode, fromChildNode);
            } else {
                if (toNode instanceof ObjectNode) {
                    ((ObjectNode) toNode).replace(fieldName, fromChildNode);
                }
            }
        }
    }

    public static void deleteCluster(FlinkDeployment flinkApp, KubernetesClient kubernetesClient) {
        deleteCluster(
                flinkApp.getMetadata().getNamespace(),
                flinkApp.getMetadata().getName(),
                kubernetesClient);
    }

    public static void deleteCluster(
            String namespace, String clusterId, KubernetesClient kubernetesClient) {
        kubernetesClient
                .apps()
                .deployments()
                .inNamespace(namespace)
                .withName(clusterId)
                .cascading(true)
                .delete();
    }
}
