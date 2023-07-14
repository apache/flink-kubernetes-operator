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

import org.apache.flink.kubernetes.operator.api.FlinkDeployment;
import org.apache.flink.kubernetes.operator.api.FlinkSessionJob;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.net.URL;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Tests for @{@link org.apache.flink.kubernetes.operator.api.validation.CrdCompatibilityChecker}.
 */
public class CrdCompatibilityCheckerTest {
    private static final ObjectMapper objectMapper = new ObjectMapper(new YAMLFactory());

    @Test
    public void testTypeMismatch() throws Exception {
        expectError(
                "openAPIV3Schema:\n"
                        + "  properties:\n"
                        + "    template:\n"
                        + "      type: string\n"
                        + "    className:\n"
                        + "      type: string\n"
                        + "  type: object",
                "openAPIV3Schema:\n"
                        + "  properties:\n"
                        + "    template:\n"
                        + "      type: string\n"
                        + "    className:\n"
                        + "      type: integer\n"
                        + "  type: object",
                "Type mismatch for .className");

        expectError(
                "openAPIV3Schema:\n"
                        + "  properties:\n"
                        + "    annotations:\n"
                        + "      additionalProperties:\n"
                        + "        type: string\n"
                        + "      type: object\n"
                        + "  type: object",
                "openAPIV3Schema:\n"
                        + "  properties:\n"
                        + "    annotations:\n"
                        + "      additionalProperties:\n"
                        + "        type: integer\n"
                        + "      type: object\n"
                        + "  type: object",
                "Type mismatch for .annotations.additionalProperties");

        expectError(
                "openAPIV3Schema:\n"
                        + "  properties:\n"
                        + "    annotations:\n"
                        + "      items:\n"
                        + "        type: string\n"
                        + "      type: array\n"
                        + "  type: object",
                "openAPIV3Schema:\n"
                        + "  properties:\n"
                        + "    annotations:\n"
                        + "      items:\n"
                        + "        type: integer\n"
                        + "      type: array\n"
                        + "  type: object",
                "Type mismatch for .annotations.items");

        expectError(
                "openAPIV3Schema:\n"
                        + "  properties:\n"
                        + "    annotations:\n"
                        + "      items:\n"
                        + "        properties:\n"
                        + "          prop1:\n"
                        + "            type: integer\n"
                        + "        type: object\n"
                        + "      type: array\n"
                        + "  type: object",
                "openAPIV3Schema:\n"
                        + "  properties:\n"
                        + "    annotations:\n"
                        + "      items:\n"
                        + "        properties:\n"
                        + "          prop1:\n"
                        + "            type: double\n"
                        + "        type: object\n"
                        + "      type: array\n"
                        + "  type: object",
                "Type mismatch for .annotations.items.prop1");
    }

    @Test
    public void testEnumCompatibility() throws Exception {
        expectError(
                "openAPIV3Schema:\n"
                        + "  properties:\n"
                        + "    enumProp:\n"
                        + "      enum:\n"
                        + "        - v1\n"
                        + "        - v2\n"
                        + "      type: string\n"
                        + "  type: object",
                "openAPIV3Schema:\n"
                        + "  properties:\n"
                        + "    enumProp:\n"
                        + "      enum:\n"
                        + "        - v1\n"
                        + "        - v3\n"
                        + "      type: string\n"
                        + "  type: object",
                "Enum value v2 has been removed for .enumProp");

        expectSuccess(
                "openAPIV3Schema:\n"
                        + "  properties:\n"
                        + "    enumProp:\n"
                        + "      enum:\n"
                        + "        - v1\n"
                        + "        - v2\n"
                        + "      type: string\n"
                        + "  type: object",
                "openAPIV3Schema:\n"
                        + "  properties:\n"
                        + "    enumProp:\n"
                        + "      enum:\n"
                        + "        - v1\n"
                        + "        - v2\n"
                        + "        - v3\n"
                        + "      type: string\n"
                        + "  type: object");

        expectSuccess(
                "openAPIV3Schema:\n"
                        + "  properties:\n"
                        + "    enumProp:\n"
                        + "      enum:\n"
                        + "        - v1\n"
                        + "        - v2\n"
                        + "      type: string\n"
                        + "  type: object",
                "openAPIV3Schema:\n"
                        + "  properties:\n"
                        + "    enumProp:\n"
                        + "      type: string\n"
                        + "  type: object");

        expectError(
                "openAPIV3Schema:\n"
                        + "  properties:\n"
                        + "    enumProp:\n"
                        + "      type: string\n"
                        + "  type: object",
                "openAPIV3Schema:\n"
                        + "  properties:\n"
                        + "    enumProp:\n"
                        + "      enum:\n"
                        + "        - v1\n"
                        + "        - v3\n"
                        + "      type: string\n"
                        + "  type: object",
                "Cannot turn string into enum for .enumProp");
    }

    @Test
    public void testOtherPropertyMismatch() throws Exception {
        expectError(
                "openAPIV3Schema:\n"
                        + "  properties:\n"
                        + "    template:\n"
                        + "      maxLength: 123\n"
                        + "      type: string\n"
                        + "  type: object",
                "openAPIV3Schema:\n"
                        + "  properties:\n"
                        + "    template:\n"
                        + "      maxLength: 124\n"
                        + "      type: string\n"
                        + "  type: object",
                "Other property mismatch for .template");
        expectError(
                "openAPIV3Schema:\n"
                        + "  properties:\n"
                        + "    template:\n"
                        + "      type: string\n"
                        + "  type: object",
                "openAPIV3Schema:\n"
                        + "  properties:\n"
                        + "    template:\n"
                        + "      maxLength: 124\n"
                        + "      type: string\n"
                        + "  type: object",
                "Other property mismatch for .template");

        expectError(
                "openAPIV3Schema:\n"
                        + "  properties:\n"
                        + "    template:\n"
                        + "      maxLength: 123\n"
                        + "      type: string\n"
                        + "  type: object",
                "openAPIV3Schema:\n"
                        + "  properties:\n"
                        + "    template:\n"
                        + "      type: string\n"
                        + "  type: object",
                "Other property mismatch for .template");

        expectSuccess(
                "openAPIV3Schema:\n"
                        + "  properties:\n"
                        + "    template:\n"
                        + "      maxLength: 123\n"
                        + "      type: string\n"
                        + "  type: object",
                "openAPIV3Schema:\n"
                        + "  properties:\n"
                        + "    template:\n"
                        + "      maxLength: 123\n"
                        + "      type: string\n"
                        + "  type: object");
    }

    @Test
    public void testCreateFlinkSessionJobIgnoreUnknownFields() throws IOException {
        FlinkSessionJob flinkSessionJobWithUnknownFields =
                objectMapper.readValue(
                        new File("src/test/resources/test-session-job-with-unknown-fields.yaml"),
                        FlinkSessionJob.class);
        FlinkSessionJob flinkSessionJob =
                objectMapper.readValue(
                        new File("src/test/resources/test-session-job.yaml"),
                        FlinkSessionJob.class);
        assertEquals(flinkSessionJobWithUnknownFields.toString(), flinkSessionJob.toString());
    }

    @Test
    public void testCreateFlinkDeploymentIgnoreUnknownFields() throws IOException {
        FlinkDeployment flinkDeploymentWithUnknownFields =
                objectMapper.readValue(
                        new File("src/test/resources/test-deployment-with-unknown-fields.yaml"),
                        FlinkDeployment.class);
        FlinkDeployment flinkDeployment =
                objectMapper.readValue(
                        new File("src/test/resources/test-deployment.yaml"), FlinkDeployment.class);
        assertEquals(flinkDeploymentWithUnknownFields.toString(), flinkDeployment.toString());
    }

    @Test
    public void testGetSchemaFromUrl() throws IOException {
        var fileUrl =
                "file://" + new File("src/test/resources/test-deployment.yaml").getAbsolutePath();
        var httpUrl =
                "https://raw.githubusercontent.com/apache/flink-kubernetes-operator/release-1.4.0/helm/flink-kubernetes-operator/crds/flinkdeployments.flink.apache.org-v1.yml";
        assertEquals("file", new URL(fileUrl).getProtocol());
        assertEquals("https", new URL(httpUrl).getProtocol());
        var fileNode = objectMapper.readTree(new File(fileUrl.substring(7)));
        var httpNode = objectMapper.readTree(new URL(httpUrl));
        assertEquals("FlinkDeployment", fileNode.get("kind").asText());
        assertEquals("CustomResourceDefinition", httpNode.get("kind").asText());
    }

    private void expectSuccess(String oldSchema, String newSchema) throws JsonProcessingException {
        var oldNode = objectMapper.readTree(oldSchema).get("openAPIV3Schema");
        var newNode = objectMapper.readTree(newSchema).get("openAPIV3Schema");
        CrdCompatibilityChecker.checkObjectCompatibility("", oldNode, newNode);
    }

    private void expectError(String oldSchema, String newSchema, String err)
            throws JsonProcessingException {
        var oldNode = objectMapper.readTree(oldSchema).get("openAPIV3Schema");
        var newNode = objectMapper.readTree(newSchema).get("openAPIV3Schema");

        try {
            CrdCompatibilityChecker.checkObjectCompatibility("", oldNode, newNode);
            fail();
        } catch (CrdCompatibilityChecker.CompatibilityError ce) {
            assertEquals(err, ce.getMessage());
        }
    }
}
