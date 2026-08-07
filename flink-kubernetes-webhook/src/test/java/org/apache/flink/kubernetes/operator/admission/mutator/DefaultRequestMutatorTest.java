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

package org.apache.flink.kubernetes.operator.admission.mutator;

import org.apache.flink.kubernetes.operator.api.FlinkDeployment;
import org.apache.flink.kubernetes.operator.api.spec.FlinkDeploymentSpec;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.ObjectMeta;
import io.fabric8.kubernetes.api.model.admission.v1.AdmissionRequest;
import io.javaoperatorsdk.webhook.admission.NotAllowedException;
import io.javaoperatorsdk.webhook.admission.Operation;
import io.javaoperatorsdk.webhook.admission.mutation.Mutator;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.HashMap;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Tests for {@link DefaultRequestMutator}. */
class DefaultRequestMutatorTest {

    private static final ObjectMapper mapper = new ObjectMapper();

    @Test
    void handleWithNoMutationProducesEmptyPatch() {
        var requestMutator = createRequestMutator((resource, operation) -> resource);
        var request = createAdmissionRequest(createDeployment());

        var response = requestMutator.handle(request);

        assertTrue(response.getAllowed());
        assertEquals("JSONPatch", response.getPatchType());
        var patch = decodePatch(response.getPatch());
        assertEquals("[]", patch, "No-op mutation should produce an empty JSON patch array");
    }

    @Test
    void handleWithLabelMutationProducesPatchWithAddOperation() throws Exception {
        var requestMutator =
                createRequestMutator(
                        (resource, operation) -> {
                            var labels = resource.getMetadata().getLabels();
                            if (labels == null) {
                                labels = new HashMap<>();
                            }
                            labels.put("injected", "true");
                            resource.getMetadata().setLabels(labels);
                            return resource;
                        });
        var request = createAdmissionRequest(createDeployment());

        var response = requestMutator.handle(request);

        assertTrue(response.getAllowed());
        var patch = decodePatch(response.getPatch());
        var patchNodes = mapper.readTree(patch);
        assertFalse(patchNodes.isEmpty(), "Mutation should produce at least one patch operation");
        assertTrue(patch.contains("injected"));
    }

    @Test
    void handleWithSpecMutationProducesPatchWithReplaceOperation() {
        var requestMutator =
                createRequestMutator(
                        (resource, operation) -> {
                            ((FlinkDeployment) resource).getSpec().setImage("mutated:latest");
                            return resource;
                        });
        var deployment = createDeployment();
        deployment.getSpec().setImage("original:1.0");
        var request = createAdmissionRequest(deployment);

        var response = requestMutator.handle(request);

        assertTrue(response.getAllowed());
        assertTrue(decodePatch(response.getPatch()).contains("mutated:latest"));
    }

    @Test
    void handleWithNotAllowedExceptionReturnsNotAllowedResponse() {
        var requestMutator =
                createRequestMutator(
                        (resource, operation) -> {
                            throw new NotAllowedException("rejected for testing");
                        });
        var request = createAdmissionRequest(createDeployment());

        var response = requestMutator.handle(request);

        assertFalse(response.getAllowed());
        assertNotNull(response.getStatus());
        assertTrue(response.getStatus().getMessage().contains("rejected for testing"));
    }

    private FlinkDeployment createDeployment() {
        var deployment = new FlinkDeployment();
        var meta = new ObjectMeta();
        meta.setName("test-deployment");
        meta.setNamespace("default");
        deployment.setMetadata(meta);
        deployment.setSpec(new FlinkDeploymentSpec());
        return deployment;
    }

    private AdmissionRequest createAdmissionRequest(FlinkDeployment resource) {
        var request = new AdmissionRequest();
        request.setOperation(Operation.CREATE.name());
        request.setObject(resource);
        return request;
    }

    private String decodePatch(String base64Patch) {
        return new String(Base64.getDecoder().decode(base64Patch), StandardCharsets.UTF_8);
    }

    private DefaultRequestMutator<HasMetadata> createRequestMutator(Mutator<HasMetadata> mutator) {
        return new DefaultRequestMutator<>(mutator);
    }
}
