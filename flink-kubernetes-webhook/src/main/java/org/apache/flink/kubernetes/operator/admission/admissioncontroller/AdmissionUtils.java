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

package org.apache.flink.kubernetes.operator.admission.admissioncontroller;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.fabric8.kubernetes.api.model.KubernetesResource;
import io.fabric8.kubernetes.api.model.admission.v1.AdmissionRequest;
import io.fabric8.kubernetes.api.model.admission.v1.AdmissionResponse;
import io.fabric8.zjsonpatch.JsonDiff;

import java.nio.charset.StandardCharsets;
import java.util.Base64;

/** Copied as is from https://github.com/java-operator-sdk/admission-controller-framework. */
public class AdmissionUtils {

    public static final String JSON_PATCH = "JSONPatch";
    private static final ObjectMapper mapper = new ObjectMapper();

    public static AdmissionResponse notAllowedExceptionToAdmissionResponse(
            NotAllowedException notAllowedException) {
        AdmissionResponse admissionResponse = new AdmissionResponse();
        admissionResponse.setAllowed(false);
        admissionResponse.setStatus(notAllowedException.getStatus());
        return admissionResponse;
    }

    public static KubernetesResource getTargetResource(
            AdmissionRequest admissionRequest, Operation operation) {
        return operation == Operation.DELETE
                ? admissionRequest.getOldObject()
                : admissionRequest.getObject();
    }

    public static AdmissionResponse admissionResponseFromMutation(
            KubernetesResource originalResource, KubernetesResource mutatedResource) {
        AdmissionResponse admissionResponse = new AdmissionResponse();
        admissionResponse.setAllowed(true);
        admissionResponse.setPatchType(JSON_PATCH);
        JsonNode originalResNode = mapper.valueToTree(originalResource);
        JsonNode mutatedResNode = mapper.valueToTree(mutatedResource);

        JsonNode diff = JsonDiff.asJson(originalResNode, mutatedResNode);
        String base64Diff =
                Base64.getEncoder()
                        .encodeToString(diff.toString().getBytes(StandardCharsets.UTF_8));
        admissionResponse.setPatch(base64Diff);
        return admissionResponse;
    }
}
