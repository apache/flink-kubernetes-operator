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

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.fabric8.kubernetes.api.model.KubernetesResource;
import io.fabric8.kubernetes.api.model.admission.v1.AdmissionRequest;
import io.fabric8.kubernetes.api.model.admission.v1.AdmissionResponse;
import io.fabric8.zjsonpatch.JsonDiff;
import io.javaoperatorsdk.webhook.admission.AdmissionRequestHandler;
import io.javaoperatorsdk.webhook.admission.AdmissionUtils;
import io.javaoperatorsdk.webhook.admission.NotAllowedException;
import io.javaoperatorsdk.webhook.admission.Operation;
import io.javaoperatorsdk.webhook.admission.mutation.Mutator;
import io.javaoperatorsdk.webhook.clone.Cloner;
import io.javaoperatorsdk.webhook.clone.ObjectMapperCloner;

import java.nio.charset.StandardCharsets;
import java.util.Base64;

/**
 * The default request mutator. It's copied from the {@link DefaultRequestMutator} with a modified
 * path diff util to serialize out include non-null.
 *
 * @param <T> Resource type.
 */
public class DefaultRequestMutator<T extends KubernetesResource>
        implements AdmissionRequestHandler {
    private static final ObjectMapper mapper =
            new ObjectMapper().setSerializationInclusion(JsonInclude.Include.NON_NULL);
    private final Mutator<T> mutator;
    private final Cloner<T> cloner;

    public DefaultRequestMutator(Mutator<T> mutator) {
        this(mutator, new ObjectMapperCloner<>());
    }

    public DefaultRequestMutator(Mutator<T> mutator, Cloner<T> cloner) {
        this.mutator = mutator;
        this.cloner = cloner;
    }

    public AdmissionResponse handle(AdmissionRequest admissionRequest) {
        Operation operation = Operation.valueOf(admissionRequest.getOperation());
        KubernetesResource originalResource =
                AdmissionUtils.getTargetResource(admissionRequest, operation);
        T clonedResource = this.cloner.clone((T) originalResource);

        AdmissionResponse admissionResponse;
        try {
            T mutatedResource = this.mutator.mutate(clonedResource, operation);
            admissionResponse = admissionResponseFromMutation(originalResource, mutatedResource);
        } catch (NotAllowedException e) {
            admissionResponse = AdmissionUtils.notAllowedExceptionToAdmissionResponse(e);
        }

        return admissionResponse;
    }

    public static AdmissionResponse admissionResponseFromMutation(
            KubernetesResource originalResource, KubernetesResource mutatedResource) {
        AdmissionResponse admissionResponse = new AdmissionResponse();
        admissionResponse.setAllowed(true);
        // It only allowed JSONPatch now, So we should avoid serialize out null value
        // https://github.com/kubernetes/kubernetes/blob/3f1a9f9f3eaeae3d387b9152ea9aebb52be72319/pkg/apis/admission/types.go#L134
        admissionResponse.setPatchType("JSONPatch");
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
