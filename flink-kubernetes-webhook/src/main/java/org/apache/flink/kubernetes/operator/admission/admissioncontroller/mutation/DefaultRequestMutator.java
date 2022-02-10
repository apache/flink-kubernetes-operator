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

package org.apache.flink.kubernetes.operator.admission.admissioncontroller.mutation;

import org.apache.flink.kubernetes.operator.admission.admissioncontroller.AdmissionUtils;
import org.apache.flink.kubernetes.operator.admission.admissioncontroller.NotAllowedException;
import org.apache.flink.kubernetes.operator.admission.admissioncontroller.Operation;
import org.apache.flink.kubernetes.operator.admission.admissioncontroller.RequestHandler;
import org.apache.flink.kubernetes.operator.admission.admissioncontroller.clone.Cloner;
import org.apache.flink.kubernetes.operator.admission.admissioncontroller.clone.ObjectMapperCloner;

import io.fabric8.kubernetes.api.model.KubernetesResource;
import io.fabric8.kubernetes.api.model.admission.v1.AdmissionRequest;
import io.fabric8.kubernetes.api.model.admission.v1.AdmissionResponse;

import static org.apache.flink.kubernetes.operator.admission.admissioncontroller.AdmissionUtils.admissionResponseFromMutation;
import static org.apache.flink.kubernetes.operator.admission.admissioncontroller.AdmissionUtils.getTargetResource;

/** Copied as is from https://github.com/java-operator-sdk/admission-controller-framework. */
public class DefaultRequestMutator<T extends KubernetesResource> implements RequestHandler {

    private final Mutator<T> mutator;
    private final Cloner<T> cloner;

    public DefaultRequestMutator(Mutator<T> mutator) {
        this(mutator, new ObjectMapperCloner<>());
    }

    public DefaultRequestMutator(Mutator<T> mutator, Cloner<T> cloner) {
        this.mutator = mutator;
        this.cloner = cloner;
    }

    @Override
    public AdmissionResponse handle(AdmissionRequest admissionRequest) {
        Operation operation = Operation.valueOf(admissionRequest.getOperation());
        T originalResource = (T) getTargetResource(admissionRequest, operation);
        T clonedResource = cloner.clone(originalResource);
        AdmissionResponse admissionResponse;
        try {
            T mutatedResource = mutator.mutate(clonedResource, operation);
            admissionResponse = admissionResponseFromMutation(originalResource, mutatedResource);
        } catch (NotAllowedException e) {
            admissionResponse = AdmissionUtils.notAllowedExceptionToAdmissionResponse(e);
        }
        return admissionResponse;
    }
}
