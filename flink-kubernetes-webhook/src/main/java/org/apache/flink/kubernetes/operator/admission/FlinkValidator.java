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

package org.apache.flink.kubernetes.operator.admission;

import org.apache.flink.kubernetes.operator.admission.admissioncontroller.NotAllowedException;
import org.apache.flink.kubernetes.operator.admission.admissioncontroller.Operation;
import org.apache.flink.kubernetes.operator.admission.admissioncontroller.validation.Validator;
import org.apache.flink.kubernetes.operator.crd.FlinkDeployment;
import org.apache.flink.kubernetes.operator.validation.FlinkResourceValidator;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.fabric8.kubernetes.api.model.GenericKubernetesResource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;

/** Validator for FlinkDeployment creation and updates. */
public class FlinkValidator implements Validator<GenericKubernetesResource> {
    private static final Logger LOG = LoggerFactory.getLogger(FlinkValidator.class);
    private static final ObjectMapper objectMapper = new ObjectMapper();

    private final FlinkResourceValidator deploymentValidator;

    public FlinkValidator(FlinkResourceValidator deploymentValidator) {
        this.deploymentValidator = deploymentValidator;
    }

    @Override
    public void validate(GenericKubernetesResource resource, Operation operation)
            throws NotAllowedException {
        LOG.debug("Validating resource {}", resource);

        FlinkDeployment flinkDeployment =
                objectMapper.convertValue(resource, FlinkDeployment.class);

        Optional<String> validationError = deploymentValidator.validateDeployment(flinkDeployment);
        if (validationError.isPresent()) {
            throw new NotAllowedException(validationError.get());
        }
    }
}
