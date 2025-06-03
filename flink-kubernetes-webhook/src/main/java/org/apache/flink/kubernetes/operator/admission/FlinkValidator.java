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

import org.apache.flink.kubernetes.operator.admission.informer.InformerManager;
import org.apache.flink.kubernetes.operator.api.AbstractFlinkResource;
import org.apache.flink.kubernetes.operator.api.CrdConstants;
import org.apache.flink.kubernetes.operator.api.FlinkDeployment;
import org.apache.flink.kubernetes.operator.api.FlinkSessionJob;
import org.apache.flink.kubernetes.operator.api.FlinkStateSnapshot;
import org.apache.flink.kubernetes.operator.api.spec.JobKind;
import org.apache.flink.kubernetes.operator.health.CanaryResourceManager;
import org.apache.flink.kubernetes.operator.utils.FlinkStateSnapshotUtils;
import org.apache.flink.kubernetes.operator.validation.FlinkResourceValidator;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.KubernetesResource;
import io.fabric8.kubernetes.client.informers.cache.Cache;
import io.javaoperatorsdk.webhook.admission.NotAllowedException;
import io.javaoperatorsdk.webhook.admission.Operation;
import io.javaoperatorsdk.webhook.admission.validation.Validator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;
import java.util.Set;

/** Validator for FlinkDeployment creation and updates. */
public class FlinkValidator implements Validator<HasMetadata> {
    private static final Logger LOG = LoggerFactory.getLogger(FlinkValidator.class);
    private static final ObjectMapper objectMapper = new ObjectMapper();

    private final Set<FlinkResourceValidator> validators;
    private final InformerManager informerManager;

    public FlinkValidator(Set<FlinkResourceValidator> validators, InformerManager informerManager) {
        this.validators = validators;
        this.informerManager = informerManager;
    }

    @Override
    public void validate(HasMetadata resource, HasMetadata oldResource, Operation operation)
            throws NotAllowedException {
        LOG.debug("Validating resource {}", resource);

        if (CanaryResourceManager.isCanaryResource(resource)) {
            return;
        }

        if (CrdConstants.KIND_FLINK_DEPLOYMENT.equals(resource.getKind())) {
            validateDeployment(resource);
        } else if (CrdConstants.KIND_SESSION_JOB.equals(resource.getKind())) {
            validateSessionJob(resource);
        } else if (CrdConstants.KIND_FLINK_STATE_SNAPSHOT.equals(resource.getKind())) {
            validateStateSnapshot(resource);
        } else {
            throw new NotAllowedException("Unexpected resource: " + resource.getKind());
        }
    }

    private void validateDeployment(KubernetesResource resource) {
        FlinkDeployment flinkDeployment =
                objectMapper.convertValue(resource, FlinkDeployment.class);
        for (FlinkResourceValidator validator : validators) {
            Optional<String> validationError = validator.validateDeployment(flinkDeployment);
            if (validationError.isPresent()) {
                throw new NotAllowedException(validationError.get());
            }
        }
    }

    private void validateSessionJob(KubernetesResource resource) {
        FlinkSessionJob sessionJob = objectMapper.convertValue(resource, FlinkSessionJob.class);
        var namespace = sessionJob.getMetadata().getNamespace();
        var deploymentName = sessionJob.getSpec().getDeploymentName();

        var key = Cache.namespaceKeyFunc(namespace, deploymentName);
        var deployment = informerManager.getFlinkDepInformer(namespace).getStore().getByKey(key);

        for (FlinkResourceValidator validator : validators) {
            Optional<String> validationError =
                    validator.validateSessionJob(sessionJob, Optional.ofNullable(deployment));
            if (validationError.isPresent()) {
                throw new NotAllowedException(validationError.get());
            }
        }
    }

    private void validateStateSnapshot(KubernetesResource resource) {
        FlinkStateSnapshot snapshot = objectMapper.convertValue(resource, FlinkStateSnapshot.class);

        var jobRef = snapshot.getSpec().getJobReference();

        AbstractFlinkResource<?, ?> targetResource = null;
        if (jobRef != null && jobRef.getName() != null && jobRef.getKind() != null) {
            var namespace =
                    FlinkStateSnapshotUtils.getSnapshotJobReferenceResourceId(snapshot)
                            .getNamespace()
                            .orElseThrow(
                                    () ->
                                            new IllegalArgumentException(
                                                    "Cannot determine namespace for snapshot"));
            var key = Cache.namespaceKeyFunc(namespace, jobRef.getName());

            if (JobKind.FLINK_DEPLOYMENT.equals(jobRef.getKind())) {
                targetResource =
                        informerManager.getFlinkDepInformer(namespace).getStore().getByKey(key);
            } else if (JobKind.FLINK_SESSION_JOB.equals(jobRef.getKind())) {
                targetResource =
                        informerManager
                                .getFlinkSessionJobInformer(namespace)
                                .getStore()
                                .getByKey(key);
            }
        }

        for (FlinkResourceValidator validator : validators) {
            Optional<String> validationError =
                    validator.validateStateSnapshot(snapshot, Optional.ofNullable(targetResource));
            if (validationError.isPresent()) {
                throw new NotAllowedException(validationError.get());
            }
        }
    }
}
