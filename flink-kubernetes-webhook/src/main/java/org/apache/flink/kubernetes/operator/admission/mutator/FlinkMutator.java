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

import org.apache.flink.kubernetes.operator.admission.informer.InformerManager;
import org.apache.flink.kubernetes.operator.api.CrdConstants;
import org.apache.flink.kubernetes.operator.api.FlinkDeployment;
import org.apache.flink.kubernetes.operator.api.FlinkSessionJob;
import org.apache.flink.kubernetes.operator.api.FlinkStateSnapshot;
import org.apache.flink.kubernetes.operator.mutator.FlinkResourceMutator;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.client.informers.cache.Cache;
import io.javaoperatorsdk.webhook.admission.NotAllowedException;
import io.javaoperatorsdk.webhook.admission.Operation;
import io.javaoperatorsdk.webhook.admission.mutation.Mutator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;
import java.util.Set;

/** The default mutator. */
public class FlinkMutator implements Mutator<HasMetadata> {
    private static final Logger LOG = LoggerFactory.getLogger(FlinkMutator.class);
    private static final ObjectMapper mapper = new ObjectMapper();
    private final Set<FlinkResourceMutator> mutators;
    private final InformerManager informerManager;

    public FlinkMutator(Set<FlinkResourceMutator> mutators, InformerManager informerManager) {
        this.mutators = mutators;
        this.informerManager = informerManager;
    }

    @Override
    public HasMetadata mutate(HasMetadata resource, Operation operation)
            throws NotAllowedException {
        if (operation == Operation.CREATE || operation == Operation.UPDATE) {
            LOG.debug("Mutating resource {}", resource);
            if (CrdConstants.KIND_SESSION_JOB.equals(resource.getKind())) {
                return mutateSessionJob(resource);
            }
            if (CrdConstants.KIND_FLINK_DEPLOYMENT.equals(resource.getKind())) {
                return mutateDeployment(resource);
            }
            if (CrdConstants.KIND_FLINK_STATE_SNAPSHOT.equals(resource.getKind())) {
                return mutateStateSnapshot(resource);
            }
        }
        return resource;
    }

    private FlinkSessionJob mutateSessionJob(HasMetadata resource) {
        try {
            var sessionJob = mapper.convertValue(resource, FlinkSessionJob.class);
            var namespace = sessionJob.getMetadata().getNamespace();
            var deploymentName = sessionJob.getSpec().getDeploymentName();
            var key = Cache.namespaceKeyFunc(namespace, deploymentName);
            var deployment =
                    informerManager.getFlinkDepInformer(namespace).getStore().getByKey(key);

            for (FlinkResourceMutator mutator : mutators) {
                sessionJob = mutator.mutateSessionJob(sessionJob, Optional.ofNullable(deployment));
            }

            return sessionJob;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private FlinkDeployment mutateDeployment(HasMetadata resource) {
        try {
            var flinkDeployment = mapper.convertValue(resource, FlinkDeployment.class);
            for (FlinkResourceMutator mutator : mutators) {
                flinkDeployment = mutator.mutateDeployment(flinkDeployment);
            }
            return flinkDeployment;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private FlinkStateSnapshot mutateStateSnapshot(HasMetadata resource) {
        try {
            var snapshot = mapper.convertValue(resource, FlinkStateSnapshot.class);
            for (var mutator : mutators) {
                snapshot = mutator.mutateStateSnapshot(snapshot);
            }
            return snapshot;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
