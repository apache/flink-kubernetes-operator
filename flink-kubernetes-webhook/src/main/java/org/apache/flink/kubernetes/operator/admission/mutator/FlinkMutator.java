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
import org.apache.flink.kubernetes.operator.api.FlinkMiniCluster;
import org.apache.flink.kubernetes.operator.api.FlinkSessionJob;
import org.apache.flink.kubernetes.operator.api.FlinkStateSnapshot;
import org.apache.flink.kubernetes.operator.api.mutator.FlinkResourceMutator;

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
import java.util.function.BiFunction;

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
            try {
                if (CrdConstants.KIND_SESSION_JOB.equals(resource.getKind())) {
                    return mutateSessionJob(resource);
                }
                if (CrdConstants.KIND_FLINK_DEPLOYMENT.equals(resource.getKind())) {
                    return mutateDeployment(resource);
                }
                if (CrdConstants.KIND_FLINK_STATE_SNAPSHOT.equals(resource.getKind())) {
                    return mutateStateSnapshot(resource);
                }
                if (CrdConstants.KIND_FLINK_MINI_CLUSTER.equals(resource.getKind())) {
                    return mutateMiniCluster(resource);
                }
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
        return resource;
    }

    private HasMetadata mutateSessionJob(HasMetadata resource) {
        var sessionJob = mapper.convertValue(resource, FlinkSessionJob.class);
        var namespace = sessionJob.getMetadata().getNamespace();
        var deploymentName = sessionJob.getSpec().getDeploymentName();
        var key = Cache.namespaceKeyFunc(namespace, deploymentName);
        var deployment =
                Optional.ofNullable(
                        informerManager.getFlinkDepInformer(namespace).getStore().getByKey(key));
        return applyMutators(
                resource, sessionJob, (mutator, job) -> mutator.mutateSessionJob(job, deployment));
    }

    private HasMetadata mutateDeployment(HasMetadata resource) {
        var flinkDeployment = mapper.convertValue(resource, FlinkDeployment.class);
        return applyMutators(resource, flinkDeployment, FlinkResourceMutator::mutateDeployment);
    }

    private HasMetadata mutateMiniCluster(HasMetadata resource) {
        var miniCluster = mapper.convertValue(resource, FlinkMiniCluster.class);
        return applyMutators(resource, miniCluster, FlinkResourceMutator::mutateMiniCluster);
    }

    private HasMetadata mutateStateSnapshot(HasMetadata resource) {
        var snapshot = mapper.convertValue(resource, FlinkStateSnapshot.class);
        return applyMutators(resource, snapshot, FlinkResourceMutator::mutateStateSnapshot);
    }

    private <T extends HasMetadata> HasMetadata applyMutators(
            HasMetadata resource, T converted, BiFunction<FlinkResourceMutator, T, T> mutation) {
        var original = mapper.valueToTree(converted);
        var mutated = converted;
        for (FlinkResourceMutator mutator : mutators) {
            mutated = mutation.apply(mutator, mutated);
        }
        // Return the original resource if no mutation was applied to avoid
        // the serialization round-trip producing a different object, which causes
        // kubectl to inconsistently omit the "(no change)" suffix on patch responses
        if (original.equals(mapper.valueToTree(mutated))) {
            return resource;
        }
        return mutated;
    }
}
