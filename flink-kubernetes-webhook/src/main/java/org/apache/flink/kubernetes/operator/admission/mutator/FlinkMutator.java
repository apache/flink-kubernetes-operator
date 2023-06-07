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

import org.apache.flink.kubernetes.operator.api.CrdConstants;
import org.apache.flink.kubernetes.operator.api.FlinkSessionJob;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.fabric8.kubernetes.api.model.HasMetadata;
import io.javaoperatorsdk.webhook.admission.NotAllowedException;
import io.javaoperatorsdk.webhook.admission.Operation;
import io.javaoperatorsdk.webhook.admission.mutation.Mutator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;

/** The default mutator. */
public class FlinkMutator implements Mutator<HasMetadata> {
    private static final Logger LOG = LoggerFactory.getLogger(FlinkMutator.class);
    private static final ObjectMapper mapper = new ObjectMapper();

    @Override
    public HasMetadata mutate(HasMetadata resource, Operation operation)
            throws NotAllowedException {
        if (operation == Operation.CREATE) {
            LOG.debug("Mutating resource {}", resource);

            if (CrdConstants.KIND_SESSION_JOB.equals(resource.getKind())) {
                try {
                    var sessionJob = mapper.convertValue(resource, FlinkSessionJob.class);
                    setSessionTargetLabel(sessionJob);
                    return sessionJob;
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
        }
        return resource;
    }

    private void setSessionTargetLabel(FlinkSessionJob flinkSessionJob) {
        var labels = flinkSessionJob.getMetadata().getLabels();
        if (labels == null) {
            labels = new HashMap<>();
        }
        var deploymentName = flinkSessionJob.getSpec().getDeploymentName();
        if (deploymentName != null
                && !deploymentName.equals(labels.get(CrdConstants.LABEL_TARGET_SESSION))) {
            labels.put(
                    CrdConstants.LABEL_TARGET_SESSION,
                    flinkSessionJob.getSpec().getDeploymentName());
            flinkSessionJob.getMetadata().setLabels(labels);
        }
    }
}
