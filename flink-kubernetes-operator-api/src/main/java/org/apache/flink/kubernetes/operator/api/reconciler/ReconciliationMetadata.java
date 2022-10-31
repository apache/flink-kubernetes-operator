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

package org.apache.flink.kubernetes.operator.api.reconciler;

import org.apache.flink.kubernetes.operator.api.AbstractFlinkResource;
import org.apache.flink.kubernetes.operator.api.status.ReconciliationState;

import com.fasterxml.jackson.annotation.JsonInclude;
import io.fabric8.kubernetes.api.model.ObjectMeta;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/** Extra metadata to be attached to the reconciled spec. */
@Data
@NoArgsConstructor
@AllArgsConstructor
@JsonInclude(JsonInclude.Include.NON_NULL)
public class ReconciliationMetadata {

    private String apiVersion;

    private ObjectMeta metadata;

    private boolean firstDeployment;

    public static ReconciliationMetadata from(AbstractFlinkResource<?, ?> resource) {
        ObjectMeta metadata = new ObjectMeta();
        metadata.setGeneration(resource.getMetadata().getGeneration());

        var firstDeploy =
                resource.getStatus().getReconciliationStatus().isBeforeFirstDeployment()
                        || isFirstDeployment(resource);

        return new ReconciliationMetadata(resource.getApiVersion(), metadata, firstDeploy);
    }

    private static boolean isFirstDeployment(AbstractFlinkResource<?, ?> resource) {
        var reconStatus = resource.getStatus().getReconciliationStatus();
        return reconStatus.getState() == ReconciliationState.DEPLOYED
                && reconStatus.deserializeLastReconciledSpecWithMeta().getMeta().firstDeployment;
    }
}
