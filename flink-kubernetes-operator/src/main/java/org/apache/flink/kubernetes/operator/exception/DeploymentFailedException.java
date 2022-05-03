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

package org.apache.flink.kubernetes.operator.exception;

import org.apache.flink.kubernetes.operator.crd.FlinkDeployment;

import io.fabric8.kubernetes.api.model.ContainerStateWaiting;
import io.fabric8.kubernetes.api.model.Event;
import io.fabric8.kubernetes.api.model.EventBuilder;
import io.fabric8.kubernetes.api.model.apps.DeploymentCondition;

/** Exception to signal terminal deployment failure. */
public class DeploymentFailedException extends RuntimeException {
    public static final String COMPONENT_JOBMANAGER = "JobManagerDeployment";
    public static final String REASON_CRASH_LOOP_BACKOFF = "CrashLoopBackOff";

    private static final long serialVersionUID = -1070179896083579221L;

    public final String component;
    public final String type;
    public final String reason;
    public final String lastTransitionTime;
    public final String lastUpdateTime;

    public DeploymentFailedException(String component, DeploymentCondition deployCondition) {
        super(deployCondition.getMessage());
        this.component = component;
        this.type = deployCondition.getType();
        this.reason = deployCondition.getReason();
        this.lastTransitionTime = deployCondition.getLastTransitionTime();
        this.lastUpdateTime = deployCondition.getLastUpdateTime();
    }

    public DeploymentFailedException(
            String component, String type, ContainerStateWaiting stateWaiting) {
        super(stateWaiting.getMessage());
        this.component = component;
        this.type = type;
        this.reason = stateWaiting.getReason();
        this.lastTransitionTime = null;
        this.lastUpdateTime = null;
    }

    public DeploymentFailedException(String component, String type, String error) {
        super(error);
        this.component = component;
        this.type = type;
        this.reason = error;
        this.lastTransitionTime = null;
        this.lastUpdateTime = null;
    }

    public static Event asEvent(DeploymentFailedException dfe, FlinkDeployment flinkApp) {
        EventBuilder evtb =
                new EventBuilder()
                        .withApiVersion("v1")
                        .withNewInvolvedObject()
                        .withKind(flinkApp.getKind())
                        .withName(flinkApp.getMetadata().getName())
                        .withNamespace(flinkApp.getMetadata().getNamespace())
                        .withUid(flinkApp.getMetadata().getUid())
                        .endInvolvedObject()
                        .withType(dfe.type)
                        .withReason(dfe.reason)
                        .withFirstTimestamp(dfe.lastTransitionTime)
                        .withLastTimestamp(dfe.lastUpdateTime)
                        .withMessage(dfe.getMessage())
                        .withNewMetadata()
                        .withGenerateName(flinkApp.getMetadata().getName())
                        .withNamespace(flinkApp.getMetadata().getNamespace())
                        .endMetadata()
                        .withNewSource()
                        .withComponent(dfe.component)
                        .endSource();
        return evtb.build();
    }
}
