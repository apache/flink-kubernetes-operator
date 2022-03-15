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

import io.fabric8.kubernetes.api.model.Event;
import io.fabric8.kubernetes.api.model.EventBuilder;
import io.fabric8.kubernetes.api.model.apps.DeploymentCondition;

/** Exception to signal terminal deployment failure. */
public class DeploymentFailedException extends RuntimeException {
    public static final String COMPONENT_JOBMANAGER = "JobManagerDeployment";
    private static final long serialVersionUID = -1070179896083579221L;

    public final String component;
    public final DeploymentCondition deployCondition;

    public DeploymentFailedException(String component, DeploymentCondition deployCondition) {
        super(deployCondition.getMessage());
        this.component = component;
        this.deployCondition = deployCondition;
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
                        .withType(dfe.deployCondition.getType())
                        .withReason(dfe.deployCondition.getReason())
                        .withFirstTimestamp(dfe.deployCondition.getLastTransitionTime())
                        .withLastTimestamp(dfe.deployCondition.getLastUpdateTime())
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
