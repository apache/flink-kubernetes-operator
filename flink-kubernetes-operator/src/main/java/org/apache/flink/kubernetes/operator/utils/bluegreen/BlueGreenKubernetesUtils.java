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

package org.apache.flink.kubernetes.operator.utils.bluegreen;

import org.apache.flink.api.common.JobStatus;
import org.apache.flink.kubernetes.operator.api.FlinkBlueGreenDeployment;
import org.apache.flink.kubernetes.operator.api.FlinkDeployment;
import org.apache.flink.kubernetes.operator.api.bluegreen.DeploymentType;
import org.apache.flink.kubernetes.operator.api.lifecycle.ResourceLifecycleState;
import org.apache.flink.kubernetes.operator.api.spec.JobState;
import org.apache.flink.kubernetes.operator.api.status.Savepoint;

import io.fabric8.kubernetes.api.model.ObjectMeta;
import io.fabric8.kubernetes.api.model.OwnerReference;
import io.fabric8.kubernetes.api.model.StatusDetails;
import io.javaoperatorsdk.operator.api.reconciler.Context;

import java.util.List;

import static org.apache.flink.kubernetes.operator.utils.bluegreen.BlueGreenSpecUtils.prepareFlinkDeployment;

/** Utility methods for handling Kubernetes operations in Blue/Green deployments. */
public class BlueGreenKubernetesUtils {

    /**
     * Creates ObjectMeta for a dependent Kubernetes resource with proper owner references.
     *
     * @param bgDeployment the parent Blue/Green deployment
     * @return ObjectMeta configured with namespace and owner references
     */
    public static ObjectMeta getDependentObjectMeta(FlinkBlueGreenDeployment bgDeployment) {
        ObjectMeta bgMeta = bgDeployment.getMetadata();
        ObjectMeta objectMeta = new ObjectMeta();
        objectMeta.setNamespace(bgMeta.getNamespace());
        objectMeta.setOwnerReferences(
                List.of(
                        new OwnerReference(
                                bgDeployment.getApiVersion(),
                                true,
                                false,
                                bgDeployment.getKind(),
                                bgMeta.getName(),
                                bgMeta.getUid())));
        return objectMeta;
    }

    /**
     * Deletes a Kubernetes FlinkDeployment resource.
     *
     * @param currentDeployment the FlinkDeployment to delete
     * @param josdkContext the JOSDK context for Kubernetes API access
     * @return true if the deployment was successfully deleted, false otherwise
     */
    public static boolean deleteKubernetesDeployment(
            FlinkDeployment currentDeployment, Context<FlinkBlueGreenDeployment> josdkContext) {
        String deploymentName = currentDeployment.getMetadata().getName();
        List<StatusDetails> deletedStatus =
                josdkContext
                        .getClient()
                        .resources(FlinkDeployment.class)
                        .inNamespace(currentDeployment.getMetadata().getNamespace())
                        .withName(deploymentName)
                        .delete();

        return deletedStatus.size() == 1
                && deletedStatus.get(0).getKind().equals("FlinkDeployment");
    }

    public static void deployCluster(
            FlinkBlueGreenDeployment bgDeployment,
            DeploymentType deploymentType,
            Savepoint lastCheckpoint,
            Context<FlinkBlueGreenDeployment> josdkContext,
            boolean isFirstDeployment) {
        ObjectMeta bgMeta = bgDeployment.getMetadata();

        FlinkDeployment flinkDeployment =
                prepareFlinkDeployment(
                        bgDeployment, deploymentType, lastCheckpoint, isFirstDeployment, bgMeta);

        // Deploy
        josdkContext.getClient().resource(flinkDeployment).createOrReplace();
    }

    /**
     * Checks if a FlinkDeployment is ready (STABLE lifecycle state and RUNNING job status).
     *
     * @param deployment the FlinkDeployment to check
     * @return true if the deployment is ready, false otherwise
     */
    public static boolean isDeploymentReady(FlinkDeployment deployment) {
        return ResourceLifecycleState.STABLE == deployment.getStatus().getLifecycleState()
                && JobStatus.RUNNING == deployment.getStatus().getJobStatus().getState();
    }

    public static void suspendDeployment(
            Context<FlinkBlueGreenDeployment> josdkContext, FlinkDeployment nextDeployment) {
        nextDeployment.getSpec().getJob().setState(JobState.SUSPENDED);
        updateFlinkDeployment(nextDeployment, josdkContext);
    }

    public static void updateFlinkDeployment(
            FlinkDeployment nextDeployment, Context<FlinkBlueGreenDeployment> josdkContext) {
        josdkContext.getClient().resource(nextDeployment).update();
    }

    public static void replaceFlinkBlueGreenDeployment(
            FlinkBlueGreenDeployment bgDeployment, Context<FlinkBlueGreenDeployment> josdkContext) {
        josdkContext.getClient().resource(bgDeployment).replace();
    }
}
