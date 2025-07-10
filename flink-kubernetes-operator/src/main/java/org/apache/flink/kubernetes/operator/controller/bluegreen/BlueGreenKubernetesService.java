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

package org.apache.flink.kubernetes.operator.controller.bluegreen;

import org.apache.flink.kubernetes.operator.api.FlinkBlueGreenDeployment;
import org.apache.flink.kubernetes.operator.api.FlinkDeployment;
import org.apache.flink.kubernetes.operator.api.lifecycle.ResourceLifecycleState;
import org.apache.flink.kubernetes.operator.api.spec.JobState;
import org.apache.flink.kubernetes.operator.reconciler.ReconciliationUtils;

import io.fabric8.kubernetes.api.model.ObjectMeta;
import io.fabric8.kubernetes.api.model.OwnerReference;
import io.fabric8.kubernetes.api.model.StatusDetails;

import java.util.List;

/** Utility methods for handling Kubernetes operations in Blue/Green deployments. */
public class BlueGreenKubernetesService {

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

    public static void deployCluster(BlueGreenContext context, FlinkDeployment flinkDeployment) {
        // Deploy
        context.getJosdkContext().getClient().resource(flinkDeployment).createOrReplace();
    }

    /**
     * Checks if a FlinkDeployment is ready (STABLE lifecycle state and RUNNING job status).
     *
     * @param deployment the FlinkDeployment to check
     * @return true if the deployment is ready, false otherwise
     */
    public static boolean isFlinkDeploymentReady(FlinkDeployment deployment) {
        return ResourceLifecycleState.STABLE == deployment.getStatus().getLifecycleState()
                && ReconciliationUtils.isJobRunning(deployment.getStatus());
    }

    public static void suspendFlinkDeployment(
            BlueGreenContext context, FlinkDeployment nextDeployment) {
        nextDeployment.getSpec().getJob().setState(JobState.SUSPENDED);
        updateFlinkDeployment(nextDeployment, context);
    }

    public static void updateFlinkDeployment(
            FlinkDeployment nextDeployment, BlueGreenContext context) {
        String namespace = context.getBgDeployment().getMetadata().getNamespace();
        context.getJosdkContext()
                .getClient()
                .resource(nextDeployment)
                .inNamespace(namespace)
                .update();
    }

    public static void replaceFlinkBlueGreenDeployment(BlueGreenContext context) {
        String namespace = context.getBgDeployment().getMetadata().getNamespace();
        context.getJosdkContext()
                .getClient()
                .resource(context.getBgDeployment())
                .inNamespace(namespace)
                .replace();
    }

    /**
     * Deletes a Kubernetes FlinkDeployment resource.
     *
     * @param currentDeployment the FlinkDeployment to delete
     * @param context the Blue/Green transition context
     * @return true if the deployment was successfully deleted, false otherwise
     */
    public static boolean deleteFlinkDeployment(
            FlinkDeployment currentDeployment, BlueGreenContext context) {
        String deploymentName = currentDeployment.getMetadata().getName();
        List<StatusDetails> deletedStatus =
                context.getJosdkContext()
                        .getClient()
                        .resources(FlinkDeployment.class)
                        .inNamespace(currentDeployment.getMetadata().getNamespace())
                        .withName(deploymentName)
                        .delete();

        return deletedStatus.size() == 1
                && deletedStatus.get(0).getKind().equals("FlinkDeployment");
    }
}
