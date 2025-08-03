/// *
// * Licensed to the Apache Software Foundation (ASF) under one or more
// * contributor license agreements.  See the NOTICE file distributed with
// * this work for additional information regarding copyright ownership.
// * The ASF licenses this file to You under the Apache License, Version 2.0
// * (the "License"); you may not use this file except in compliance with
// * the License.  You may obtain a copy of the License at
// *
// *     http://www.apache.org/licenses/LICENSE-2.0
// *
// * Unless required by applicable law or agreed to in writing, software
// * distributed under the License is distributed on an "AS IS" BASIS,
// * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// * See the License for the specific language governing permissions and
// * limitations under the License.
// */
//
// package org.apache.flink.kubernetes.operator.controller;
//
// import io.fabric8.kubernetes.api.model.StatusDetails;
// import io.javaoperatorsdk.operator.api.reconciler.Context;
// import org.apache.flink.api.common.JobStatus;
// import org.apache.flink.configuration.ConfigOption;
// import org.apache.flink.configuration.Configuration;
// import org.apache.flink.kubernetes.operator.api.FlinkBlueGreenDeployment;
// import org.apache.flink.kubernetes.operator.api.FlinkDeployment;
// import org.apache.flink.kubernetes.operator.api.lifecycle.ResourceLifecycleState;
// import org.apache.flink.kubernetes.operator.api.spec.FlinkBlueGreenDeploymentSpec;
// import org.apache.flink.kubernetes.operator.api.status.FlinkBlueGreenDeploymentStatus;
// import org.apache.flink.kubernetes.operator.api.utils.SpecUtils;
//
// import io.fabric8.kubernetes.api.model.ObjectMeta;
// import io.fabric8.kubernetes.api.model.OwnerReference;
//
// import java.time.Instant;
// import java.util.List;
// import java.util.Map;
//
// import static
// org.apache.flink.kubernetes.operator.api.spec.FlinkBlueGreenDeploymentConfigOptions.ABORT_GRACE_PERIOD;
// import static
// org.apache.flink.kubernetes.operator.api.spec.FlinkBlueGreenDeploymentConfigOptions.DEPLOYMENT_DELETION_DELAY;
// import static
// org.apache.flink.kubernetes.operator.api.spec.FlinkBlueGreenDeploymentConfigOptions.RECONCILIATION_RESCHEDULING_INTERVAL;
//
/// ** Utility methods for the FlinkBlueGreenDeploymentController. */
// public class FlinkBlueGreenDeploymentUtils {
//
//    public static ObjectMeta getDependentObjectMeta(FlinkBlueGreenDeployment bgDeployment) {
//        ObjectMeta bgMeta = bgDeployment.getMetadata();
//        ObjectMeta objectMeta = new ObjectMeta();
//        objectMeta.setNamespace(bgMeta.getNamespace());
//        objectMeta.setOwnerReferences(
//                List.of(
//                        new OwnerReference(
//                                bgDeployment.getApiVersion(),
//                                true,
//                                false,
//                                bgDeployment.getKind(),
//                                bgMeta.getName(),
//                                bgMeta.getUid())));
//        return objectMeta;
//    }
//
//    public static <T> T adjustNameReferences(
//            T spec,
//            String deploymentName,
//            String childDeploymentName,
//            String wrapperKey,
//            Class<T> valueType) {
//        String serializedSpec = SpecUtils.writeSpecAsJSON(spec, wrapperKey);
//        String replacedSerializedSpec = serializedSpec.replace(deploymentName,
// childDeploymentName);
//        return SpecUtils.readSpecFromJSON(replacedSerializedSpec, wrapperKey, valueType);
//    }
//
//    public static String millisToInstantStr(long millis) {
//        return Instant.ofEpochMilli(millis).toString();
//    }
//
//    public static long instantStrToMillis(String instant) {
//        if (instant == null) {
//            return 0;
//        }
//        return Instant.parse(instant).toEpochMilli();
//    }
//
//    public static <T> T getConfigOption(
//            FlinkBlueGreenDeployment bgDeployment, ConfigOption<T> option) {
//        Map<String, String> configuration =
// bgDeployment.getSpec().getTemplate().getConfiguration();
//
//        if (configuration == null) {
//            return option.defaultValue();
//        }
//
//        return Configuration.fromMap(configuration).get(option);
//    }
//
//    public static long getReconciliationReschedInterval(FlinkBlueGreenDeployment bgDeployment) {
//        return Math.max(
//                getConfigOption(bgDeployment, RECONCILIATION_RESCHEDULING_INTERVAL).toMillis(),
// 0);
//    }
//
//    public static long getDeploymentDeletionDelay(FlinkBlueGreenDeployment bgDeployment) {
//        return Math.max(getConfigOption(bgDeployment, DEPLOYMENT_DELETION_DELAY).toMillis(), 0);
//    }
//
//    public static long getAbortGracePeriod(FlinkBlueGreenDeployment bgDeployment) {
//        long abortGracePeriod = getConfigOption(bgDeployment, ABORT_GRACE_PERIOD).toMillis();
//        return Math.max(abortGracePeriod,
// FlinkBlueGreenDeploymentController.minimumAbortGracePeriodMs);
//    }
//
//    public static void setAbortTimestamp(
//            FlinkBlueGreenDeployment bgDeployment,
//            FlinkBlueGreenDeploymentStatus deploymentStatus) {
//        deploymentStatus.setAbortTimestamp(
//                millisToInstantStr(System.currentTimeMillis() +
// getAbortGracePeriod(bgDeployment)));
//    }
//
//    public static boolean deleteKubernetesDeployment(
//            FlinkDeployment currentDeployment,
//            Context<FlinkBlueGreenDeployment> josdkContext) {
//        String deploymentName = currentDeployment.getMetadata().getName();
//        List<StatusDetails> deletedStatus =
//                josdkContext
//                        .getClient()
//                        .resources(FlinkDeployment.class)
//                        .inNamespace(currentDeployment.getMetadata().getNamespace())
//                        .withName(deploymentName)
//                        .delete();
//
//        return deletedStatus.size() == 1
//                && deletedStatus.get(0).getKind().equals("FlinkDeployment");
//    }
//
//    public static boolean isDeploymentReady(FlinkDeployment deployment) {
//        return ResourceLifecycleState.STABLE == deployment.getStatus().getLifecycleState()
//                && JobStatus.RUNNING == deployment.getStatus().getJobStatus().getState();
//    }
//
//    public static boolean hasSpecChanged(
//            FlinkBlueGreenDeploymentSpec newSpec, FlinkBlueGreenDeploymentStatus deploymentStatus)
// {
//
//        String lastReconciledSpec = deploymentStatus.getLastReconciledSpec();
//        String newSpecSerialized = SpecUtils.writeSpecAsJSON(newSpec, "spec");
//
//        return !lastReconciledSpec.equals(newSpecSerialized);
//    }
// }
