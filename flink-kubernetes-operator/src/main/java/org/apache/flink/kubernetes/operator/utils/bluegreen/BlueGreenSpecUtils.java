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

import org.apache.flink.api.common.JobID;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.kubernetes.operator.api.FlinkBlueGreenDeployment;
import org.apache.flink.kubernetes.operator.api.FlinkDeployment;
import org.apache.flink.kubernetes.operator.api.bluegreen.BlueGreenDiffType;
import org.apache.flink.kubernetes.operator.api.bluegreen.DeploymentType;
import org.apache.flink.kubernetes.operator.api.spec.FlinkBlueGreenDeploymentSpec;
import org.apache.flink.kubernetes.operator.api.spec.KubernetesDeploymentMode;
import org.apache.flink.kubernetes.operator.api.status.FlinkBlueGreenDeploymentStatus;
import org.apache.flink.kubernetes.operator.api.status.Savepoint;
import org.apache.flink.kubernetes.operator.api.utils.SpecUtils;
import org.apache.flink.kubernetes.operator.controller.FlinkResourceContext;
import org.apache.flink.kubernetes.operator.controller.bluegreen.BlueGreenContext;
import org.apache.flink.kubernetes.operator.reconciler.diff.FlinkBlueGreenDeploymentSpecDiff;

import io.fabric8.kubernetes.api.model.ObjectMeta;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.Map;
import java.util.Optional;

import static org.apache.flink.kubernetes.operator.controller.bluegreen.BlueGreenKubernetesService.getDependentObjectMeta;
import static org.apache.flink.kubernetes.operator.controller.bluegreen.BlueGreenKubernetesService.replaceFlinkBlueGreenDeployment;

/** Utility methods for handling Blue/Green deployment specifications. */
public class BlueGreenSpecUtils {

    private static final Logger LOG = LoggerFactory.getLogger(BlueGreenSpecUtils.class);

    /**
     * Adjusts name references in a spec by replacing deployment names with child deployment names.
     *
     * @param spec the spec to adjust
     * @param deploymentName the original deployment name
     * @param childDeploymentName the child deployment name to replace with
     * @param wrapperKey the JSON wrapper key
     * @param valueType the spec type
     * @return adjusted spec with name references updated
     */
    public static <T> T adjustNameReferences(
            T spec,
            String deploymentName,
            String childDeploymentName,
            String wrapperKey,
            Class<T> valueType) {
        String serializedSpec = SpecUtils.writeSpecAsJSON(spec, wrapperKey);
        String replacedSerializedSpec = serializedSpec.replace(deploymentName, childDeploymentName);
        return SpecUtils.readSpecFromJSON(replacedSerializedSpec, wrapperKey, valueType);
    }

    /**
     * Checks if the Blue/Green deployment spec has changed compared to the last reconciled spec.
     *
     * @param context the Blue/Green transition context
     * @return true if the spec has changed, false otherwise
     */
    public static boolean hasSpecChanged(BlueGreenContext context) {

        FlinkBlueGreenDeploymentStatus deploymentStatus = context.getDeploymentStatus();
        String lastReconciledSpec = deploymentStatus.getLastReconciledSpec();
        FlinkBlueGreenDeploymentSpec lastSpec =
                SpecUtils.readSpecFromJSON(
                        lastReconciledSpec, "spec", FlinkBlueGreenDeploymentSpec.class);

        FlinkBlueGreenDeploymentSpecDiff diff =
                new FlinkBlueGreenDeploymentSpecDiff(
                        KubernetesDeploymentMode.NATIVE,
                        lastSpec,
                        context.getBgDeployment().getSpec());

        BlueGreenDiffType diffType = diff.compare();

        return diffType != BlueGreenDiffType.IGNORE;
    }

    /**
     * Extracts a configuration option value from the Blue/Green deployment spec.
     *
     * @param bgDeployment the Blue/Green deployment
     * @param option the configuration option to extract
     * @return the configuration value or default if not found
     */
    public static <T> T getConfigOption(
            FlinkBlueGreenDeployment bgDeployment, ConfigOption<T> option) {
        Map<String, String> configuration = bgDeployment.getSpec().getTemplate().getConfiguration();

        if (configuration == null) {
            return option.defaultValue();
        }

        return Configuration.fromMap(configuration).get(option);
    }

    public static FlinkDeployment prepareFlinkDeployment(
            FlinkBlueGreenDeployment bgDeployment,
            DeploymentType deploymentType,
            Savepoint lastCheckpoint,
            boolean isFirstDeployment,
            ObjectMeta bgMeta) {
        // Deployment
        FlinkDeployment flinkDeployment = new FlinkDeployment();
        FlinkBlueGreenDeploymentSpec spec = bgDeployment.getSpec();

        String childDeploymentName =
                bgMeta.getName() + "-" + deploymentType.toString().toLowerCase();

        FlinkBlueGreenDeploymentSpec adjustedSpec =
                adjustNameReferences(
                        spec,
                        bgMeta.getName(),
                        childDeploymentName,
                        "spec",
                        FlinkBlueGreenDeploymentSpec.class);

        // The B/G initialSavepointPath is only used in first time deployments
        if (isFirstDeployment) {
            String initialSavepointPath =
                    adjustedSpec.getTemplate().getSpec().getJob().getInitialSavepointPath();
            if (initialSavepointPath != null && !initialSavepointPath.isEmpty()) {
                LOG.info("Using initialSavepointPath: " + initialSavepointPath);
                adjustedSpec
                        .getTemplate()
                        .getSpec()
                        .getJob()
                        .setInitialSavepointPath(initialSavepointPath);
            } else {
                LOG.info("Clean start up, no checkpoint/savepoint");
            }
        } else if (lastCheckpoint != null) {
            String location = lastCheckpoint.getLocation().replace("file:", "");
            LOG.info("Using B/G checkpoint: " + location);
            adjustedSpec.getTemplate().getSpec().getJob().setInitialSavepointPath(location);
        }

        flinkDeployment.setSpec(adjustedSpec.getTemplate().getSpec());

        // Deployment metadata
        ObjectMeta flinkDeploymentMeta = getDependentObjectMeta(bgDeployment);
        flinkDeploymentMeta.setName(childDeploymentName);
        flinkDeploymentMeta.setLabels(Map.of(DeploymentType.LABEL_KEY, deploymentType.toString()));
        flinkDeployment.setMetadata(flinkDeploymentMeta);
        return flinkDeployment;
    }

    public static void setLastReconciledSpec(BlueGreenContext context) {
        FlinkBlueGreenDeploymentStatus deploymentStatus = context.getDeploymentStatus();
        deploymentStatus.setLastReconciledSpec(
                SpecUtils.writeSpecAsJSON(context.getBgDeployment().getSpec(), "spec"));
        deploymentStatus.setLastReconciledTimestamp(Instant.now().toString());
    }

    public static Savepoint configureSavepoint(
            FlinkResourceContext<FlinkDeployment> resourceContext) {
        // TODO: if the user specified an initialSavepointPath, use it and skip this?
        Optional<Savepoint> lastCheckpoint =
                resourceContext
                        .getFlinkService()
                        .getLastCheckpoint(
                                JobID.fromHexString(
                                        resourceContext
                                                .getResource()
                                                .getStatus()
                                                .getJobStatus()
                                                .getJobId()),
                                resourceContext.getObserveConfig());

        // Alternative action if no checkpoint is available?
        if (lastCheckpoint.isEmpty()) {
            throw new IllegalStateException(
                    "Last Checkpoint for Job "
                            + resourceContext.getResource().getMetadata().getName()
                            + " not found!");
        }

        return lastCheckpoint.get();
    }

    public static void revertToLastSpec(BlueGreenContext context) {
        context.getBgDeployment()
                .setSpec(
                        SpecUtils.readSpecFromJSON(
                                context.getDeploymentStatus().getLastReconciledSpec(),
                                "spec",
                                FlinkBlueGreenDeploymentSpec.class));
        replaceFlinkBlueGreenDeployment(context);
    }
}
