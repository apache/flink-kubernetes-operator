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
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.kubernetes.operator.api.FlinkBlueGreenDeployment;
import org.apache.flink.kubernetes.operator.api.FlinkDeployment;
import org.apache.flink.kubernetes.operator.api.bluegreen.BlueGreenDeploymentType;
import org.apache.flink.kubernetes.operator.api.bluegreen.BlueGreenDiffType;
import org.apache.flink.kubernetes.operator.api.spec.FlinkBlueGreenDeploymentSpec;
import org.apache.flink.kubernetes.operator.api.spec.KubernetesDeploymentMode;
import org.apache.flink.kubernetes.operator.api.spec.UpgradeMode;
import org.apache.flink.kubernetes.operator.api.status.FlinkBlueGreenDeploymentStatus;
import org.apache.flink.kubernetes.operator.api.status.Savepoint;
import org.apache.flink.kubernetes.operator.api.utils.SpecUtils;
import org.apache.flink.kubernetes.operator.config.KubernetesOperatorConfigOptions;
import org.apache.flink.kubernetes.operator.controller.FlinkResourceContext;
import org.apache.flink.kubernetes.operator.controller.bluegreen.BlueGreenContext;
import org.apache.flink.kubernetes.operator.observer.SavepointFetchResult;
import org.apache.flink.kubernetes.operator.reconciler.diff.FlinkBlueGreenDeploymentSpecDiff;
import org.apache.flink.util.Preconditions;

import io.fabric8.kubernetes.api.model.ObjectMeta;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.Map;
import java.util.Optional;

import static org.apache.flink.kubernetes.operator.api.spec.FlinkBlueGreenDeploymentConfigOptions.ABORT_GRACE_PERIOD;
import static org.apache.flink.kubernetes.operator.api.spec.FlinkBlueGreenDeploymentConfigOptions.DEPLOYMENT_DELETION_DELAY;
import static org.apache.flink.kubernetes.operator.api.spec.FlinkBlueGreenDeploymentConfigOptions.RECONCILIATION_RESCHEDULING_INTERVAL;
import static org.apache.flink.kubernetes.operator.controller.bluegreen.BlueGreenKubernetesService.getDependentObjectMeta;
import static org.apache.flink.kubernetes.operator.controller.bluegreen.BlueGreenKubernetesService.replaceFlinkBlueGreenDeployment;

/** Consolidated utility methods for Blue/Green deployment operations. */
public class BlueGreenUtils {

    private static final Logger LOG = LoggerFactory.getLogger(BlueGreenUtils.class);

    // ==================== Spec Operations ====================

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
        BlueGreenDiffType diffType = getSpecDiff(context);
        return diffType != BlueGreenDiffType.IGNORE;
    }

    public static BlueGreenDiffType getSpecDiff(BlueGreenContext context) {
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

        return diff.compare();
    }

    public static void setLastReconciledSpec(BlueGreenContext context) {
        FlinkBlueGreenDeploymentStatus deploymentStatus = context.getDeploymentStatus();
        deploymentStatus.setLastReconciledSpec(
                SpecUtils.writeSpecAsJSON(context.getBgDeployment().getSpec(), "spec"));
        deploymentStatus.setLastReconciledTimestamp(Instant.now().toString());
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

    /**
     * Extracts a configuration option value from the Blue/Green deployment spec.
     *
     * @param bgDeployment the Blue/Green deployment
     * @param option the configuration option to extract
     * @return the configuration value or default if not found
     */
    public static <T> T getConfigOption(
            FlinkBlueGreenDeployment bgDeployment, ConfigOption<T> option) {
        Map<String, String> configuration = bgDeployment.getSpec().getConfiguration();

        if (configuration == null) {
            return option.defaultValue();
        }

        return Configuration.fromMap(configuration).get(option);
    }

    // ==================== Time Utilities ====================

    /**
     * Converts milliseconds to ISO instant string.
     *
     * @param millis the milliseconds since epoch
     * @return ISO instant string representation
     */
    public static String millisToInstantStr(long millis) {
        return Instant.ofEpochMilli(millis).toString();
    }

    /**
     * Converts ISO instant string to milliseconds.
     *
     * @param instant the ISO instant string
     * @return milliseconds since epoch, or 0 if instant is null
     */
    public static long instantStrToMillis(String instant) {
        if (instant == null) {
            return 0;
        }
        return Instant.parse(instant).toEpochMilli();
    }

    /**
     * Gets the reconciliation rescheduling interval for the Blue/Green deployment.
     *
     * @param context the Blue/Green transition context
     * @return reconciliation interval in milliseconds
     */
    public static long getReconciliationReschedInterval(BlueGreenContext context) {
        return Math.max(
                getConfigOption(context.getBgDeployment(), RECONCILIATION_RESCHEDULING_INTERVAL)
                        .toMillis(),
                0);
    }

    /**
     * Gets the deployment deletion delay for the Blue/Green deployment.
     *
     * @param context the Blue/Green transition context
     * @return deletion delay in milliseconds
     */
    public static long getDeploymentDeletionDelay(BlueGreenContext context) {
        return Math.max(
                getConfigOption(context.getBgDeployment(), DEPLOYMENT_DELETION_DELAY).toMillis(),
                0);
    }

    /**
     * Gets the abort grace period for the Blue/Green deployment.
     *
     * @param context the Blue/Green transition context
     * @return abort grace period in milliseconds
     */
    public static long getAbortGracePeriod(BlueGreenContext context) {
        long abortGracePeriod =
                getConfigOption(context.getBgDeployment(), ABORT_GRACE_PERIOD).toMillis();
        return abortGracePeriod;
    }

    /**
     * Sets the abort timestamp in the deployment status based on current time and grace period.
     *
     * @param context the Blue/Green transition context
     */
    public static void setAbortTimestamp(BlueGreenContext context) {
        context.getDeploymentStatus()
                .setAbortTimestamp(
                        millisToInstantStr(
                                System.currentTimeMillis() + getAbortGracePeriod(context)));
    }

    // ==================== Savepoint/Checkpoint Operations ====================

    /**
     * Determines if a savepoint is required based on the deployment's upgrade mode. Currently all
     * upgrade modes except STATELESS require savepoints.
     *
     * @param context the Blue/Green transition context
     * @return true if savepoint is required, false otherwise
     */
    public static boolean isSavepointRequired(BlueGreenContext context) {
        UpgradeMode upgradeMode =
                context.getBgDeployment()
                        .getSpec()
                        .getTemplate()
                        .getSpec()
                        .getJob()
                        .getUpgradeMode();
        //        return UpgradeMode.SAVEPOINT == upgradeMode;
        // Currently taking savepoints for all modes except STATELESS
        // (previously only SAVEPOINT mode required savepoints)
        return UpgradeMode.STATELESS != upgradeMode;
    }

    public static boolean lookForCheckpoint(BlueGreenContext context) {
        FlinkBlueGreenDeploymentStatus deploymentStatus = context.getDeploymentStatus();
        String lastReconciledSpec = deploymentStatus.getLastReconciledSpec();
        FlinkBlueGreenDeploymentSpec lastSpec =
                SpecUtils.readSpecFromJSON(
                        lastReconciledSpec, "spec", FlinkBlueGreenDeploymentSpec.class);

        var previousUpgradeMode = lastSpec.getTemplate().getSpec().getJob().getUpgradeMode();
        var nextUpgradeMode =
                context.getBgDeployment()
                        .getSpec()
                        .getTemplate()
                        .getSpec()
                        .getJob()
                        .getUpgradeMode();

        return previousUpgradeMode == nextUpgradeMode && nextUpgradeMode == UpgradeMode.LAST_STATE;
    }

    public static String triggerSavepoint(FlinkResourceContext<FlinkDeployment> ctx)
            throws Exception {
        var jobId = ctx.getResource().getStatus().getJobStatus().getJobId();
        var conf = ctx.getObserveConfig();
        var savepointFormatType =
                conf.get(KubernetesOperatorConfigOptions.OPERATOR_SAVEPOINT_FORMAT_TYPE);
        var savepointDirectory =
                Preconditions.checkNotNull(conf.get(CheckpointingOptions.SAVEPOINT_DIRECTORY));

        LOG.info("About to trigger savepoint info for jobId: {}", jobId);

        String triggerId =
                ctx.getFlinkService()
                        .triggerSavepoint(jobId, savepointFormatType, savepointDirectory, conf);

        LOG.info(
                "Triggered savepoint for jobId: {}, triggerId: {}, savepoint dir: {}",
                jobId,
                triggerId,
                savepointDirectory);

        return triggerId;
    }

    public static SavepointFetchResult fetchSavepointInfo(
            FlinkResourceContext<FlinkDeployment> ctx, String triggerId) {
        String jobId = ctx.getResource().getStatus().getJobStatus().getJobId();
        LOG.info("About to fetch savepoint info for jobId: {}, triggerId: {}", jobId, triggerId);

        var savepointFetchResult =
                ctx.getFlinkService().fetchSavepointInfo(triggerId, jobId, ctx.getObserveConfig());

        LOG.info("Fetched savepoint info for jobId: {}, triggerId: {}", jobId, triggerId);
        return savepointFetchResult;
    }

    public static Savepoint getLastCheckpoint(
            FlinkResourceContext<FlinkDeployment> resourceContext) {

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

        // Alternative fallback if no checkpoint is available could be implemented here
        if (lastCheckpoint.isEmpty()) {
            throw new IllegalStateException(
                    "Last Checkpoint for Job "
                            + resourceContext.getResource().getMetadata().getName()
                            + " not found!");
        }

        return lastCheckpoint.get();
    }

    // ==================== Deployment Preparation Utilities ====================

    /**
     * Creates a new FlinkDeployment resource for a Blue/Green deployment transition. This method
     * prepares the deployment with proper metadata, specs, and savepoint configuration.
     *
     * @param context the Blue/Green transition context
     * @param blueGreenDeploymentType the type of deployment (BLUE or GREEN)
     * @param lastCheckpoint the savepoint/checkpoint to restore from (can be null)
     * @param isFirstDeployment whether this is the initial deployment
     * @param bgMeta the metadata of the parent Blue/Green deployment
     * @return configured FlinkDeployment ready for deployment
     */
    public static FlinkDeployment prepareFlinkDeployment(
            BlueGreenContext context,
            BlueGreenDeploymentType blueGreenDeploymentType,
            Savepoint lastCheckpoint,
            boolean isFirstDeployment,
            ObjectMeta bgMeta) {
        // Deployment
        FlinkDeployment flinkDeployment = new FlinkDeployment();
        FlinkBlueGreenDeploymentSpec spec = context.getBgDeployment().getSpec();

        String childDeploymentName =
                bgMeta.getName() + "-" + blueGreenDeploymentType.toString().toLowerCase();

        FlinkBlueGreenDeploymentSpec adjustedSpec =
                adjustNameReferences(
                        spec,
                        bgMeta.getName(),
                        childDeploymentName,
                        "spec",
                        FlinkBlueGreenDeploymentSpec.class);

        // The Blue/Green initialSavepointPath is only used for first-time deployments
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
                LOG.info("Clean startup with no checkpoint/savepoint restoration");
            }
        } else if (lastCheckpoint != null) {
            String location = lastCheckpoint.getLocation().replace("file:", "");
            LOG.info("Using Blue/Green savepoint/checkpoint: " + location);
            adjustedSpec.getTemplate().getSpec().getJob().setInitialSavepointPath(location);
        }

        flinkDeployment.setSpec(adjustedSpec.getTemplate().getSpec());

        // Deployment metadata
        ObjectMeta flinkDeploymentMeta = getDependentObjectMeta(context.getBgDeployment());
        flinkDeploymentMeta.setName(childDeploymentName);
        flinkDeploymentMeta.setLabels(
                Map.of(BlueGreenDeploymentType.LABEL_KEY, blueGreenDeploymentType.toString()));
        flinkDeployment.setMetadata(flinkDeploymentMeta);
        return flinkDeployment;
    }
}
