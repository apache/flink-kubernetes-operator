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

package org.apache.flink.kubernetes.operator.autoscaler;

import org.apache.flink.autoscaler.realizer.ScalingRealizer;
import org.apache.flink.autoscaler.tuning.ConfigChanges;
import org.apache.flink.autoscaler.tuning.MemoryTuning;
import org.apache.flink.configuration.ConfigurationUtils;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.configuration.PipelineOptions;
import org.apache.flink.kubernetes.operator.api.CrdConstants;
import org.apache.flink.kubernetes.operator.api.FlinkDeployment;
import org.apache.flink.kubernetes.operator.api.spec.Resource;
import org.apache.flink.kubernetes.operator.api.spec.TaskManagerSpec;
import org.apache.flink.kubernetes.operator.api.spec.UpgradeMode;
import org.apache.flink.kubernetes.operator.config.FlinkConfigBuilder;
import org.apache.flink.kubernetes.operator.utils.ResourceConfigUtils;

import io.fabric8.kubernetes.api.model.Quantity;
import io.fabric8.kubernetes.api.model.ResourceRequirements;
import io.javaoperatorsdk.operator.processing.event.ResourceID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.Map;
import java.util.Objects;
import java.util.OptionalLong;

/** The Kubernetes implementation for applying parallelism overrides. */
public class KubernetesScalingRealizer
        implements ScalingRealizer<ResourceID, KubernetesJobAutoScalerContext> {

    private static final Logger LOG = LoggerFactory.getLogger(KubernetesScalingRealizer.class);

    @Override
    public void realizeParallelismOverrides(
            KubernetesJobAutoScalerContext context, Map<String, String> parallelismOverrides) {
        context.getResource()
                .getSpec()
                .getFlinkConfiguration()
                .put(
                        PipelineOptions.PARALLELISM_OVERRIDES.key(),
                        getOverrideString(context, parallelismOverrides));
    }

    @Override
    public void realizeConfigOverrides(
            KubernetesJobAutoScalerContext context, ConfigChanges configChanges) {
        if (!(context.getResource() instanceof FlinkDeployment)) {
            // We can't adjust the configuration of non-job deployments.
            return;
        }
        FlinkDeployment flinkDeployment = ((FlinkDeployment) context.getResource());
        // Apply config overrides

        flinkDeployment.getSpec().getFlinkConfiguration().remove(configChanges.getRemovals());
        flinkDeployment.getSpec().getFlinkConfiguration().putAllFrom(configChanges.getOverrides());

        // Update total memory in spec
        var totalMemoryOverride =
                MemoryTuning.getTotalMemory(
                        flinkDeployment.getSpec().getFlinkConfiguration().asConfiguration(),
                        context);
        if (totalMemoryOverride.compareTo(MemorySize.ZERO) <= 0) {
            LOG.warn("Total memory override {} is not valid", totalMemoryOverride);
            return;
        }
        TaskManagerSpec taskManager = flinkDeployment.getSpec().getTaskManager();
        String currentMemoryString = getTaskManagerMemory(taskManager);
        if (currentMemoryString == null) {
            // Memory is configured via neither the deprecated resource field nor resources, so
            // there is nothing to adjust (the operator cannot tune what was never set here).
            LOG.warn(
                    "Skipping memory tuning: TaskManager memory is not configured via "
                            + "spec.taskManager.resource or spec.taskManager.resources.");
            return;
        }
        // Make sure to parse in the same way as the original deploy code path.
        var currentMemory =
                MemorySize.parse(
                        ResourceConfigUtils.parseResourceMemoryString(currentMemoryString));
        if (!totalMemoryOverride.equals(currentMemory)) {
            // Adjust the resource memory to change the total TM memory
            setTaskManagerMemory(taskManager, currentMemory, totalMemoryOverride);
        }
    }

    /**
     * Reads the TaskManager memory from whichever field is in effect. Mirrors {@link
     * FlinkConfigBuilder}, which prefers {@code resources} (requests, falling back to limits) over
     * the deprecated {@code resource} field.
     */
    @Nullable
    private static String getTaskManagerMemory(TaskManagerSpec taskManager) {
        if (ResourceConfigUtils.hasResourceRequirements(taskManager.getResources())) {
            Quantity memory =
                    ResourceConfigUtils.getRequestOrLimit(
                            taskManager.getResources(), CrdConstants.MEMORY);
            return memory != null ? memory.toString() : null;
        }
        Resource resource = taskManager.getResource();
        return resource != null ? resource.getMemory() : null;
    }

    /**
     * Writes the tuned TaskManager memory back to whichever field is in effect, so the next deploy
     * picks it up. For the {@code resources} field the tuned value is written to the memory entry
     * that drives the total process memory (requests, or limits when requests is absent). When both
     * requests and limits carry memory, the limit is scaled by the same ratio so the memory limit
     * factor ({@code limits / requests}) is preserved.
     */
    private static void setTaskManagerMemory(
            TaskManagerSpec taskManager, MemorySize currentMemory, MemorySize tunedMemory) {
        if (!ResourceConfigUtils.hasResourceRequirements(taskManager.getResources())) {
            taskManager.getResource().setMemory(String.valueOf(tunedMemory.getBytes()));
            return;
        }
        ResourceRequirements resources = taskManager.getResources();
        Map<String, Quantity> requests = resources.getRequests();
        Map<String, Quantity> limits = resources.getLimits();
        String tunedBytes = String.valueOf(tunedMemory.getBytes());

        if (requests != null && requests.get(CrdConstants.MEMORY) != null) {
            requests.put(CrdConstants.MEMORY, new Quantity(tunedBytes));
            // Preserve the memory limit factor by scaling the limit by the same ratio.
            if (limits != null && limits.get(CrdConstants.MEMORY) != null) {
                limits.put(
                        CrdConstants.MEMORY,
                        scaleMemory(limits.get(CrdConstants.MEMORY), currentMemory, tunedMemory));
            }
        } else if (limits != null && limits.get(CrdConstants.MEMORY) != null) {
            // Requests defaults to limits in Kubernetes when omitted, so tune the limit directly.
            limits.put(CrdConstants.MEMORY, new Quantity(tunedBytes));
        }
    }

    /**
     * Scales the given memory quantity by the ratio between the tuned and current memory, so the
     * limit tracks the request and the memory limit factor is preserved.
     */
    private static Quantity scaleMemory(
            Quantity limit, MemorySize currentMemory, MemorySize tunedMemory) {
        if (currentMemory.getBytes() <= 0) {
            // Cannot derive a ratio; fall back to the tuned value.
            return new Quantity(String.valueOf(tunedMemory.getBytes()));
        }
        long limitBytes =
                MemorySize.parse(ResourceConfigUtils.parseResourceMemoryString(limit.toString()))
                        .getBytes();
        double ratio = (double) tunedMemory.getBytes() / currentMemory.getBytes();
        return new Quantity(String.valueOf(Math.round(limitBytes * ratio)));
    }

    /**
     * Trigger a stateful same-parallelism restart. When the deployed source/runtime and configured
     * source mode support recovery-time active-split reassignment, DynamicKafkaSource recovery can
     * then rebuild a balanced active assignment without retaining removed splits as live work.
     * LAST_STATE alone does not establish that capability.
     */
    @Override
    public OptionalLong realizeSourceAssignmentRebalance(
            KubernetesJobAutoScalerContext context, long requestId) {
        var resource = context.getResource();
        var jobSpec = resource.getSpec().getJob();
        if (jobSpec == null || jobSpec.getUpgradeMode() != UpgradeMode.LAST_STATE) {
            LOG.warn(
                    "Skipping source assignment rebalance for {} because it requires a "
                            + "LAST_STATE application job.",
                    context.getJobKey());
            return OptionalLong.empty();
        }

        var currentRestartNonce = resource.getSpec().getRestartNonce();
        var lastReconciledSpec =
                resource.getStatus().getReconciliationStatus().deserializeLastReconciledSpec();
        var lastReconciledRestartNonce =
                lastReconciledSpec == null ? null : lastReconciledSpec.getRestartNonce();
        if (currentRestartNonce != null
                && !Objects.equals(currentRestartNonce, lastReconciledRestartNonce)
                && currentRestartNonce != requestId) {
            LOG.info(
                    "Using pending restart nonce {} to cover source assignment rebalance for {}.",
                    currentRestartNonce,
                    context.getJobKey());
            return OptionalLong.of(currentRestartNonce);
        }

        if (!Objects.equals(currentRestartNonce, requestId)) {
            resource.getSpec().setRestartNonce(requestId);
            LOG.info(
                    "Requesting controlled source assignment rebalance for {} with restart nonce {}.",
                    context.getJobKey(),
                    requestId);
        }
        return OptionalLong.of(requestId);
    }

    @Nullable
    private static String getOverrideString(
            KubernetesJobAutoScalerContext context, Map<String, String> newOverrides) {
        if (context.getResource().getStatus().getReconciliationStatus().isBeforeFirstDeployment()) {
            return ConfigurationUtils.convertValue(newOverrides, String.class);
        }

        var conf = context.getResourceContext().getObserveConfig();
        var currentOverrides =
                conf.getOptional(PipelineOptions.PARALLELISM_OVERRIDES).orElse(Map.of());

        // Check that the overrides actually changed and not just the String representation.
        // This way we prevent reconciling a NOOP config change which would unnecessarily redeploy
        // the pipeline.
        if (currentOverrides.equals(newOverrides)) {
            // If overrides are identical, use the previous string as-is.
            return conf.getValue(PipelineOptions.PARALLELISM_OVERRIDES);
        } else {
            return ConfigurationUtils.convertValue(newOverrides, String.class);
        }
    }
}
