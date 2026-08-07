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

package org.apache.flink.autoscaler;

import org.apache.flink.annotation.Experimental;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.autoscaler.config.AutoScalerOptions;
import org.apache.flink.autoscaler.metrics.AutoscalerFlinkMetrics;
import org.apache.flink.autoscaler.metrics.CollectedMetricHistory;
import org.apache.flink.autoscaler.metrics.CollectedMetrics;
import org.apache.flink.autoscaler.metrics.EvaluatedMetrics;
import org.apache.flink.autoscaler.topology.JobTopology;
import org.apache.flink.client.program.rest.RestClusterClient;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.util.function.SupplierWithException;

import lombok.AccessLevel;
import lombok.Builder;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

import javax.annotation.Nullable;

import java.time.Duration;
import java.time.Instant;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.SortedMap;

/**
 * The job autoscaler context, it includes all details related to the current job.
 *
 * @param <KEY> The job key.
 */
@Experimental
@ToString
public class JobAutoScalerContext<KEY> {

    /** The identifier of each flink job. */
    @Getter private final KEY jobKey;

    /** The jobId and jobStatus can be null when the job isn't started. */
    @Nullable @Getter private final JobID jobID;

    @Nullable @Getter private final JobStatus jobStatus;

    /**
     * The configuration based on the latest user-provided spec. This is not the already deployed /
     * observed configuration.
     *
     * <p>For a plugin context this is the effective configuration: the job configuration with that
     * plugin's prefix-stripped per-plugin overrides merged on top.
     */
    @Getter private final Configuration configuration;

    /** The metric group for the current job, used to register autoscaler and plugin metrics. */
    @Getter private final MetricGroup metricGroup;

    /** Supplier of a Flink REST client for the current job, invoked lazily on first use. */
    @ToString.Exclude
    private final SupplierWithException<RestClusterClient<String>, Exception> restClientSupplier;

    /**
     * Internal scratch space populated by the autoscaler pipeline within a single scaling cycle. It
     * is lazily initialized on first access and excluded from {@code toString}, and is not part of
     * the externally-provided context (callers should not set it).
     */
    @ToString.Exclude private ScalingCycleState scalingCycleState;

    @Builder(toBuilder = true)
    public JobAutoScalerContext(
            KEY jobKey,
            @Nullable JobID jobID,
            @Nullable JobStatus jobStatus,
            Configuration configuration,
            MetricGroup metricGroup,
            SupplierWithException<RestClusterClient<String>, Exception> restClientSupplier) {
        this.jobKey = jobKey;
        this.jobID = jobID;
        this.jobStatus = jobStatus;
        this.configuration = configuration;
        this.metricGroup = metricGroup;
        this.restClientSupplier = restClientSupplier;
    }

    /**
     * Copy constructor used by plugin context subclasses. It copies all fields from the given
     * context, overlaying the plugin's prefix-stripped {@code overrides} on top of that context's
     * configuration (via {@link AutoScalerOptions#overlayConfiguration}), so a plugin context
     * exposes its effective per-plugin configuration through {@code getConfiguration()} without
     * each caller having to merge it. It also shares the {@link ScalingCycleState}, so the derived
     * context exposes the same per-cycle data (collected / evaluated metrics, topology, restart
     * time) through the inherited accessors rather than duplicating it.
     */
    protected JobAutoScalerContext(
            JobAutoScalerContext<KEY> autoScalerContext, Configuration overrides) {
        this.jobKey = autoScalerContext.jobKey;
        this.jobID = autoScalerContext.jobID;
        this.jobStatus = autoScalerContext.jobStatus;
        this.configuration =
                AutoScalerOptions.overlayConfiguration(autoScalerContext.configuration, overrides);
        this.metricGroup = autoScalerContext.metricGroup;
        this.restClientSupplier = autoScalerContext.restClientSupplier;
        this.scalingCycleState = autoScalerContext.getScalingCycleState();
    }

    /** Retrieve the currently configured TaskManager CPU. */
    public Optional<Double> getTaskManagerCpu() {
        // Not supported by default
        return Optional.empty();
    }

    /** Retrieve the currently configured TaskManager memory. */
    public Optional<MemorySize> getTaskManagerMemory() {
        return Optional.ofNullable(getConfiguration().get(TaskManagerOptions.TOTAL_PROCESS_MEMORY));
    }

    public RestClusterClient<String> getRestClusterClient() throws Exception {
        return restClientSupplier.get();
    }

    /**
     * Returns the mutable per-cycle {@link ScalingCycleState} for this context, lazily creating it
     * on first access. The metric collector, evaluator and scaling executor read from and write to
     * it so they can rely solely on the context threaded through them.
     */
    public ScalingCycleState getScalingCycleState() {
        if (scalingCycleState == null) {
            scalingCycleState = new ScalingCycleState();
        }
        return scalingCycleState;
    }

    /**
     * The job topology collected for the current cycle, derived from the {@link ScalingCycleState}.
     * Only meaningful once metrics have been collected this cycle. Intended for use by plugin
     * implementations.
     */
    public JobTopology getJobTopology() {
        return getScalingCycleState().getCollectedMetrics().getJobTopology();
    }

    /**
     * The metrics evaluated for the current cycle, derived from the {@link ScalingCycleState}. Only
     * meaningful once evaluation has run this cycle. Intended for use by plugin implementations.
     */
    public EvaluatedMetrics getEvaluatedMetrics() {
        return getScalingCycleState().getEvaluatedMetrics();
    }

    /**
     * An unmodifiable view of the collected metric history for the current cycle, derived from the
     * {@link ScalingCycleState}. Intended for use by plugin implementations.
     */
    public SortedMap<Instant, CollectedMetrics> getMetricsHistory() {
        return Collections.unmodifiableSortedMap(
                getScalingCycleState().getCollectedMetrics().getMetricHistory());
    }

    /**
     * The restart time for the current cycle, derived from the {@link ScalingCycleState}. Intended
     * for use by plugin implementations.
     */
    public Duration getRestartTime() {
        return getScalingCycleState().getRestartTime();
    }

    /**
     * Mutable, per-cycle working state of the autoscaler pipeline. A fresh instance is lazily
     * created for every {@link JobAutoScalerContext} and progressively populated as a scaling cycle
     * advances through metric collection, evaluation and scaling execution.
     *
     * <p>Threading this state through the context is what lets {@link ScalingMetricCollector},
     * {@link ScalingMetricEvaluator} and {@link ScalingExecutor} rely solely on the context passed
     * to them rather than on a long parameter list.
     */
    @Getter
    @Setter(AccessLevel.PACKAGE)
    public static class ScalingCycleState {

        /** The instant at which the current scaling cycle started, shared by all stages. */
        @Nullable private Instant now;

        /** The Flink metrics sink for the current job, used to report scaling metrics. */
        @Nullable private AutoscalerFlinkMetrics autoscalerMetrics;

        /** The metrics collected from the Flink cluster during the current cycle. */
        @Nullable private CollectedMetricHistory collectedMetrics;

        /** The scaling tracking loaded (and possibly updated) during the current cycle. */
        @Nullable private ScalingTracking scalingTracking;

        /** The trimmed scaling history loaded during the current cycle. */
        @Nullable private Map<JobVertexID, SortedMap<Instant, ScalingSummary>> scalingHistory;

        /** The restart time used by both metric evaluation and scaling execution. */
        @Nullable private Duration restartTime;

        /** The metrics evaluated from {@link #collectedMetrics}. */
        @Nullable private EvaluatedMetrics evaluatedMetrics;
    }
}
