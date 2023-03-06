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

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.client.program.rest.RestClusterClient;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.kubernetes.operator.api.AbstractFlinkResource;
import org.apache.flink.kubernetes.operator.autoscaler.config.AutoScalerOptions;
import org.apache.flink.kubernetes.operator.autoscaler.metrics.CollectedMetrics;
import org.apache.flink.kubernetes.operator.autoscaler.metrics.FlinkMetric;
import org.apache.flink.kubernetes.operator.autoscaler.metrics.ScalingMetric;
import org.apache.flink.kubernetes.operator.autoscaler.metrics.ScalingMetrics;
import org.apache.flink.kubernetes.operator.autoscaler.topology.JobTopology;
import org.apache.flink.kubernetes.operator.service.FlinkService;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.rest.messages.EmptyRequestBody;
import org.apache.flink.runtime.rest.messages.JobIDPathParameter;
import org.apache.flink.runtime.rest.messages.JobVertexIdPathParameter;
import org.apache.flink.runtime.rest.messages.job.JobDetailsInfo;
import org.apache.flink.runtime.rest.messages.job.metrics.AggregatedMetric;
import org.apache.flink.runtime.rest.messages.job.metrics.AggregatedSubtaskMetricsHeaders;
import org.apache.flink.runtime.rest.messages.job.metrics.AggregatedSubtaskMetricsParameters;
import org.apache.flink.util.Preconditions;

import io.javaoperatorsdk.operator.processing.event.ResourceID;
import lombok.SneakyThrows;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/** Metric collector using flink rest api. */
public abstract class ScalingMetricCollector {
    private static final Logger LOG = LoggerFactory.getLogger(ScalingMetricCollector.class);

    private final Map<ResourceID, Tuple2<Long, Map<JobVertexID, Map<String, FlinkMetric>>>>
            availableVertexMetricNames = new ConcurrentHashMap<>();

    private final Map<ResourceID, SortedMap<Instant, Map<JobVertexID, Map<ScalingMetric, Double>>>>
            histories = new ConcurrentHashMap<>();

    private final Map<ResourceID, JobTopology> topologies = new ConcurrentHashMap<>();

    private Clock clock = Clock.systemDefaultZone();

    public CollectedMetrics updateMetrics(
            AbstractFlinkResource<?, ?> cr,
            AutoScalerInfo scalingInformation,
            FlinkService flinkService,
            Configuration conf)
            throws Exception {

        var resourceID = ResourceID.fromResource(cr);
        var jobStatus = cr.getStatus().getJobStatus();
        var currentJobUpdateTs = Instant.ofEpochMilli(Long.parseLong(jobStatus.getUpdateTime()));
        var now = clock.instant();

        if (!currentJobUpdateTs.equals(
                scalingInformation.getJobUpdateTs().orElse(currentJobUpdateTs))) {
            scalingInformation.clearMetricHistory();
            cleanup(cr);
        }

        var topology = getJobTopology(flinkService, cr, conf, scalingInformation);

        var stabilizationDuration = conf.get(AutoScalerOptions.STABILIZATION_INTERVAL);
        var stableTime = currentJobUpdateTs.plus(stabilizationDuration);
        if (now.isBefore(stableTime)) {
            // As long as we are stabilizing, collect no metrics at all
            LOG.info("Skipping metric collection during stabilization period until {}", stableTime);
            return new CollectedMetrics(topology, Collections.emptySortedMap());
        }

        // Adjust the window size until it reaches the max size
        var metricsWindowSize =
                Duration.ofMillis(
                        Math.min(
                                now.toEpochMilli() - stableTime.toEpochMilli(),
                                conf.get(AutoScalerOptions.METRICS_WINDOW).toMillis()));

        // Extract metrics history for metric window size
        var scalingMetricHistory =
                histories.compute(
                        resourceID,
                        (k, h) -> {
                            if (h == null) {
                                h = scalingInformation.getMetricHistory();
                            }
                            return new TreeMap<>(h.tailMap(now.minus(metricsWindowSize)));
                        });

        // The filtered list of metrics we want to query for each vertex
        var filteredVertexMetricNames = queryFilteredMetricNames(flinkService, cr, conf, topology);

        // Aggregated job vertex metrics collected from Flink based on the filtered metric names
        var collectedVertexMetrics =
                queryAllAggregatedMetrics(cr, flinkService, conf, filteredVertexMetricNames);

        // The computed scaling metrics based on the collected aggregated vertex metrics
        var scalingMetrics =
                convertToScalingMetrics(resourceID, collectedVertexMetrics, topology, conf);

        // Add scaling metrics to history if they were computed successfully
        scalingMetricHistory.put(clock.instant(), scalingMetrics);
        scalingInformation.updateMetricHistory(currentJobUpdateTs, scalingMetricHistory);

        var windowFullTime = stableTime.plus(conf.get(AutoScalerOptions.METRICS_WINDOW));
        if (now.isBefore(windowFullTime)) {
            // As long as we haven't had time to collect a full window,
            // collect metrics but do not return any metrics
            LOG.info(
                    "Waiting until {} so the initial metric window is full before starting scaling",
                    windowFullTime);
            return new CollectedMetrics(topology, Collections.emptySortedMap());
        }

        return new CollectedMetrics(topology, scalingMetricHistory);
    }

    protected JobTopology getJobTopology(
            FlinkService flinkService,
            AbstractFlinkResource<?, ?> cr,
            Configuration conf,
            AutoScalerInfo scalerInfo)
            throws Exception {

        try (var restClient = (RestClusterClient<String>) flinkService.getClusterClient(conf)) {
            var jobId = JobID.fromHexString(cr.getStatus().getJobStatus().getJobId());
            var topology =
                    topologies.computeIfAbsent(
                            ResourceID.fromResource(cr),
                            r -> {
                                var t = queryJobTopology(restClient, jobId);
                                scalerInfo.updateVertexList(t.getVerticesInTopologicalOrder());
                                return t;
                            });
            updateKafkaSourceMaxParallelisms(restClient, jobId, topology);
            return topology;
        }
    }

    @VisibleForTesting
    protected JobTopology queryJobTopology(RestClusterClient<String> restClient, JobID jobId) {
        try {
            var jobDetailsInfo = restClient.getJobDetails(jobId).get();

            Map<JobVertexID, Integer> maxParallelismMap =
                    jobDetailsInfo.getJobVertexInfos().stream()
                            .collect(
                                    Collectors.toMap(
                                            JobDetailsInfo.JobVertexDetailsInfo::getJobVertexID,
                                            JobDetailsInfo.JobVertexDetailsInfo
                                                    ::getMaxParallelism));

            return JobTopology.fromJsonPlan(jobDetailsInfo.getJsonPlan(), maxParallelismMap);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private void updateKafkaSourceMaxParallelisms(
            RestClusterClient<String> restClient, JobID jobId, JobTopology topology)
            throws Exception {
        for (Map.Entry<JobVertexID, Set<JobVertexID>> entry : topology.getInputs().entrySet()) {
            if (entry.getValue().isEmpty()) {
                var sourceVertex = entry.getKey();
                queryAggregatedMetricNames(restClient, jobId, sourceVertex).stream()
                        .map(AggregatedMetric::getId)
                        .filter(s -> s.endsWith(".currentOffset"))
                        .mapToInt(
                                s -> {
                                    // We extract the partition from the pattern:
                                    // ...topic.[topic].partition.3.currentOffset
                                    var split = s.split("\\.");
                                    return Integer.parseInt(split[split.length - 2]);
                                })
                        .max()
                        .ifPresent(
                                p -> {
                                    LOG.debug(
                                            "Updating source {} max parallelism based on available partitions to {}",
                                            sourceVertex,
                                            p + 1);
                                    topology.updateMaxParallelism(sourceVertex, p + 1);
                                });
            }
        }
    }

    /**
     * Given a map of collected Flink vertex metrics we compute the scaling metrics for each job
     * vertex.
     *
     * @param collectedMetrics Collected metrics for all job vertices.
     * @return Computed scaling metrics for all job vertices.
     */
    private Map<JobVertexID, Map<ScalingMetric, Double>> convertToScalingMetrics(
            ResourceID resourceID,
            Map<JobVertexID, Map<FlinkMetric, AggregatedMetric>> collectedMetrics,
            JobTopology jobTopology,
            Configuration conf) {

        var out = new HashMap<JobVertexID, Map<ScalingMetric, Double>>();
        collectedMetrics.forEach(
                (jobVertexID, vertexFlinkMetrics) -> {
                    LOG.debug(
                            "Calculating vertex scaling metrics for {} from {}",
                            jobVertexID,
                            vertexFlinkMetrics);
                    var vertexScalingMetrics = new HashMap<ScalingMetric, Double>();
                    out.put(jobVertexID, vertexScalingMetrics);

                    if (jobTopology.isSource(jobVertexID)) {
                        ScalingMetrics.computeLagMetrics(vertexFlinkMetrics, vertexScalingMetrics);
                    }
                    ScalingMetrics.computeLoadMetrics(vertexFlinkMetrics, vertexScalingMetrics);

                    double lagGrowthRate =
                            computeLagGrowthRate(
                                    resourceID,
                                    jobVertexID,
                                    vertexScalingMetrics.get(ScalingMetric.LAG));

                    ScalingMetrics.computeDataRateMetrics(
                            jobVertexID,
                            vertexFlinkMetrics,
                            vertexScalingMetrics,
                            jobTopology,
                            lagGrowthRate,
                            conf);

                    LOG.debug(
                            "Vertex scaling metrics for {}: {}", jobVertexID, vertexScalingMetrics);
                });

        return out;
    }

    private double computeLagGrowthRate(
            ResourceID resourceID, JobVertexID jobVertexID, Double currentLag) {
        var metricHistory = histories.get(resourceID);

        if (metricHistory == null || metricHistory.isEmpty()) {
            return Double.NaN;
        }

        var lastCollectionTime = metricHistory.lastKey();
        var lastCollectedMetrics = metricHistory.get(lastCollectionTime).get(jobVertexID);

        if (lastCollectedMetrics == null) {
            return Double.NaN;
        }

        var lastLag = lastCollectedMetrics.get(ScalingMetric.LAG);

        if (lastLag == null || currentLag == null) {
            return Double.NaN;
        }

        var timeDiff = Duration.between(lastCollectionTime, clock.instant()).toSeconds();
        return (currentLag - lastLag) / timeDiff;
    }

    /** Query the available metric names for each job vertex for the current spec generation. */
    @SneakyThrows
    protected Map<JobVertexID, Map<String, FlinkMetric>> queryFilteredMetricNames(
            FlinkService flinkService,
            AbstractFlinkResource<?, ?> cr,
            Configuration conf,
            JobTopology topology) {

        var jobId = JobID.fromHexString(cr.getStatus().getJobStatus().getJobId());
        var vertices = topology.getVerticesInTopologicalOrder();

        long deployedGeneration = getDeployedGeneration(cr);

        var previousMetricNames = availableVertexMetricNames.get(ResourceID.fromResource(cr));

        if (previousMetricNames != null) {
            if (deployedGeneration == previousMetricNames.f0) {
                // We have already gathered the metric names for this spec, no need to query again
                return previousMetricNames.f1;
            } else {
                availableVertexMetricNames.remove(ResourceID.fromResource(cr));
            }
        }

        try (var restClient = (RestClusterClient<String>) flinkService.getClusterClient(conf)) {
            var names =
                    vertices.stream()
                            .collect(
                                    Collectors.toMap(
                                            v -> v,
                                            v ->
                                                    getFilteredVertexMetricNames(
                                                            restClient, jobId, v, topology, conf)));
            availableVertexMetricNames.put(
                    ResourceID.fromResource(cr), Tuple2.of(deployedGeneration, names));
            return names;
        }
    }

    public static long getDeployedGeneration(AbstractFlinkResource<?, ?> cr) {
        return cr.getStatus()
                .getReconciliationStatus()
                .deserializeLastReconciledSpecWithMeta()
                .getMeta()
                .getMetadata()
                .getGeneration();
    }

    /**
     * Query and filter metric names for a given job vertex.
     *
     * @param restClient Flink rest client.
     * @param jobID Job Id.
     * @param jobVertexID Job Vertex Id.
     * @return Map of filtered metric names.
     */
    @SneakyThrows
    protected Map<String, FlinkMetric> getFilteredVertexMetricNames(
            RestClusterClient<?> restClient,
            JobID jobID,
            JobVertexID jobVertexID,
            JobTopology topology,
            Configuration conf) {

        var allMetricNames = queryAggregatedMetricNames(restClient, jobID, jobVertexID);

        var filteredMetrics = new HashMap<String, FlinkMetric>();
        var requiredMetrics = new HashSet<FlinkMetric>();

        requiredMetrics.add(FlinkMetric.BUSY_TIME_PER_SEC);

        if (topology.isSource(jobVertexID)) {
            requiredMetrics.add(FlinkMetric.SOURCE_TASK_NUM_RECORDS_IN_PER_SEC);
            // Pending records metric won't be available for some sources.
            // The Kafka source, for instance, lazily initializes this metric on receiving
            // the first record. If this is a fresh topic or no new data has been read since
            // the last checkpoint, the pendingRecords metrics won't be available. Also, legacy
            // sources do not have this metric.
            Optional<String> pendingRecordsMetric =
                    FlinkMetric.PENDING_RECORDS.findAny(allMetricNames);
            pendingRecordsMetric.ifPresentOrElse(
                    m -> filteredMetrics.put(m, FlinkMetric.PENDING_RECORDS),
                    () ->
                            LOG.warn(
                                    "pendingRecords metric for {} could not be found. Either a legacy source or an idle source. Assuming no pending records.",
                                    jobVertexID));
            FlinkMetric.SOURCE_TASK_NUM_RECORDS_OUT_PER_SEC
                    .findAny(allMetricNames)
                    .ifPresent(
                            m ->
                                    filteredMetrics.put(
                                            m, FlinkMetric.SOURCE_TASK_NUM_RECORDS_OUT_PER_SEC));
        } else {
            // Not a source so we must have numRecordsInPerSecond
            requiredMetrics.add(FlinkMetric.NUM_RECORDS_IN_PER_SEC);
        }

        if (!topology.getOutputs().get(jobVertexID).isEmpty()) {
            // Not a sink so we must have numRecordsOutPerSecond
            requiredMetrics.add(FlinkMetric.NUM_RECORDS_OUT_PER_SEC);
        }

        for (FlinkMetric flinkMetric : requiredMetrics) {
            Optional<String> flinkMetricName = flinkMetric.findAny(allMetricNames);
            if (flinkMetricName.isPresent()) {
                // Add actual Flink metric name to list
                filteredMetrics.put(flinkMetricName.get(), flinkMetric);
            } else {
                throw new RuntimeException(
                        "Could not find required metric "
                                + flinkMetric.name()
                                + " for "
                                + jobVertexID);
            }
        }

        return filteredMetrics;
    }

    @VisibleForTesting
    protected Collection<AggregatedMetric> queryAggregatedMetricNames(
            RestClusterClient<?> restClient, JobID jobID, JobVertexID jobVertexID)
            throws Exception {
        var parameters = new AggregatedSubtaskMetricsParameters();
        var pathIt = parameters.getPathParameters().iterator();

        ((JobIDPathParameter) pathIt.next()).resolve(jobID);
        ((JobVertexIdPathParameter) pathIt.next()).resolve(jobVertexID);

        return restClient
                .sendRequest(
                        AggregatedSubtaskMetricsHeaders.getInstance(),
                        parameters,
                        EmptyRequestBody.getInstance())
                .get()
                .getMetrics();
    }

    protected abstract Map<JobVertexID, Map<FlinkMetric, AggregatedMetric>>
            queryAllAggregatedMetrics(
                    AbstractFlinkResource<?, ?> cr,
                    FlinkService flinkService,
                    Configuration conf,
                    Map<JobVertexID, Map<String, FlinkMetric>> filteredVertexMetricNames);

    public void cleanup(AbstractFlinkResource<?, ?> cr) {
        var resourceId = ResourceID.fromResource(cr);
        histories.remove(resourceId);
        availableVertexMetricNames.remove(resourceId);
        topologies.remove(resourceId);
    }

    @VisibleForTesting
    protected void setClock(Clock clock) {
        this.clock = Preconditions.checkNotNull(clock);
    }

    @VisibleForTesting
    protected Map<ResourceID, Tuple2<Long, Map<JobVertexID, Map<String, FlinkMetric>>>>
            getAvailableVertexMetricNames() {
        return availableVertexMetricNames;
    }

    @VisibleForTesting
    protected Map<ResourceID, SortedMap<Instant, Map<JobVertexID, Map<ScalingMetric, Double>>>>
            getHistories() {
        return histories;
    }

    @VisibleForTesting
    protected Map<ResourceID, JobTopology> getTopologies() {
        return topologies;
    }
}
