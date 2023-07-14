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
import org.apache.flink.client.program.rest.RestClusterClient;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.kubernetes.operator.api.AbstractFlinkResource;
import org.apache.flink.kubernetes.operator.autoscaler.config.AutoScalerOptions;
import org.apache.flink.kubernetes.operator.autoscaler.metrics.CollectedMetricHistory;
import org.apache.flink.kubernetes.operator.autoscaler.metrics.CollectedMetrics;
import org.apache.flink.kubernetes.operator.autoscaler.metrics.FlinkMetric;
import org.apache.flink.kubernetes.operator.autoscaler.metrics.ScalingMetric;
import org.apache.flink.kubernetes.operator.autoscaler.metrics.ScalingMetrics;
import org.apache.flink.kubernetes.operator.autoscaler.topology.JobTopology;
import org.apache.flink.kubernetes.operator.autoscaler.utils.AutoScalerUtils;
import org.apache.flink.kubernetes.operator.service.FlinkService;
import org.apache.flink.runtime.execution.ExecutionState;
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
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.SortedMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/** Metric collector using flink rest api. */
public abstract class ScalingMetricCollector {
    private static final Logger LOG = LoggerFactory.getLogger(ScalingMetricCollector.class);

    private final Map<ResourceID, Map<JobVertexID, Map<String, FlinkMetric>>>
            availableVertexMetricNames = new ConcurrentHashMap<>();

    private final Map<ResourceID, SortedMap<Instant, CollectedMetrics>> histories =
            new ConcurrentHashMap<>();

    private Clock clock = Clock.systemDefaultZone();

    public CollectedMetricHistory updateMetrics(
            AbstractFlinkResource<?, ?> cr,
            AutoScalerInfo autoscalerInfo,
            FlinkService flinkService,
            Configuration conf)
            throws Exception {

        var resourceID = ResourceID.fromResource(cr);
        var jobId = JobID.fromHexString(cr.getStatus().getJobStatus().getJobId());

        var now = clock.instant();

        var metricHistory =
                histories.computeIfAbsent(resourceID, (k) -> autoscalerInfo.getMetricHistory());

        // The timestamp of the first metric observation marks the start
        // If we haven't collected any metrics, we are starting now
        var metricCollectionStartTs = metricHistory.isEmpty() ? now : metricHistory.firstKey();

        var jobDetailsInfo = flinkService.getJobDetailsInfo(jobId, conf);
        var jobUpdateTs = getJobUpdateTs(jobDetailsInfo);
        if (jobUpdateTs.isAfter(metricCollectionStartTs)) {
            LOG.info("Job updated at {}. Clearing metrics.", jobUpdateTs);
            autoscalerInfo.clearMetricHistory();
            cleanup(cr);
            metricHistory.clear();
            metricCollectionStartTs = now;
        }
        var topology = getJobTopology(flinkService, conf, autoscalerInfo, jobDetailsInfo);

        // Trim metrics outside the metric window from metrics history
        var metricWindowSize = getMetricWindowSize(conf);
        metricHistory.headMap(now.minus(metricWindowSize)).clear();

        var stableTime = jobUpdateTs.plus(conf.get(AutoScalerOptions.STABILIZATION_INTERVAL));
        if (now.isBefore(stableTime)) {
            // As long as we are stabilizing, collect no metrics at all
            LOG.info("Skipping metric collection during stabilization period until {}", stableTime);
            return new CollectedMetricHistory(topology, Collections.emptySortedMap());
        }

        // The filtered list of metrics we want to query for each vertex
        var filteredVertexMetricNames = queryFilteredMetricNames(flinkService, cr, conf, topology);

        // Aggregated job vertex metrics collected from Flink based on the filtered metric names
        var collectedVertexMetrics =
                queryAllAggregatedMetrics(cr, flinkService, conf, filteredVertexMetricNames);

        // The computed scaling metrics based on the collected aggregated vertex metrics
        var scalingMetrics =
                convertToScalingMetrics(resourceID, collectedVertexMetrics, topology, conf);

        // Add scaling metrics to history if they were computed successfully
        metricHistory.put(now, scalingMetrics);
        autoscalerInfo.updateMetricHistory(metricHistory);

        var collectedMetrics = new CollectedMetricHistory(topology, metricHistory);

        var windowFullTime = metricCollectionStartTs.plus(metricWindowSize);
        collectedMetrics.setFullyCollected(!now.isBefore(windowFullTime));

        if (!collectedMetrics.isFullyCollected()) {
            LOG.info("Metric window not full until {}", windowFullTime);
        }

        return collectedMetrics;
    }

    protected Duration getMetricWindowSize(Configuration conf) {
        return conf.get(AutoScalerOptions.METRICS_WINDOW);
    }

    @VisibleForTesting
    protected Instant getJobUpdateTs(JobDetailsInfo jobDetailsInfo) {
        return Instant.ofEpochMilli(
                jobDetailsInfo.getTimestamps().values().stream().max(Long::compare).get());
    }

    protected JobTopology getJobTopology(
            FlinkService flinkService,
            Configuration conf,
            AutoScalerInfo scalerInfo,
            JobDetailsInfo jobDetailsInfo)
            throws Exception {

        var t = getJobTopology(jobDetailsInfo);
        scalerInfo.updateVertexList(t.getVerticesInTopologicalOrder(), clock.instant(), conf);
        updateKafkaSourceMaxParallelisms(flinkService, conf, jobDetailsInfo.getJobId(), t);
        AutoScalerUtils.excludeVerticesFromScaling(conf, t.getFinishedVertices());
        return t;
    }

    @VisibleForTesting
    @SneakyThrows
    protected JobTopology getJobTopology(JobDetailsInfo jobDetailsInfo) {
        Map<JobVertexID, Integer> maxParallelismMap =
                jobDetailsInfo.getJobVertexInfos().stream()
                        .collect(
                                Collectors.toMap(
                                        JobDetailsInfo.JobVertexDetailsInfo::getJobVertexID,
                                        JobDetailsInfo.JobVertexDetailsInfo::getMaxParallelism));

        // Flink 1.17 introduced a strange behaviour where the JobDetailsInfo#getJsonPlan()
        // doesn't return the jsonPlan correctly but it wraps with RawJson{json='plan'}
        var rawPlan = jobDetailsInfo.getJsonPlan();
        var json = rawPlan.substring("RawJson{json='".length(), rawPlan.length() - "'}".length());

        return JobTopology.fromJsonPlan(
                json,
                maxParallelismMap,
                jobDetailsInfo.getJobVertexInfos().stream()
                        .filter(jvdi -> jvdi.getExecutionState() == ExecutionState.FINISHED)
                        .map(JobDetailsInfo.JobVertexDetailsInfo::getJobVertexID)
                        .collect(Collectors.toSet()));
    }

    private void updateKafkaSourceMaxParallelisms(
            FlinkService flinkService, Configuration conf, JobID jobId, JobTopology topology)
            throws Exception {
        try (var restClient = flinkService.getClusterClient(conf)) {
            var partitionRegex = Pattern.compile("^.*\\.partition\\.\\d+\\.currentOffset$");
            for (Map.Entry<JobVertexID, Set<JobVertexID>> entry : topology.getInputs().entrySet()) {
                if (entry.getValue().isEmpty()) {
                    var sourceVertex = entry.getKey();
                    var numPartitions =
                            queryAggregatedMetricNames(restClient, jobId, sourceVertex).stream()
                                    .map(AggregatedMetric::getId)
                                    .filter(partitionRegex.asMatchPredicate())
                                    .count();
                    if (numPartitions > 0) {
                        LOG.debug(
                                "Updating source {} max parallelism based on available partitions to {}",
                                sourceVertex,
                                numPartitions);
                        topology.updateMaxParallelism(sourceVertex, (int) numPartitions);
                    }
                }
            }
        }
    }

    /**
     * Given a map of collected Flink vertex metrics we compute the scaling metrics for each job
     * vertex.
     *
     * @param resourceID Resource ID of CR
     * @param collectedMetrics Collected Flink metrics
     * @param jobTopology Topology
     * @param conf Configuration
     * @return Collected metrics.
     */
    private CollectedMetrics convertToScalingMetrics(
            ResourceID resourceID,
            Map<JobVertexID, Map<FlinkMetric, AggregatedMetric>> collectedMetrics,
            JobTopology jobTopology,
            Configuration conf) {

        var out = new HashMap<JobVertexID, Map<ScalingMetric, Double>>();

        var finishedVertices = jobTopology.getFinishedVertices();
        if (!finishedVertices.isEmpty()) {
            collectedMetrics = new HashMap<>(collectedMetrics);
            for (JobVertexID v : finishedVertices) {
                collectedMetrics.put(v, FlinkMetric.FINISHED_METRICS);
            }
        }

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

                    ScalingMetrics.computeLoadMetrics(
                            jobVertexID, vertexFlinkMetrics, vertexScalingMetrics, conf);

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

                    vertexScalingMetrics
                            .entrySet()
                            .forEach(e -> e.setValue(ScalingMetrics.roundMetric(e.getValue())));

                    LOG.debug(
                            "Vertex scaling metrics for {}: {}", jobVertexID, vertexScalingMetrics);
                });

        var outputRatios = ScalingMetrics.computeOutputRatios(collectedMetrics, jobTopology);
        LOG.debug("Output ratios: {}", outputRatios);
        return new CollectedMetrics(out, outputRatios);
    }

    private double computeLagGrowthRate(
            ResourceID resourceID, JobVertexID jobVertexID, Double currentLag) {
        var metricHistory = histories.get(resourceID);

        if (metricHistory == null || metricHistory.isEmpty()) {
            return Double.NaN;
        }

        var lastCollectionTime = metricHistory.lastKey();
        var lastCollectedMetrics =
                metricHistory.get(lastCollectionTime).getVertexMetrics().get(jobVertexID);

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

    /** Query the available metric names for each job vertex. */
    @SneakyThrows
    protected Map<JobVertexID, Map<String, FlinkMetric>> queryFilteredMetricNames(
            FlinkService flinkService,
            AbstractFlinkResource<?, ?> cr,
            Configuration conf,
            JobTopology topology) {

        var jobId = JobID.fromHexString(cr.getStatus().getJobStatus().getJobId());
        var vertices = topology.getVerticesInTopologicalOrder();

        var resourceId = ResourceID.fromResource(cr);
        var names =
                availableVertexMetricNames.compute(
                        resourceId,
                        (k, previousMetricNames) -> {
                            if (previousMetricNames != null
                                    && previousMetricNames
                                            .keySet()
                                            .equals(topology.getParallelisms().keySet())) {
                                // We have already gathered the metric names for this topology
                                return previousMetricNames;
                            }

                            try (var restClient = flinkService.getClusterClient(conf)) {
                                return vertices.stream()
                                        .filter(v -> !topology.getFinishedVertices().contains(v))
                                        .collect(
                                                Collectors.toMap(
                                                        v -> v,
                                                        v ->
                                                                getFilteredVertexMetricNames(
                                                                        restClient,
                                                                        jobId,
                                                                        v,
                                                                        topology)));
                            } catch (Exception e) {
                                throw new RuntimeException(e);
                            }
                        });
        names.keySet().removeAll(topology.getFinishedVertices());
        return names;
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
            JobTopology topology) {

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
            List<String> pendingRecordsMetric = FlinkMetric.PENDING_RECORDS.findAll(allMetricNames);
            if (pendingRecordsMetric.isEmpty()) {
                LOG.warn(
                        "pendingRecords metric for {} could not be found. Either a legacy source or an idle source. Assuming no pending records.",
                        jobVertexID);
            }
            pendingRecordsMetric.forEach(m -> filteredMetrics.put(m, FlinkMetric.PENDING_RECORDS));
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
                        "Could not find required metric " + flinkMetric + " for " + jobVertexID);
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
    }

    @VisibleForTesting
    protected void setClock(Clock clock) {
        this.clock = Preconditions.checkNotNull(clock);
    }

    @VisibleForTesting
    protected Map<ResourceID, Map<JobVertexID, Map<String, FlinkMetric>>>
            getAvailableVertexMetricNames() {
        return availableVertexMetricNames;
    }

    @VisibleForTesting
    protected Map<ResourceID, SortedMap<Instant, CollectedMetrics>> getHistories() {
        return histories;
    }
}
