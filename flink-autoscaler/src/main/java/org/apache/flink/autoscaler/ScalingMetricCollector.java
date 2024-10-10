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

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.autoscaler.config.AutoScalerOptions;
import org.apache.flink.autoscaler.exceptions.NotReadyException;
import org.apache.flink.autoscaler.metrics.CollectedMetricHistory;
import org.apache.flink.autoscaler.metrics.CollectedMetrics;
import org.apache.flink.autoscaler.metrics.FlinkMetric;
import org.apache.flink.autoscaler.metrics.MetricNotFoundException;
import org.apache.flink.autoscaler.metrics.ScalingMetric;
import org.apache.flink.autoscaler.metrics.ScalingMetrics;
import org.apache.flink.autoscaler.state.AutoScalerStateStore;
import org.apache.flink.autoscaler.topology.IOMetrics;
import org.apache.flink.autoscaler.topology.JobTopology;
import org.apache.flink.client.program.rest.RestClusterClient;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.rest.messages.EmptyRequestBody;
import org.apache.flink.runtime.rest.messages.JobIDPathParameter;
import org.apache.flink.runtime.rest.messages.JobVertexIdPathParameter;
import org.apache.flink.runtime.rest.messages.job.JobDetailsInfo;
import org.apache.flink.runtime.rest.messages.job.metrics.AggregatedMetric;
import org.apache.flink.runtime.rest.messages.job.metrics.AggregatedSubtaskMetricsHeaders;
import org.apache.flink.runtime.rest.messages.job.metrics.AggregatedSubtaskMetricsParameters;
import org.apache.flink.runtime.rest.messages.job.metrics.Metric;
import org.apache.flink.util.Preconditions;

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
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.SortedMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.flink.autoscaler.metrics.ScalingHistoryUtils.updateVertexList;
import static org.apache.flink.autoscaler.utils.AutoScalerUtils.excludeVerticesFromScaling;
import static org.apache.flink.autoscaler.utils.DateTimeUtils.readable;
import static org.apache.flink.util.Preconditions.checkState;

/** Metric collector using flink rest api. */
public abstract class ScalingMetricCollector<KEY, Context extends JobAutoScalerContext<KEY>> {
    private static final Logger LOG = LoggerFactory.getLogger(ScalingMetricCollector.class);

    private final Map<KEY, Map<JobVertexID, Map<String, FlinkMetric>>> availableVertexMetricNames =
            new ConcurrentHashMap<>();
    private final Map<KEY, SortedMap<Instant, CollectedMetrics>> histories =
            new ConcurrentHashMap<>();
    protected final Map<KEY, Boolean> jobsWithGcMetrics = new ConcurrentHashMap<KEY, Boolean>();

    private Clock clock = Clock.systemDefaultZone();

    public CollectedMetricHistory updateMetrics(
            Context ctx, AutoScalerStateStore<KEY, Context> stateStore) throws Exception {

        var jobKey = ctx.getJobKey();
        var conf = ctx.getConfiguration();

        var now = clock.instant();

        var metricHistory =
                histories.computeIfAbsent(
                        jobKey,
                        (k) -> {
                            try {
                                return stateStore.getCollectedMetrics(ctx);
                            } catch (Exception exception) {
                                throw new RuntimeException(
                                        "Get evaluated metrics failed.", exception);
                            }
                        });

        var jobDetailsInfo =
                getJobDetailsInfo(ctx, conf.get(AutoScalerOptions.FLINK_CLIENT_TIMEOUT));
        var jobRunningTs = getJobRunningTs(jobDetailsInfo);
        // We detect job change compared to our collected metrics by checking against the earliest
        // metric timestamp
        if (!metricHistory.isEmpty() && jobRunningTs.isAfter(metricHistory.firstKey())) {
            LOG.info("Job updated at {}. Clearing metrics.", readable(jobRunningTs));
            stateStore.removeCollectedMetrics(ctx);
            cleanup(ctx.getJobKey());
            metricHistory.clear();
        }
        var topology = getJobTopology(ctx, stateStore, jobDetailsInfo);
        var stableTime = jobRunningTs.plus(conf.get(AutoScalerOptions.STABILIZATION_INTERVAL));
        final boolean isStabilizing = now.isBefore(stableTime);

        // Calculate timestamp when the metric windows is full
        var metricWindowSize = getMetricWindowSize(conf);
        var windowFullTime =
                getWindowFullTime(metricHistory.tailMap(stableTime), now, metricWindowSize);

        // The filtered list of metrics we want to query for each vertex
        var filteredVertexMetricNames = queryFilteredMetricNames(ctx, topology, isStabilizing);

        // Aggregated job vertex metrics collected from Flink based on the filtered metric names
        var collectedVertexMetrics = queryAllAggregatedMetrics(ctx, filteredVertexMetricNames);

        var collectedJmMetrics = queryJmMetrics(ctx);
        var collectedTmMetrics = queryTmMetrics(ctx);

        // The computed scaling metrics based on the collected aggregated vertex metrics
        var scalingMetrics =
                convertToScalingMetrics(
                        jobKey,
                        collectedVertexMetrics,
                        collectedJmMetrics,
                        collectedTmMetrics,
                        topology,
                        conf);

        // Add scaling metrics to history if they were computed successfully
        metricHistory.put(now, scalingMetrics);

        if (isStabilizing) {
            LOG.info("Stabilizing until {}", readable(stableTime));
            stateStore.storeCollectedMetrics(ctx, metricHistory);
            return new CollectedMetricHistory(topology, Collections.emptySortedMap(), jobRunningTs);
        }

        var collectedMetrics = new CollectedMetricHistory(topology, metricHistory, jobRunningTs);
        if (now.isBefore(windowFullTime)) {
            LOG.info("Metric window not full until {}", readable(windowFullTime));
        } else {
            collectedMetrics.setFullyCollected(true);
            // Trim metrics outside the metric window from metrics history
            metricHistory.headMap(now.minus(metricWindowSize)).clear();
        }
        stateStore.storeCollectedMetrics(ctx, metricHistory);
        return collectedMetrics;
    }

    protected abstract Map<FlinkMetric, Metric> queryJmMetrics(Context ctx) throws Exception;

    protected abstract Map<FlinkMetric, AggregatedMetric> queryTmMetrics(Context ctx)
            throws Exception;

    protected Duration getMetricWindowSize(Configuration conf) {
        return conf.get(AutoScalerOptions.METRICS_WINDOW);
    }

    private static Instant getWindowFullTime(
            SortedMap<Instant, CollectedMetrics> metricsAfterStable,
            Instant now,
            Duration metricWindowSize) {
        return metricsAfterStable.isEmpty()
                ? now.plus(metricWindowSize)
                : metricsAfterStable.firstKey().plus(metricWindowSize);
    }

    @VisibleForTesting
    protected Instant getJobRunningTs(JobDetailsInfo jobDetailsInfo) {
        final Map<JobStatus, Long> timestamps = jobDetailsInfo.getTimestamps();

        final Long runningTs = timestamps.get(JobStatus.RUNNING);
        checkState(runningTs != null, "Unable to find when the job was switched to RUNNING.");
        return Instant.ofEpochMilli(runningTs);
    }

    protected JobTopology getJobTopology(
            Context ctx,
            AutoScalerStateStore<KEY, Context> stateStore,
            JobDetailsInfo jobDetailsInfo)
            throws Exception {

        var t = getJobTopology(jobDetailsInfo);

        Set<JobVertexID> vertexSet = Set.copyOf(t.getVerticesInTopologicalOrder());
        updateVertexList(stateStore, ctx, clock.instant(), vertexSet);
        updateKafkaPulsarSourceNumPartitions(ctx, jobDetailsInfo.getJobId(), t);
        excludeVerticesFromScaling(ctx.getConfiguration(), t.getFinishedVertices());
        return t;
    }

    @VisibleForTesting
    @SneakyThrows
    protected JobTopology getJobTopology(JobDetailsInfo jobDetailsInfo) {
        var slotSharingGroupIdMap =
                jobDetailsInfo.getJobVertexInfos().stream()
                        .filter(e -> e.getSlotSharingGroupId() != null)
                        .collect(
                                Collectors.toMap(
                                        JobDetailsInfo.JobVertexDetailsInfo::getJobVertexID,
                                        JobDetailsInfo.JobVertexDetailsInfo
                                                ::getSlotSharingGroupId));
        var maxParallelismMap =
                jobDetailsInfo.getJobVertexInfos().stream()
                        .collect(
                                Collectors.toMap(
                                        JobDetailsInfo.JobVertexDetailsInfo::getJobVertexID,
                                        JobDetailsInfo.JobVertexDetailsInfo::getMaxParallelism));

        // Flink 1.17 introduced a strange behaviour where the JobDetailsInfo#getJsonPlan()
        // doesn't return the jsonPlan correctly but it wraps with RawJson{json='plan'}
        var rawPlan = jobDetailsInfo.getJsonPlan();
        var json = rawPlan.substring("RawJson{json='".length(), rawPlan.length() - "'}".length());

        var metrics = new HashMap<JobVertexID, IOMetrics>();
        var finished = new HashSet<JobVertexID>();
        jobDetailsInfo
                .getJobVertexInfos()
                .forEach(
                        d -> {
                            if (d.getExecutionState() == ExecutionState.FINISHED) {
                                finished.add(d.getJobVertexID());
                            }
                            metrics.put(
                                    d.getJobVertexID(), IOMetrics.from(d.getJobVertexMetrics()));
                        });

        return JobTopology.fromJsonPlan(
                json, slotSharingGroupIdMap, maxParallelismMap, metrics, finished);
    }

    private void updateKafkaPulsarSourceNumPartitions(
            Context ctx, JobID jobId, JobTopology topology) throws Exception {
        try (var restClient = ctx.getRestClusterClient()) {
            Pattern partitionRegex =
                    Pattern.compile(
                            "^.*\\.KafkaSourceReader\\.topic\\.(?<kafkaTopic>.+)\\.partition\\.(?<kafkaId>\\d+)\\.currentOffset$"
                                    + "|^.*\\.PulsarConsumer\\.(?<pulsarTopic>.+)-partition-(?<pulsarId>\\d+)\\..*\\.numMsgsReceived$");
            for (var vertexInfo : topology.getVertexInfos().values()) {
                if (vertexInfo.getInputs().isEmpty()) {
                    var sourceVertex = vertexInfo.getId();
                    var numPartitions =
                            queryAggregatedMetricNames(restClient, jobId, sourceVertex).stream()
                                    .map(
                                            v -> {
                                                Matcher matcher = partitionRegex.matcher(v);
                                                if (matcher.matches()) {
                                                    String kafkaTopic = matcher.group("kafkaTopic");
                                                    String kafkaId = matcher.group("kafkaId");
                                                    String pulsarTopic =
                                                            matcher.group("pulsarTopic");
                                                    String pulsarId = matcher.group("pulsarId");
                                                    return kafkaTopic != null
                                                            ? kafkaTopic + "-" + kafkaId
                                                            : pulsarTopic + "-" + pulsarId;
                                                }
                                                return null;
                                            })
                                    .filter(Objects::nonNull)
                                    .collect(Collectors.toSet())
                                    .size();
                    if (numPartitions > 0) {
                        LOG.debug(
                                "Updating source {} max parallelism based on available partitions to {}",
                                sourceVertex,
                                numPartitions);
                        topology.get(sourceVertex).setNumSourcePartitions((int) numPartitions);
                    }
                }
            }
        }
    }

    /**
     * Given a series of collected Flink vertex metrics we compute the scaling metrics for each job
     * vertex.
     */
    private CollectedMetrics convertToScalingMetrics(
            KEY jobKey,
            Map<JobVertexID, Map<FlinkMetric, AggregatedMetric>> collectedMetrics,
            Map<FlinkMetric, Metric> collectedJmMetrics,
            Map<FlinkMetric, AggregatedMetric> collectedTmMetrics,
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
                            jobVertexID,
                            vertexFlinkMetrics,
                            vertexScalingMetrics,
                            jobTopology.get(jobVertexID).getIoMetrics(),
                            conf);

                    var metricHistory =
                            histories.getOrDefault(jobKey, Collections.emptySortedMap());

                    ScalingMetrics.computeDataRateMetrics(
                            jobVertexID,
                            vertexFlinkMetrics,
                            vertexScalingMetrics,
                            jobTopology,
                            conf,
                            observedTprAvg(
                                    jobVertexID,
                                    metricHistory,
                                    conf.get(
                                            AutoScalerOptions
                                                    .OBSERVED_TRUE_PROCESSING_RATE_MIN_OBSERVATIONS)));
                    vertexScalingMetrics
                            .entrySet()
                            .forEach(e -> e.setValue(ScalingMetrics.roundMetric(e.getValue())));

                    LOG.debug(
                            "Vertex scaling metrics for {}: {}", jobVertexID, vertexScalingMetrics);
                });

        var globalMetrics =
                ScalingMetrics.computeGlobalMetrics(collectedJmMetrics, collectedTmMetrics, conf);
        LOG.debug("Global metrics: {}", globalMetrics);

        return new CollectedMetrics(out, globalMetrics);
    }

    private static Supplier<Double> observedTprAvg(
            JobVertexID jobVertexID,
            SortedMap<Instant, CollectedMetrics> metricHistory,
            int minObservations) {
        return () ->
                ScalingMetricEvaluator.getAverage(
                        ScalingMetric.OBSERVED_TPR, jobVertexID, metricHistory, minObservations);
    }

    private Map<JobVertexID, Map<String, FlinkMetric>> queryFilteredMetricNames(
            Context ctx, JobTopology topology, boolean isStabilizing) {
        try {
            return queryFilteredMetricNames(ctx, topology);
        } catch (MetricNotFoundException e) {
            if (isStabilizing) {
                throw new NotReadyException(e);
            }
            throw e;
        }
    }

    /** Query the available metric names for each job vertex. */
    protected Map<JobVertexID, Map<String, FlinkMetric>> queryFilteredMetricNames(
            Context ctx, JobTopology topology) {

        var vertices = topology.getVerticesInTopologicalOrder();

        var names =
                availableVertexMetricNames.compute(
                        ctx.getJobKey(),
                        (k, previousMetricNames) -> {
                            if (previousMetricNames != null
                                    && previousMetricNames
                                            .keySet()
                                            .equals(topology.getVertexInfos().keySet())) {
                                var newMetricNames = new HashMap<>(previousMetricNames);
                                var sourceMetricNames =
                                        queryFilteredMetricNames(
                                                ctx,
                                                topology,
                                                vertices.stream().filter(topology::isSource));
                                newMetricNames.putAll(sourceMetricNames);
                                return newMetricNames;
                            }

                            // Query all metric names
                            return queryFilteredMetricNames(ctx, topology, vertices.stream());
                        });
        names.keySet().removeAll(topology.getFinishedVertices());
        return names;
    }

    @SneakyThrows
    private Map<JobVertexID, Map<String, FlinkMetric>> queryFilteredMetricNames(
            Context ctx, JobTopology topology, Stream<JobVertexID> vertexStream) {
        try (var restClient = ctx.getRestClusterClient()) {
            return vertexStream
                    .filter(v -> !topology.getFinishedVertices().contains(v))
                    .collect(
                            Collectors.toMap(
                                    v -> v,
                                    v ->
                                            getFilteredVertexMetricNames(
                                                    restClient, ctx.getJobID(), v, topology)));
        }
    }

    /** Query and filter metric names for a given job vertex. */
    Map<String, FlinkMetric> getFilteredVertexMetricNames(
            RestClusterClient<?> restClient,
            JobID jobID,
            JobVertexID jobVertexID,
            JobTopology topology) {

        var allMetricNames = queryAggregatedMetricNames(restClient, jobID, jobVertexID);
        var filteredMetrics = new HashMap<String, FlinkMetric>();
        var requiredMetrics = new HashSet<FlinkMetric>();

        requiredMetrics.add(FlinkMetric.BUSY_TIME_PER_SEC);

        if (topology.isSource(jobVertexID)) {
            requiredMetrics.add(FlinkMetric.BACKPRESSURE_TIME_PER_SEC);
            requiredMetrics.add(FlinkMetric.SOURCE_TASK_NUM_RECORDS_IN_PER_SEC);
            requiredMetrics.add(FlinkMetric.SOURCE_TASK_NUM_RECORDS_IN);
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
            FlinkMetric.SOURCE_TASK_NUM_RECORDS_OUT
                    .findAny(allMetricNames)
                    .ifPresent(
                            m -> filteredMetrics.put(m, FlinkMetric.SOURCE_TASK_NUM_RECORDS_OUT));
        }

        for (FlinkMetric flinkMetric : requiredMetrics) {
            Optional<String> flinkMetricName = flinkMetric.findAny(allMetricNames);
            if (flinkMetricName.isPresent()) {
                // Add actual Flink metric name to list
                filteredMetrics.put(flinkMetricName.get(), flinkMetric);
            } else {
                throw new MetricNotFoundException(flinkMetric, jobVertexID);
            }
        }

        return filteredMetrics;
    }

    @VisibleForTesting
    @SneakyThrows
    protected Collection<String> queryAggregatedMetricNames(
            RestClusterClient<?> restClient, JobID jobID, JobVertexID jobVertexID) {
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
                .getMetrics()
                .stream()
                .map(AggregatedMetric::getId)
                .collect(Collectors.toSet());
    }

    protected abstract Map<JobVertexID, Map<FlinkMetric, AggregatedMetric>>
            queryAllAggregatedMetrics(
                    Context ctx,
                    Map<JobVertexID, Map<String, FlinkMetric>> filteredVertexMetricNames);

    public JobDetailsInfo getJobDetailsInfo(
            JobAutoScalerContext<KEY> context, Duration clientTimeout) throws Exception {
        try (var restClient = context.getRestClusterClient()) {
            return restClient
                    .getJobDetails(context.getJobID())
                    .get(clientTimeout.toSeconds(), TimeUnit.SECONDS);
        }
    }

    public void cleanup(KEY jobKey) {
        histories.remove(jobKey);
        availableVertexMetricNames.remove(jobKey);
        jobsWithGcMetrics.remove(jobKey);
    }

    @VisibleForTesting
    protected void setClock(Clock clock) {
        this.clock = Preconditions.checkNotNull(clock);
    }

    @VisibleForTesting
    protected Map<KEY, Map<JobVertexID, Map<String, FlinkMetric>>> getAvailableVertexMetricNames() {
        return availableVertexMetricNames;
    }

    @VisibleForTesting
    protected Map<KEY, SortedMap<Instant, CollectedMetrics>> getHistories() {
        return histories;
    }
}
