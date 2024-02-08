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

import org.apache.flink.api.common.JobID;
import org.apache.flink.autoscaler.metrics.FlinkMetric;
import org.apache.flink.autoscaler.metrics.TestMetrics;
import org.apache.flink.autoscaler.topology.JobTopology;
import org.apache.flink.client.program.rest.RestClusterClient;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.rest.messages.job.JobDetailsInfo;
import org.apache.flink.runtime.rest.messages.job.metrics.AggregatedMetric;
import org.apache.flink.runtime.rest.messages.job.metrics.Metric;

import lombok.Setter;

import java.time.Duration;
import java.time.Instant;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Consumer;

/** Testing {@link ScalingMetricCollector} implementation. */
public class TestingMetricsCollector<KEY, Context extends JobAutoScalerContext<KEY>>
        extends ScalingMetricCollector<KEY, Context> {

    @Setter private JobTopology jobTopology;

    @Setter private Duration testMetricWindowSize;

    @Setter private Instant jobUpdateTs = Instant.ofEpochMilli(0);

    private Map<JobVertexID, TestMetrics> metrics = new HashMap<>();

    @Setter private Map<JobVertexID, Map<FlinkMetric, AggregatedMetric>> currentMetrics;

    public void updateMetrics(JobVertexID v, TestMetrics tm) {
        metrics.put(v, tm);
    }

    public void updateMetrics(JobVertexID v, Consumer<TestMetrics>... consumers) {
        for (Consumer<TestMetrics> consumer : consumers) {
            consumer.accept(metrics.get(v));
        }
    }

    @Setter private Map<JobVertexID, Collection<String>> metricNames = new HashMap<>();

    public TestingMetricsCollector(JobTopology jobTopology) {
        this.jobTopology = jobTopology;
    }

    @Override
    protected JobTopology getJobTopology(JobDetailsInfo jobDetailsInfo) {
        return jobTopology;
    }

    @Override
    protected Map<JobVertexID, Map<FlinkMetric, AggregatedMetric>> queryAllAggregatedMetrics(
            Context ctx, Map<JobVertexID, Map<String, FlinkMetric>> filteredVertexMetricNames) {

        if (currentMetrics != null) {
            return currentMetrics;
        }

        var out = new HashMap<JobVertexID, Map<FlinkMetric, AggregatedMetric>>();
        metrics.forEach(
                (v, m) -> {
                    jobTopology.get(v).setIoMetrics(m.toIoMetrics());
                    out.put(v, m.toFlinkMetrics());
                });
        return out;
    }

    @Override
    protected Map<JobVertexID, Map<String, FlinkMetric>> queryFilteredMetricNames(
            Context ctx, JobTopology topology) {
        return Collections.emptyMap();
    }

    @Override
    protected Collection<String> queryAggregatedMetricNames(
            RestClusterClient<?> restClient, JobID jobID, JobVertexID jobVertexID) {
        return metricNames.getOrDefault(jobVertexID, Collections.emptyList());
    }

    @Override
    protected Map<FlinkMetric, Metric> queryJmMetrics(Context ctx) throws Exception {
        return Map.of();
    }

    @Override
    protected Map<FlinkMetric, AggregatedMetric> queryTmMetrics(Context ctx) {
        return Map.of();
    }

    @Override
    protected Duration getMetricWindowSize(Configuration conf) {
        if (testMetricWindowSize != null) {
            return testMetricWindowSize;
        }
        return super.getMetricWindowSize(conf);
    }

    @Override
    protected Instant getJobRunningTs(JobDetailsInfo jobDetailsInfo) {
        return jobUpdateTs;
    }
}
