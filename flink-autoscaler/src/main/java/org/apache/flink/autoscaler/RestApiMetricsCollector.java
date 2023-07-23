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

import org.apache.flink.autoscaler.metrics.FlinkMetric;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.rest.messages.EmptyRequestBody;
import org.apache.flink.runtime.rest.messages.JobIDPathParameter;
import org.apache.flink.runtime.rest.messages.JobVertexIdPathParameter;
import org.apache.flink.runtime.rest.messages.job.metrics.AggregatedMetric;
import org.apache.flink.runtime.rest.messages.job.metrics.AggregatedSubtaskMetricsHeaders;
import org.apache.flink.runtime.rest.messages.job.metrics.AggregatedSubtaskMetricsParameters;

import lombok.SneakyThrows;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.stream.Collectors;

/** Metric collector using flink rest api. */
public class RestApiMetricsCollector<KEY, INFO> extends ScalingMetricCollector<KEY, INFO> {
    private static final Logger LOG = LoggerFactory.getLogger(RestApiMetricsCollector.class);

    @Override
    protected Map<JobVertexID, Map<FlinkMetric, AggregatedMetric>> queryAllAggregatedMetrics(
            JobAutoScalerContext<KEY, INFO> context,
            Map<JobVertexID, Map<String, FlinkMetric>> filteredVertexMetricNames) {

        return filteredVertexMetricNames.entrySet().stream()
                .collect(
                        Collectors.toMap(
                                Map.Entry::getKey,
                                e ->
                                        queryAggregatedVertexMetrics(
                                                context, e.getKey(), e.getValue())));
    }

    @SneakyThrows
    protected Map<FlinkMetric, AggregatedMetric> queryAggregatedVertexMetrics(
            JobAutoScalerContext<KEY, INFO> context,
            JobVertexID jobVertexID,
            Map<String, FlinkMetric> metrics) {

        LOG.debug("Querying metrics {} for {}", metrics, jobVertexID);

        var parameters = new AggregatedSubtaskMetricsParameters();
        var pathIt = parameters.getPathParameters().iterator();

        ((JobIDPathParameter) pathIt.next()).resolve(context.getJobID());
        ((JobVertexIdPathParameter) pathIt.next()).resolve(jobVertexID);

        parameters
                .getQueryParameters()
                .iterator()
                .next()
                .resolveFromString(StringUtils.join(metrics.keySet(), ","));

        try (var restClient = context.getRestClusterClient()) {

            var responseBody =
                    restClient
                            .sendRequest(
                                    AggregatedSubtaskMetricsHeaders.getInstance(),
                                    parameters,
                                    EmptyRequestBody.getInstance())
                            .get();

            return responseBody.getMetrics().stream()
                    .collect(
                            Collectors.toMap(
                                    m -> metrics.get(m.getId()),
                                    m -> m,
                                    (m1, m2) ->
                                            new AggregatedMetric(
                                                    m1.getId() + " merged with " + m2.getId(),
                                                    Math.min(m1.getMin(), m2.getMin()),
                                                    Math.max(m1.getMax(), m2.getMax()),
                                                    // Average can't be computed
                                                    Double.NaN,
                                                    m1.getSum() + m2.getSum())));
        }
    }
}
