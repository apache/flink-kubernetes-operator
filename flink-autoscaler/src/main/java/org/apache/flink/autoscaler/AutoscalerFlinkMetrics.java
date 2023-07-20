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

import org.apache.flink.autoscaler.metrics.EvaluatedScalingMetric;
import org.apache.flink.autoscaler.metrics.ScalingMetric;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.runtime.jobgraph.JobVertexID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Supplier;

/** Autoscaler metrics for observability. */
public class AutoscalerFlinkMetrics {

    private static final Logger LOG = LoggerFactory.getLogger(AutoscalerFlinkMetrics.class);

    private final Counter numScalings;

    private final Counter numErrors;

    private final Counter numBalanced;

    private final MetricGroup metricGroup;

    private final Set<JobVertexID> vertexMetrics = new HashSet<>();

    public AutoscalerFlinkMetrics(MetricGroup metricGroup) {
        this.numScalings = metricGroup.counter("scalings");
        this.numErrors = metricGroup.counter("errors");
        this.numBalanced = metricGroup.counter("balanced");
        this.metricGroup = metricGroup;
    }

    public Counter getNumScalings() {
        return numScalings;
    }

    public Counter getNumErrors() {
        return numErrors;
    }

    public Counter getNumBalanced() {
        return numBalanced;
    }

    public void registerScalingMetrics(
            Supplier<Map<JobVertexID, Map<ScalingMetric, EvaluatedScalingMetric>>>
                    currentVertexMetrics) {
        currentVertexMetrics
                .get()
                .forEach(
                        (jobVertexID, evaluated) -> {
                            if (!vertexMetrics.add(jobVertexID)) {
                                return;
                            }
                            LOG.info("Registering scaling metrics for job vertex {}", jobVertexID);
                            var jobVertexMg =
                                    metricGroup.addGroup("jobVertexID", jobVertexID.toHexString());

                            evaluated.forEach(
                                    (sm, esm) -> {
                                        var smGroup = jobVertexMg.addGroup(sm.name());

                                        smGroup.gauge(
                                                "Current",
                                                () ->
                                                        Optional.ofNullable(
                                                                        currentVertexMetrics.get())
                                                                .map(m -> m.get(jobVertexID))
                                                                .map(metrics -> metrics.get(sm))
                                                                .map(
                                                                        EvaluatedScalingMetric
                                                                                ::getCurrent)
                                                                .orElse(null));

                                        if (sm.isCalculateAverage()) {
                                            smGroup.gauge(
                                                    "Average",
                                                    () ->
                                                            Optional.ofNullable(
                                                                            currentVertexMetrics
                                                                                    .get())
                                                                    .map(m -> m.get(jobVertexID))
                                                                    .map(metrics -> metrics.get(sm))
                                                                    .map(
                                                                            EvaluatedScalingMetric
                                                                                    ::getAverage)
                                                                    .orElse(null));
                                        }
                                    });
                        });
    }
}
