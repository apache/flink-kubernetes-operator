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

package org.apache.flink.autoscaler.event;

import org.apache.flink.autoscaler.ScalingSummary;
import org.apache.flink.autoscaler.metrics.EvaluatedScalingMetric;
import org.apache.flink.runtime.jobgraph.JobVertexID;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import static org.apache.flink.autoscaler.metrics.ScalingMetric.EXPECTED_PROCESSING_RATE;
import static org.apache.flink.autoscaler.metrics.ScalingMetric.TARGET_DATA_RATE;
import static org.apache.flink.autoscaler.metrics.ScalingMetric.TRUE_PROCESSING_RATE;
import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link AutoScalerEventHandler}. */
class AutoScalerEventHandlerTest {

    private static List<Locale> localesProvider() {
        return List.of(
                Locale.ENGLISH,
                Locale.UK,
                Locale.US,
                Locale.GERMAN,
                Locale.CHINA,
                Locale.CANADA,
                Locale.ITALIAN,
                Locale.JAPANESE,
                Locale.KOREA);
    }

    @ParameterizedTest
    @MethodSource("localesProvider")
    void testScalingReport(Locale locale) {
        Locale.setDefault(locale);
        var expectedJson =
                String.format(
                        "Scaling execution enabled, begin scaling vertices:"
                                + "{ Vertex ID ea632d67b7d595e5b851708ae9ad79d6 | Parallelism 3 -> 1 | "
                                + "Processing capacity %.2f -> %.2f | Target data rate %.2f}"
                                + "{ Vertex ID bc764cd8ddf7a0cff126f51c16239658 | Parallelism 4 -> 2 | "
                                + "Processing capacity %.2f -> %.2f | Target data rate %.2f}"
                                + "{ Vertex ID 0a448493b4782967b150582570326227 | Parallelism 5 -> 8 | "
                                + "Processing capacity %.2f -> %.2f | Target data rate %.2f}",
                        12424.68,
                        123.40,
                        403.67,
                        Double.POSITIVE_INFINITY,
                        Double.POSITIVE_INFINITY,
                        812.58,
                        404.73,
                        645.00,
                        404.27);

        final var scalingSummaries = buildScalingSummaries();
        assertThat(
                        AutoScalerEventHandler.scalingReport(
                                scalingSummaries,
                                AutoScalerEventHandler
                                        .SCALING_SUMMARY_HEADER_SCALING_EXECUTION_ENABLED))
                .isEqualTo(expectedJson);

        assertThat(AutoscalerEventUtils.parseVertexScalingReports(expectedJson))
                .containsExactlyInAnyOrderElementsOf(buildExpectedScalingResults());
    }

    private HashMap<JobVertexID, ScalingSummary> buildScalingSummaries() {
        final var jobVertex1 = JobVertexID.fromHexString("0a448493b4782967b150582570326227");
        final var jobVertex2 = JobVertexID.fromHexString("bc764cd8ddf7a0cff126f51c16239658");
        final var jobVertex3 = JobVertexID.fromHexString("ea632d67b7d595e5b851708ae9ad79d6");

        final var scalingSummaries = new HashMap<JobVertexID, ScalingSummary>();
        scalingSummaries.put(
                jobVertex2,
                generateScalingSummary(
                        4, 2, Double.POSITIVE_INFINITY, Double.POSITIVE_INFINITY, 812.583));
        scalingSummaries.put(jobVertex3, generateScalingSummary(3, 1, 12424.678, 123.4, 403.673));
        scalingSummaries.put(jobVertex1, generateScalingSummary(5, 8, 404.727, 645.0, 404.268));

        return scalingSummaries;
    }

    private List<VertexScalingReport> buildExpectedScalingResults() {
        return List.of(
                new VertexScalingReport(
                        "0a448493b4782967b150582570326227", 5, 8, 404.73, 645.0, 404.27),
                new VertexScalingReport(
                        "bc764cd8ddf7a0cff126f51c16239658",
                        4,
                        2,
                        Double.POSITIVE_INFINITY,
                        Double.POSITIVE_INFINITY,
                        812.58),
                new VertexScalingReport(
                        "ea632d67b7d595e5b851708ae9ad79d6", 3, 1, 12424.68, 123.4, 403.67));
    }

    private ScalingSummary generateScalingSummary(
            int currentParallelism,
            int newParallelism,
            double currentProcessCapacity,
            double expectedProcessCapacity,
            double targetDataRate) {
        final var scalingSummary = new ScalingSummary();
        scalingSummary.setCurrentParallelism(currentParallelism);
        scalingSummary.setNewParallelism(newParallelism);

        final var metrics =
                Map.of(
                        TRUE_PROCESSING_RATE,
                        new EvaluatedScalingMetric(400, currentProcessCapacity),
                        EXPECTED_PROCESSING_RATE,
                        new EvaluatedScalingMetric(expectedProcessCapacity, 400),
                        TARGET_DATA_RATE,
                        new EvaluatedScalingMetric(400, targetDataRate));
        scalingSummary.setMetrics(metrics);
        return scalingSummary;
    }
}
