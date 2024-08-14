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

package org.apache.flink.autoscaler.jdbc.event;

import org.apache.flink.api.common.JobID;
import org.apache.flink.autoscaler.JobAutoScalerContext;
import org.apache.flink.autoscaler.ScalingSummary;
import org.apache.flink.autoscaler.event.AutoScalerEventHandler;
import org.apache.flink.autoscaler.event.AutoscalerEventUtils;
import org.apache.flink.autoscaler.event.VertexScalingReport;
import org.apache.flink.autoscaler.jdbc.testutils.databases.DatabaseTest;
import org.apache.flink.autoscaler.jdbc.testutils.databases.derby.DerbyTestBase;
import org.apache.flink.autoscaler.jdbc.testutils.databases.mysql.MySQL56TestBase;
import org.apache.flink.autoscaler.jdbc.testutils.databases.mysql.MySQL57TestBase;
import org.apache.flink.autoscaler.jdbc.testutils.databases.mysql.MySQL8TestBase;
import org.apache.flink.autoscaler.jdbc.testutils.databases.postgres.PostgreSQLTestBase;
import org.apache.flink.autoscaler.metrics.EvaluatedScalingMetric;
import org.apache.flink.autoscaler.metrics.ScalingMetric;
import org.apache.flink.runtime.jobgraph.JobVertexID;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import javax.annotation.Nonnull;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.temporal.ChronoUnit;
import java.util.Map;

import static org.apache.flink.autoscaler.TestingAutoscalerUtils.createDefaultJobAutoScalerContext;
import static org.assertj.core.api.Assertions.assertThat;

/** The abstract IT case for {@link JdbcAutoScalerEventHandler}. */
abstract class AbstractJdbcAutoscalerEventHandlerITCase implements DatabaseTest {

    private final String jobVertex = "1b51e99e55e89e404d9a0443fd98d9e2";
    private final Duration interval = Duration.ofMinutes(30);
    private final int currentParallelism = 1;
    private final int newParallelism = 2;
    private final double metricAvg = 10.1d;
    private final double metricCurrent = 20.5d;
    private final Instant createTime = Instant.now().truncatedTo(ChronoUnit.SECONDS);
    private final Map<JobVertexID, ScalingSummary> scalingSummaries =
            generateScalingSummaries(currentParallelism, newParallelism, metricAvg, metricCurrent);

    private CountableJdbcEventInteractor jdbcEventInteractor;
    private JdbcAutoScalerEventHandler<JobID, JobAutoScalerContext<JobID>> eventHandler;
    private JobAutoScalerContext<JobID> ctx;

    @BeforeEach
    void beforeEach() throws Exception {
        jdbcEventInteractor = new CountableJdbcEventInteractor(getConnection());
        jdbcEventInteractor.setClock(Clock.fixed(createTime, ZoneId.systemDefault()));
        eventHandler = new JdbcAutoScalerEventHandler<>(jdbcEventInteractor, Duration.ZERO);
        ctx = createDefaultJobAutoScalerContext();
    }

    /** All events shouldn't be deduplicated when interval is null. */
    @Test
    void testEventWithoutInterval() throws Exception {
        var reason = "ExpectedEventReason";
        var message = "ExpectedEventMessage";

        jdbcEventInteractor.assertCounters(0, 0, 0);

        eventHandler.handleEvent(
                ctx, AutoScalerEventHandler.Type.Normal, reason, message, null, null);
        jdbcEventInteractor.assertCounters(0, 0, 1);

        assertThat(jdbcEventInteractor.queryEvents(ctx.getJobKey().toString(), reason))
                .singleElement()
                .satisfies(event -> assertEvent(event, createTime, createTime, reason, message, 1));

        // Handler the same event.
        jdbcEventInteractor.setClock(
                Clock.fixed(createTime.plusSeconds(1), ZoneId.systemDefault()));
        eventHandler.handleEvent(
                ctx, AutoScalerEventHandler.Type.Normal, reason, message, null, null);
        jdbcEventInteractor.assertCounters(0, 0, 2);

        assertThat(jdbcEventInteractor.queryEvents(ctx.getJobKey().toString(), reason))
                .hasSize(2)
                .as("All events shouldn't be deduplicated when interval is null.")
                .satisfiesExactlyInAnyOrder(
                        event -> assertEvent(event, createTime, createTime, reason, message, 1),
                        event ->
                                assertEvent(
                                        event,
                                        createTime.plusSeconds(1),
                                        createTime.plusSeconds(1),
                                        reason,
                                        message,
                                        1));
    }

    /**
     * The message will be the message key, and the event should be deduplicated within interval.
     */
    @Test
    void testEventIntervalWithoutMessageKey() throws Exception {
        var reason = "ExpectedEventReason";
        var message = "ExpectedEventMessage";

        jdbcEventInteractor.assertCounters(0, 0, 0);
        eventHandler.handleEvent(
                ctx, AutoScalerEventHandler.Type.Normal, reason, message, null, interval);
        assertThat(jdbcEventInteractor.queryEvents(ctx.getJobKey().toString(), reason))
                .singleElement()
                .satisfies(event -> assertEvent(event, createTime, createTime, reason, message, 1));
        jdbcEventInteractor.assertCounters(1, 0, 1);

        // Handler the same event within interval.
        final Instant updateTime = createTime.plusSeconds(Duration.ofMinutes(20).toSeconds());
        jdbcEventInteractor.setClock(Clock.fixed(updateTime, ZoneId.systemDefault()));

        eventHandler.handleEvent(
                ctx, AutoScalerEventHandler.Type.Normal, reason, message, null, interval);
        jdbcEventInteractor.assertCounters(2, 1, 1);
        assertThat(jdbcEventInteractor.queryEvents(ctx.getJobKey().toString(), reason))
                .singleElement()
                .as(
                        "We expect to update old event instead of create a new one when handler the same event within interval.")
                .satisfies(event -> assertEvent(event, createTime, updateTime, reason, message, 2));

        // Handler the same event after interval.
        final Instant secondCreateTime = createTime.plusSeconds(Duration.ofMinutes(40).toSeconds());
        jdbcEventInteractor.setClock(Clock.fixed(secondCreateTime, ZoneId.systemDefault()));

        eventHandler.handleEvent(
                ctx, AutoScalerEventHandler.Type.Normal, reason, message, null, interval);
        jdbcEventInteractor.assertCounters(3, 1, 2);
        assertThat(jdbcEventInteractor.queryEvents(ctx.getJobKey().toString(), reason))
                .as("We expect to create a new event when handler the same event after interval.")
                .hasSize(2)
                .satisfiesExactlyInAnyOrder(
                        event -> assertEvent(event, createTime, updateTime, reason, message, 2),
                        event ->
                                assertEvent(
                                        event,
                                        secondCreateTime,
                                        secondCreateTime,
                                        reason,
                                        message,
                                        1));
    }

    /** The event should be deduplicated within interval when the message key is same. */
    @Test
    void testEventWithIntervalAndMessageKey() throws Exception {
        var reason = "ExpectedEventReason";
        var messageKey = "ExpectedMessageKey";
        var firstMessage = "FirstMessage";
        var secondMessage = "SecondMessage";
        var thirdMessage = "ThirdMessage";

        jdbcEventInteractor.assertCounters(0, 0, 0);
        eventHandler.handleEvent(
                ctx,
                AutoScalerEventHandler.Type.Normal,
                reason,
                firstMessage,
                messageKey,
                interval);
        assertThat(jdbcEventInteractor.queryEvents(ctx.getJobKey().toString(), reason))
                .singleElement()
                .satisfies(
                        event ->
                                assertEvent(
                                        event, createTime, createTime, reason, firstMessage, 1));
        jdbcEventInteractor.assertCounters(1, 0, 1);

        // Handler the same event within interval.
        final Instant updateTime = createTime.plusSeconds(Duration.ofMinutes(20).toSeconds());
        jdbcEventInteractor.setClock(Clock.fixed(updateTime, ZoneId.systemDefault()));

        eventHandler.handleEvent(
                ctx,
                AutoScalerEventHandler.Type.Normal,
                reason,
                secondMessage,
                messageKey,
                interval);
        jdbcEventInteractor.assertCounters(2, 1, 1);
        assertThat(jdbcEventInteractor.queryEvents(ctx.getJobKey().toString(), reason))
                .singleElement()
                .as(
                        "We expect to update old event instead of create a new one when handler the same event within interval.")
                .satisfies(
                        event ->
                                assertEvent(
                                        event, createTime, updateTime, reason, secondMessage, 2));

        // Handler the same event after interval.
        final Instant secondCreateTime = createTime.plusSeconds(Duration.ofMinutes(40).toSeconds());
        jdbcEventInteractor.setClock(Clock.fixed(secondCreateTime, ZoneId.systemDefault()));

        eventHandler.handleEvent(
                ctx,
                AutoScalerEventHandler.Type.Normal,
                reason,
                thirdMessage,
                messageKey,
                interval);
        jdbcEventInteractor.assertCounters(3, 1, 2);
        assertThat(jdbcEventInteractor.queryEvents(ctx.getJobKey().toString(), reason))
                .as("We expect to create a new event when handler the same event after interval.")
                .hasSize(2)
                .satisfiesExactlyInAnyOrder(
                        event ->
                                assertEvent(
                                        event, createTime, updateTime, reason, secondMessage, 2),
                        event ->
                                assertEvent(
                                        event,
                                        secondCreateTime,
                                        secondCreateTime,
                                        reason,
                                        thirdMessage,
                                        1));
    }

    /** All scaling events shouldn't be deduplicated when scaling happens. */
    @Test
    void testScalingEventWithScalingHappens() throws Exception {
        Map<JobVertexID, ScalingSummary> scalingSummaries =
                generateScalingSummaries(
                        currentParallelism, newParallelism, metricAvg, metricCurrent);

        jdbcEventInteractor.assertCounters(0, 0, 0);
        eventHandler.handleScalingEvent(
                ctx,
                scalingSummaries,
                AutoScalerEventHandler.SCALING_SUMMARY_HEADER_SCALING_EXECUTION_ENABLED,
                interval);
        jdbcEventInteractor.assertCounters(0, 0, 1);

        assertThat(
                        jdbcEventInteractor.queryEvents(
                                ctx.getJobKey().toString(),
                                AutoScalerEventHandler.SCALING_REPORT_REASON))
                .singleElement()
                .satisfies(
                        event -> {
                            assertScalingEvent(event, createTime, createTime, 1);
                            assertScalingReport(
                                    event.getMessage(),
                                    jobVertex,
                                    currentParallelism,
                                    newParallelism,
                                    metricAvg,
                                    metricCurrent,
                                    metricAvg);
                        });

        // Handler the same event.
        final Instant updateTime = createTime.plusSeconds(1);
        jdbcEventInteractor.setClock(Clock.fixed(updateTime, ZoneId.systemDefault()));

        eventHandler.handleScalingEvent(
                ctx,
                scalingSummaries,
                AutoScalerEventHandler.SCALING_SUMMARY_HEADER_SCALING_EXECUTION_ENABLED,
                interval);
        jdbcEventInteractor.assertCounters(0, 0, 2);

        assertThat(
                        jdbcEventInteractor.queryEvents(
                                ctx.getJobKey().toString(),
                                AutoScalerEventHandler.SCALING_REPORT_REASON))
                .as("All scaling events shouldn't be deduplicated when scaling happens.")
                .hasSize(2)
                .satisfiesExactlyInAnyOrder(
                        event -> assertScalingEvent(event, createTime, createTime, 1),
                        event -> assertScalingEvent(event, updateTime, updateTime, 1))
                .allSatisfy(
                        event ->
                                assertScalingReport(
                                        event.getMessage(),
                                        jobVertex,
                                        currentParallelism,
                                        newParallelism,
                                        metricAvg,
                                        metricCurrent,
                                        metricAvg));
    }

    /** The deduplication only works when parallelism is changed and within the interval. */
    @Test
    void testScalingEventDeduplication() throws Exception {
        createFirstScalingEvent();

        // The metric changed, but parallelism is not changed.
        final Instant updateTime = createTime.plusSeconds(1);
        jdbcEventInteractor.setClock(Clock.fixed(updateTime, ZoneId.systemDefault()));

        Map<JobVertexID, ScalingSummary> newScalingSummaries =
                generateScalingSummaries(currentParallelism, 2, 11.1d, metricCurrent);
        eventHandler.handleScalingEvent(
                ctx,
                newScalingSummaries,
                AutoScalerEventHandler.SCALING_SUMMARY_HEADER_SCALING_EXECUTION_DISABLED,
                interval);
        jdbcEventInteractor.assertCounters(2, 1, 1);

        assertThat(
                        jdbcEventInteractor.queryEvents(
                                ctx.getJobKey().toString(),
                                AutoScalerEventHandler.SCALING_REPORT_REASON))
                .as(
                        "The event should be deduplicated when parallelism is not changed and within the interval.")
                .singleElement()
                .satisfies(
                        event -> {
                            assertScalingEvent(event, createTime, updateTime, 2);
                            // Metric is changed.
                            assertScalingReport(
                                    event.getMessage(),
                                    jobVertex,
                                    currentParallelism,
                                    newParallelism,
                                    11.1d,
                                    metricCurrent,
                                    11.1d);
                        });
    }

    /** We should create a new event after the interval. */
    @Test
    void testScalingEventNotWithinInterval() throws Exception {
        createFirstScalingEvent();

        // The parallelism is not changed, but the old event is too early.
        final Instant newCreateTime = createTime.plusSeconds(Duration.ofMinutes(30).toSeconds());
        jdbcEventInteractor.setClock(Clock.fixed(newCreateTime, ZoneId.systemDefault()));

        eventHandler.handleScalingEvent(
                ctx,
                scalingSummaries,
                AutoScalerEventHandler.SCALING_SUMMARY_HEADER_SCALING_EXECUTION_DISABLED,
                interval);
        jdbcEventInteractor.assertCounters(2, 0, 2);

        assertThat(
                        jdbcEventInteractor.queryEvents(
                                ctx.getJobKey().toString(),
                                AutoScalerEventHandler.SCALING_REPORT_REASON))
                .as("We should create a new event when the old event is too early.")
                .hasSize(2)
                .satisfiesExactlyInAnyOrder(
                        event -> assertScalingEvent(event, createTime, createTime, 1),
                        event -> assertScalingEvent(event, newCreateTime, newCreateTime, 1))
                .allSatisfy(
                        event ->
                                assertScalingReport(
                                        event.getMessage(),
                                        jobVertex,
                                        currentParallelism,
                                        newParallelism,
                                        metricAvg,
                                        metricCurrent,
                                        metricAvg));
    }

    /** We should create a new event when the parallelism is changed. */
    @Test
    void testScalingEventWithParallelismChange() throws Exception {
        createFirstScalingEvent();

        // The parallelism is changed.
        final Instant newCreateTime = createTime.plusSeconds(1);
        jdbcEventInteractor.setClock(Clock.fixed(newCreateTime, ZoneId.systemDefault()));

        var secondNewParallelism = 3;
        Map<JobVertexID, ScalingSummary> newScalingSummaries =
                generateScalingSummaries(
                        currentParallelism, secondNewParallelism, metricAvg, metricCurrent);
        eventHandler.handleScalingEvent(
                ctx,
                newScalingSummaries,
                AutoScalerEventHandler.SCALING_SUMMARY_HEADER_SCALING_EXECUTION_DISABLED,
                interval);
        jdbcEventInteractor.assertCounters(2, 0, 2);

        assertThat(
                        jdbcEventInteractor.queryEvents(
                                ctx.getJobKey().toString(),
                                AutoScalerEventHandler.SCALING_REPORT_REASON))
                .as("We should create a new event when the old event is too early.")
                .hasSize(2)
                .satisfiesExactlyInAnyOrder(
                        event -> {
                            assertScalingEvent(event, createTime, createTime, 1);
                            assertScalingReport(
                                    event.getMessage(),
                                    jobVertex,
                                    currentParallelism,
                                    newParallelism,
                                    metricAvg,
                                    metricCurrent,
                                    metricAvg);
                        },
                        event -> {
                            assertScalingEvent(event, newCreateTime, newCreateTime, 1);
                            assertScalingReport(
                                    event.getMessage(),
                                    jobVertex,
                                    currentParallelism,
                                    secondNewParallelism,
                                    metricAvg,
                                    metricCurrent,
                                    metricAvg);
                        });
    }

    private void createFirstScalingEvent() throws Exception {
        jdbcEventInteractor.assertCounters(0, 0, 0);
        eventHandler.handleScalingEvent(
                ctx,
                scalingSummaries,
                AutoScalerEventHandler.SCALING_SUMMARY_HEADER_SCALING_EXECUTION_DISABLED,
                interval);
        jdbcEventInteractor.assertCounters(1, 0, 1);

        assertThat(
                        jdbcEventInteractor.queryEvents(
                                ctx.getJobKey().toString(),
                                AutoScalerEventHandler.SCALING_REPORT_REASON))
                .singleElement()
                .satisfies(
                        event -> {
                            assertScalingEvent(event, createTime, createTime, 1);
                            assertScalingReport(
                                    event.getMessage(),
                                    jobVertex,
                                    currentParallelism,
                                    newParallelism,
                                    metricAvg,
                                    metricCurrent,
                                    metricAvg);
                        });
    }

    private void assertScalingReport(
            String scalingReport,
            String expectedJobVertex,
            int expectedCurrentParallelism,
            int expectedNewParallelism,
            double expectedCurrentProcessCapacity,
            double expectedExpectedProcessCapacity,
            double expectedTargetDataRate) {
        var vertexScalingReports = AutoscalerEventUtils.parseVertexScalingReports(scalingReport);

        var expectedVertexScalingReport = new VertexScalingReport();
        expectedVertexScalingReport.setVertexId(expectedJobVertex);
        expectedVertexScalingReport.setCurrentParallelism(expectedCurrentParallelism);
        expectedVertexScalingReport.setNewParallelism(expectedNewParallelism);
        expectedVertexScalingReport.setCurrentProcessCapacity(expectedCurrentProcessCapacity);
        expectedVertexScalingReport.setExpectedProcessCapacity(expectedExpectedProcessCapacity);
        expectedVertexScalingReport.setTargetDataRate(expectedTargetDataRate);
        assertThat(vertexScalingReports).singleElement().isEqualTo(expectedVertexScalingReport);
    }

    @Nonnull
    private Map<JobVertexID, ScalingSummary> generateScalingSummaries(
            int currentParallelism, int newParallelism, double metricAvg, double metricCurrent) {
        var jobVertexID = JobVertexID.fromHexString(jobVertex);
        var evaluatedScalingMetric = new EvaluatedScalingMetric();
        evaluatedScalingMetric.setAverage(metricAvg);
        evaluatedScalingMetric.setCurrent(metricCurrent);
        return Map.of(
                jobVertexID,
                new ScalingSummary(
                        currentParallelism,
                        newParallelism,
                        Map.of(
                                ScalingMetric.TRUE_PROCESSING_RATE,
                                evaluatedScalingMetric,
                                ScalingMetric.EXPECTED_PROCESSING_RATE,
                                evaluatedScalingMetric,
                                ScalingMetric.TARGET_DATA_RATE,
                                evaluatedScalingMetric)));
    }

    private void assertEvent(
            AutoScalerEvent event,
            Instant expectedCreateTime,
            Instant expectedUpdateTime,
            String expectedReason,
            String expectedMessage,
            int expectedCount) {
        assertThat(event.getCreateTime()).isEqualTo(expectedCreateTime);
        assertThat(event.getUpdateTime()).isEqualTo(expectedUpdateTime);
        assertThat(event.getJobKey()).isEqualTo(ctx.getJobKey().toString());
        assertThat(event.getReason()).isEqualTo(expectedReason);
        assertThat(event.getEventType()).isEqualTo(AutoScalerEventHandler.Type.Normal.toString());
        assertThat(event.getMessage()).isEqualTo(expectedMessage);
        assertThat(event.getCount()).isEqualTo(expectedCount);
    }

    private void assertScalingEvent(
            AutoScalerEvent event,
            Instant expectedCreateTime,
            Instant expectedUpdateTime,
            int expectedCount) {
        assertThat(event.getCreateTime()).isEqualTo(expectedCreateTime);
        assertThat(event.getUpdateTime()).isEqualTo(expectedUpdateTime);
        assertThat(event.getJobKey()).isEqualTo(ctx.getJobKey().toString());
        assertThat(event.getReason()).isEqualTo(AutoScalerEventHandler.SCALING_REPORT_REASON);
        assertThat(event.getEventType()).isEqualTo(AutoScalerEventHandler.Type.Normal.toString());
        assertThat(event.getCount()).isEqualTo(expectedCount);
    }
}

/** Test {@link JdbcAutoScalerEventHandler} via Derby. */
class DerbyJdbcAutoscalerEventHandlerITCase extends AbstractJdbcAutoscalerEventHandlerITCase
        implements DerbyTestBase {}

/** Test {@link JdbcAutoScalerEventHandler} via MySQL 5.6.x. */
class MySQL56JdbcAutoscalerEventHandlerITCase extends AbstractJdbcAutoscalerEventHandlerITCase
        implements MySQL56TestBase {}

/** Test {@link JdbcAutoScalerEventHandler} via MySQL 5.7.x. */
class MySQL57JdbcAutoscalerEventHandlerITCase extends AbstractJdbcAutoscalerEventHandlerITCase
        implements MySQL57TestBase {}

/** Test {@link JdbcAutoScalerEventHandler} via MySQL 8.x. */
class MySQL8JdbcAutoscalerEventHandlerITCase extends AbstractJdbcAutoscalerEventHandlerITCase
        implements MySQL8TestBase {}

/** Test {@link JdbcAutoScalerEventHandler} via Postgre SQL. */
class PostgreSQLJdbcAutoscalerEventHandlerITCase extends AbstractJdbcAutoscalerEventHandlerITCase
        implements PostgreSQLTestBase {}
