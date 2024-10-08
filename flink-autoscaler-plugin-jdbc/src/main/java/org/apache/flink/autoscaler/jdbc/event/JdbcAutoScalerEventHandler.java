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

import org.apache.flink.annotation.Experimental;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.autoscaler.JobAutoScalerContext;
import org.apache.flink.autoscaler.ScalingSummary;
import org.apache.flink.autoscaler.event.AutoScalerEventHandler;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.util.Preconditions;

import org.apache.flink.shaded.guava31.com.google.common.util.concurrent.ThreadFactoryBuilder;

import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

import javax.annotation.Nullable;

import java.sql.Timestamp;
import java.time.Duration;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * The event handler which persists its event in JDBC related database.
 *
 * @param <KEY> The job key.
 * @param <Context> The job autoscaler context.
 */
@Experimental
@Slf4j
public class JdbcAutoScalerEventHandler<KEY, Context extends JobAutoScalerContext<KEY>>
        implements AutoScalerEventHandler<KEY, Context> {

    private final JdbcEventInteractor jdbcEventInteractor;
    private final Duration eventHandlerTtl;
    @Nullable private final ScheduledExecutorService scheduledEventHandlerCleaner;

    public JdbcAutoScalerEventHandler(
            JdbcEventInteractor jdbcEventInteractor, Duration eventHandlerTtl) {
        this.jdbcEventInteractor = jdbcEventInteractor;
        this.eventHandlerTtl = Preconditions.checkNotNull(eventHandlerTtl);

        if (eventHandlerTtl.toMillis() <= 0) {
            this.scheduledEventHandlerCleaner = null;
            return;
        }
        this.scheduledEventHandlerCleaner =
                Executors.newSingleThreadScheduledExecutor(
                        new ThreadFactoryBuilder()
                                .setNameFormat("jdbc-autoscaler-events-cleaner")
                                .setDaemon(true)
                                .build());
        this.scheduledEventHandlerCleaner.scheduleAtFixedRate(
                this::cleanExpiredEvents,
                Duration.ofDays(1).toMillis(),
                Duration.ofDays(1).toMillis(),
                TimeUnit.MILLISECONDS);
    }

    @SneakyThrows
    @Override
    public void handleEvent(
            Context context,
            Type type,
            String reason,
            String message,
            @Nullable String messageKey,
            @Nullable Duration interval) {
        final var jobKey = context.getJobKey().toString();
        var eventKey =
                Integer.toString(
                        Objects.hash(
                                jobKey, type, reason, messageKey != null ? messageKey : message));
        if (interval == null) {
            // Don't deduplicate when interval is null.
            jdbcEventInteractor.createEvent(jobKey, reason, type, message, eventKey);
            return;
        }

        final var oldEventOpt = jdbcEventInteractor.queryLatestEvent(jobKey, reason, eventKey);
        // Updating the old event when old event is present and the old event is created within
        // interval to avoid generating a large number of duplicate events.
        // Creating a new event when old event isn't present or old event is created before
        // interval.
        if (oldEventOpt.isPresent() && intervalCheck(oldEventOpt.get(), interval)) {
            final var oldEvent = oldEventOpt.get();
            jdbcEventInteractor.updateEvent(oldEvent.getId(), message, oldEvent.getCount() + 1);
        } else {
            jdbcEventInteractor.createEvent(jobKey, reason, type, message, eventKey);
        }
    }

    @Override
    public void handleScalingEvent(
            Context context,
            Map<JobVertexID, ScalingSummary> scalingSummaries,
            String message,
            Duration interval) {
        if (message.contains(SCALING_SUMMARY_HEADER_SCALING_EXECUTION_ENABLED)) {
            // Don't deduplicate when scaling happens.
            AutoScalerEventHandler.super.handleScalingEvent(
                    context, scalingSummaries, message, null);
        } else {
            // When scaling doesn't happen, autoscaler will generate a lot of scaling event.
            // So we deduplicate event based on the parallelism hashcode. If the recommended
            // parallelism isn't changed, we only create a new ScalingReport event per interval.
            handleEvent(
                    context,
                    Type.Normal,
                    SCALING_REPORT_REASON,
                    AutoScalerEventHandler.scalingReport(scalingSummaries, message),
                    AutoScalerEventHandler.getParallelismHashCode(scalingSummaries),
                    interval);
        }
    }

    @Override
    public void close() {
        if (Objects.nonNull(scheduledEventHandlerCleaner)
                && !scheduledEventHandlerCleaner.isShutdown()) {
            scheduledEventHandlerCleaner.shutdownNow();
        }
    }

    @VisibleForTesting
    void cleanExpiredEvents() {
        final var batchSize = 2000;
        final var sleepMs = 1000;

        var date =
                Timestamp.from(
                        jdbcEventInteractor
                                .getCurrentInstant()
                                .minusMillis(eventHandlerTtl.toMillis()));
        try {
            var deletedTotalCount = 0L;
            while (true) {
                Long minId = jdbcEventInteractor.queryMinEventIdByCreateTime(date);
                if (Objects.isNull(minId)) {
                    log.info(
                            "Deleted expired {} event handler records successfully",
                            deletedTotalCount);
                    break;
                }

                boolean cleanable = true;
                for (long startId = minId; cleanable; startId += batchSize) {
                    int deleted =
                            jdbcEventInteractor.deleteExpiredEventsByIdRangeAndDate(
                                    startId, startId + batchSize, date);
                    cleanable = deleted == batchSize;
                    deletedTotalCount += deleted;
                    Thread.sleep(sleepMs);
                }
            }
        } catch (Exception e) {
            log.error("Error in cleaning expired event handler records.", e);
        }
    }

    /**
     * @return True means the existing event is still in the interval duration we can update it.
     *     Otherwise, it's too early, we should create a new one instead of updating it.
     */
    private boolean intervalCheck(AutoScalerEvent existing, Duration interval) {
        return existing.getCreateTime()
                .isAfter(jdbcEventInteractor.getCurrentInstant().minusMillis(interval.toMillis()));
    }
}
