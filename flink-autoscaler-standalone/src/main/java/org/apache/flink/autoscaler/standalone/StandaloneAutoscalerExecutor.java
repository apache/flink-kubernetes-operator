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

package org.apache.flink.autoscaler.standalone;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.autoscaler.JobAutoScaler;
import org.apache.flink.autoscaler.JobAutoScalerContext;
import org.apache.flink.autoscaler.event.AutoScalerEventHandler;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.concurrent.ExecutorThreadFactory;

import org.apache.flink.shaded.guava31.com.google.common.util.concurrent.ThreadFactoryBuilder;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import javax.annotation.Nonnull;

import java.io.Closeable;
import java.time.Duration;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static org.apache.flink.autoscaler.standalone.config.AutoscalerStandaloneOptions.CONTROL_LOOP_INTERVAL;
import static org.apache.flink.autoscaler.standalone.config.AutoscalerStandaloneOptions.CONTROL_LOOP_PARALLELISM;

/** The executor of the standalone autoscaler. */
public class StandaloneAutoscalerExecutor<KEY, Context extends JobAutoScalerContext<KEY>>
        implements Closeable {

    private static final Logger LOG = LoggerFactory.getLogger(StandaloneAutoscalerExecutor.class);

    @VisibleForTesting protected static final String AUTOSCALER_ERROR = "AutoscalerError";

    private final Duration scalingInterval;
    private final JobListFetcher<KEY, Context> jobListFetcher;
    private final AutoScalerEventHandler<KEY, Context> eventHandler;
    private final JobAutoScaler<KEY, Context> autoScaler;
    private final ScheduledExecutorService scheduledExecutorService;
    private final ExecutorService scalingThreadPool;

    /**
     * Maintain a set of job keys that during scaling, it should be updated at {@link
     * #scheduledExecutorService} thread.
     */
    private final Set<KEY> scalingJobKeys;

    public StandaloneAutoscalerExecutor(
            @Nonnull Configuration conf,
            @Nonnull JobListFetcher<KEY, Context> jobListFetcher,
            @Nonnull AutoScalerEventHandler<KEY, Context> eventHandler,
            @Nonnull JobAutoScaler<KEY, Context> autoScaler) {
        this.scalingInterval = conf.get(CONTROL_LOOP_INTERVAL);
        this.jobListFetcher = jobListFetcher;
        this.eventHandler = eventHandler;
        this.autoScaler = autoScaler;
        this.scheduledExecutorService =
                Executors.newSingleThreadScheduledExecutor(
                        new ThreadFactoryBuilder()
                                .setNameFormat("autoscaler-standalone-control-loop")
                                .setDaemon(false)
                                .build());

        int parallelism = conf.get(CONTROL_LOOP_PARALLELISM);
        this.scalingThreadPool =
                Executors.newFixedThreadPool(
                        parallelism, new ExecutorThreadFactory("autoscaler-standalone-scaling"));
        this.scalingJobKeys = new HashSet<>();
    }

    public void start() {
        LOG.info("Schedule control loop.");
        scheduledExecutorService.scheduleWithFixedDelay(
                this::scaling, 0, scalingInterval.toMillis(), TimeUnit.MILLISECONDS);
    }

    @Override
    public void close() {
        scheduledExecutorService.shutdownNow();
        scalingThreadPool.shutdownNow();
    }

    @VisibleForTesting
    protected void scaling() {
        LOG.info("Standalone autoscaler starts scaling.");
        Collection<Context> jobList;
        try {
            jobList = jobListFetcher.fetch();
        } catch (Throwable e) {
            LOG.error("Error while fetch job list.", e);
            return;
        }

        for (var jobContext : jobList) {
            final var jobKey = jobContext.getJobKey();
            if (scalingJobKeys.contains(jobKey)) {
                continue;
            }
            scalingJobKeys.add(jobKey);
            CompletableFuture.runAsync(() -> scalingSingleJob(jobContext), scalingThreadPool)
                    .whenCompleteAsync(
                            (result, throwable) -> {
                                if (throwable != null) {
                                    LOG.error(
                                            "Error while jobKey: {} executing scaling .",
                                            jobKey,
                                            throwable);
                                }
                                scalingJobKeys.remove(jobKey);
                            },
                            scheduledExecutorService);
        }
    }

    @VisibleForTesting
    protected void scalingSingleJob(Context jobContext) {
        try {
            MDC.put("job.key", jobContext.getJobKey().toString());
            autoScaler.scale(jobContext);
        } catch (Throwable e) {
            LOG.error("Error while scaling job", e);
            eventHandler.handleException(jobContext, AUTOSCALER_ERROR, e);
        } finally {
            MDC.clear();
        }
    }
}
