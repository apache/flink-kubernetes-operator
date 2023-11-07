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

import org.apache.flink.shaded.guava30.com.google.common.util.concurrent.ThreadFactoryBuilder;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;

import java.io.Closeable;
import java.time.Duration;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

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

    public StandaloneAutoscalerExecutor(
            @Nonnull Duration scalingInterval,
            @Nonnull JobListFetcher<KEY, Context> jobListFetcher,
            @Nonnull AutoScalerEventHandler<KEY, Context> eventHandler,
            @Nonnull JobAutoScaler<KEY, Context> autoScaler) {
        this.scalingInterval = scalingInterval;
        this.jobListFetcher = jobListFetcher;
        this.eventHandler = eventHandler;
        this.autoScaler = autoScaler;
        this.scheduledExecutorService =
                Executors.newSingleThreadScheduledExecutor(
                        new ThreadFactoryBuilder()
                                .setNameFormat("StandaloneAutoscalerControlLoop")
                                .setDaemon(false)
                                .build());
    }

    public void start() {
        LOG.info("Schedule control loop.");
        scheduledExecutorService.scheduleWithFixedDelay(
                this::scaling, 0, scalingInterval.toMillis(), TimeUnit.MILLISECONDS);
    }

    @Override
    public void close() {
        scheduledExecutorService.shutdownNow();
    }

    @VisibleForTesting
    protected void scaling() {
        LOG.info("Standalone autoscaler starts scaling.");
        try {
            var jobList = jobListFetcher.fetch();
            for (var jobContext : jobList) {
                try {
                    autoScaler.scale(jobContext);
                } catch (Throwable e) {
                    LOG.error("Error while scaling job", e);
                    eventHandler.handleEvent(
                            jobContext,
                            AutoScalerEventHandler.Type.Warning,
                            AUTOSCALER_ERROR,
                            e.getMessage(),
                            null,
                            null);
                }
            }
        } catch (Throwable e) {
            LOG.error("Error while fetch job list.", e);
        }
    }
}
