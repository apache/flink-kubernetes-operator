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

package org.apache.flink.autoscaler.handler;

import org.apache.flink.autoscaler.JobAutoScalerContext;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * The logged auto scaler event handler.
 *
 * @param <KEY>
 * @param <INFO>
 */
public class LoggerAutoScalerEventHandler<KEY, INFO> implements AutoScalerEventHandler<KEY, INFO> {

    private static final Logger LOG = LoggerFactory.getLogger(LoggerAutoScalerEventHandler.class);

    @Override
    public void handlerScalingFailure(
            JobAutoScalerContext<KEY, INFO> context,
            FailureReason failureReason,
            String errorMessage) {
        String logMessage =
                String.format(
                        "The scaling of job %s fails with reason %s, the error message is %s.",
                        context.getJobKey(), failureReason, errorMessage);
        if (failureReason.isError()) {
            LOG.warn(logMessage);
        } else {
            LOG.info(logMessage);
        }
    }

    @Override
    public void handlerScalingReport(
            JobAutoScalerContext<KEY, INFO> context, String scalingReportMessage) {
        LOG.info("The scaling report of job {} is {}", context.getJobKey(), scalingReportMessage);
    }

    @Override
    public void handlerRecommendedParallelism(
            JobAutoScalerContext<KEY, INFO> context, Map<String, String> recommendedParallelism) {
        LOG.info(
                "The recommended parallelism of job {} is {}",
                context.getJobKey(),
                recommendedParallelism);
    }
}
