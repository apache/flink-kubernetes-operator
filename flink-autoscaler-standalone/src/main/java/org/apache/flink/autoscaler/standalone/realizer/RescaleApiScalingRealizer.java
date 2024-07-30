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

package org.apache.flink.autoscaler.standalone.realizer;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.autoscaler.JobAutoScalerContext;
import org.apache.flink.autoscaler.config.AutoScalerOptions;
import org.apache.flink.autoscaler.event.AutoScalerEventHandler;
import org.apache.flink.autoscaler.realizer.ScalingRealizer;
import org.apache.flink.autoscaler.tuning.ConfigChanges;
import org.apache.flink.client.program.rest.RestClusterClient;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.runtime.jobgraph.JobResourceRequirements;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobgraph.JobVertexResourceRequirements;
import org.apache.flink.runtime.rest.messages.EmptyRequestBody;
import org.apache.flink.runtime.rest.messages.JobMessageParameters;
import org.apache.flink.runtime.rest.messages.job.JobResourceRequirementsBody;
import org.apache.flink.runtime.rest.messages.job.JobResourceRequirementsHeaders;
import org.apache.flink.runtime.rest.messages.job.JobResourcesRequirementsUpdateHeaders;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * A ScalingRealizer which uses the Rescale API to apply parallelism changes.
 *
 * <p>Note: This is based on code copied from the operator, and they don't depend on each other, so
 * some code is duplicated.
 */
public class RescaleApiScalingRealizer<KEY, Context extends JobAutoScalerContext<KEY>>
        implements ScalingRealizer<KEY, Context> {

    private static final Logger LOG = LoggerFactory.getLogger(RescaleApiScalingRealizer.class);

    @VisibleForTesting static final String SCALING = "Scaling";

    private final AutoScalerEventHandler<KEY, Context> eventHandler;

    public RescaleApiScalingRealizer(AutoScalerEventHandler<KEY, Context> eventHandler) {
        this.eventHandler = eventHandler;
    }

    @Override
    public void realizeParallelismOverrides(
            Context context, Map<String, String> parallelismOverrides) throws Exception {
        Configuration conf = context.getConfiguration();
        if (!conf.get(JobManagerOptions.SCHEDULER)
                .equals(JobManagerOptions.SchedulerType.Adaptive)) {
            LOG.warn("In-place rescaling is only available with the adaptive scheduler.");
            return;
        }

        var jobID = context.getJobID();
        if (JobStatus.RUNNING != context.getJobStatus()) {
            LOG.warn("Job in terminal or reconciling state cannot be scaled in-place.");
            return;
        }

        var flinkRestClientTimeout = conf.get(AutoScalerOptions.FLINK_CLIENT_TIMEOUT);

        try (var client = context.getRestClusterClient()) {
            var requirements =
                    new HashMap<>(getVertexResources(client, jobID, flinkRestClientTimeout));
            var parallelismUpdated = false;

            for (Map.Entry<JobVertexID, JobVertexResourceRequirements> entry :
                    requirements.entrySet()) {
                var jobVertexId = entry.getKey().toString();
                var parallelism = entry.getValue().getParallelism();
                var overrideStr = parallelismOverrides.get(jobVertexId);

                // No overrides for this vertex
                if (overrideStr == null) {
                    continue;
                }

                // We have an override for the vertex
                var p = Integer.parseInt(overrideStr);
                var newParallelism = new JobVertexResourceRequirements.Parallelism(1, p);
                // If the requirements changed we mark this as scaling triggered
                if (!parallelism.equals(newParallelism)) {
                    entry.setValue(new JobVertexResourceRequirements(newParallelism));
                    parallelismUpdated = true;
                }
            }
            if (parallelismUpdated) {
                updateVertexResources(client, jobID, flinkRestClientTimeout, requirements);
                eventHandler.handleEvent(
                        context,
                        AutoScalerEventHandler.Type.Normal,
                        SCALING,
                        String.format(
                                "In-place scaling triggered, the new requirements is %s.",
                                requirements),
                        null,
                        null);
            } else {
                LOG.info("Vertex resources requirements already match target, nothing to do...");
            }
        }
    }

    @Override
    public void realizeConfigOverrides(Context context, ConfigChanges configChanges) {
        // Not currently supported
        LOG.warn(
                "{} does not support updating the TaskManager configuration ({})",
                getClass().getSimpleName(),
                configChanges);
    }

    private Map<JobVertexID, JobVertexResourceRequirements> getVertexResources(
            RestClusterClient<String> client, JobID jobID, Duration restClientTimeout)
            throws Exception {
        var jobParameters = new JobMessageParameters();
        jobParameters.jobPathParameter.resolve(jobID);

        var currentRequirements =
                client.sendRequest(
                                new JobResourceRequirementsHeaders(),
                                jobParameters,
                                EmptyRequestBody.getInstance())
                        .get(restClientTimeout.toSeconds(), TimeUnit.SECONDS);

        return currentRequirements.asJobResourceRequirements().get().getJobVertexParallelisms();
    }

    private void updateVertexResources(
            RestClusterClient<String> client,
            JobID jobID,
            Duration restClientTimeout,
            Map<JobVertexID, JobVertexResourceRequirements> newReqs)
            throws Exception {
        var jobParameters = new JobMessageParameters();
        jobParameters.jobPathParameter.resolve(jobID);

        var requestBody = new JobResourceRequirementsBody(new JobResourceRequirements(newReqs));

        client.sendRequest(new JobResourcesRequirementsUpdateHeaders(), jobParameters, requestBody)
                .get(restClientTimeout.toSeconds(), TimeUnit.SECONDS);
    }
}
