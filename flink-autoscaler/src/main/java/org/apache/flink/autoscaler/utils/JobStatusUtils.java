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

package org.apache.flink.autoscaler.utils;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.runtime.client.JobStatusMessage;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.messages.webmonitor.JobDetails;
import org.apache.flink.runtime.messages.webmonitor.MultipleJobsDetails;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.stream.Collectors;

/** Job status related utilities. */
public class JobStatusUtils {

    private static final Logger LOG = LoggerFactory.getLogger(JobStatusUtils.class);

    public static List<JobStatusMessage> toJobStatusMessage(
            MultipleJobsDetails multipleJobsDetails) {
        return multipleJobsDetails.getJobs().stream()
                .map(JobStatusUtils::toJobStatusMessage)
                .collect(Collectors.toList());
    }

    public static JobStatusMessage toJobStatusMessage(JobDetails details) {
        return new JobStatusMessage(
                details.getJobId(),
                details.getJobName(),
                getEffectiveStatus(details),
                details.getStartTime());
    }

    @VisibleForTesting
    static JobStatus getEffectiveStatus(JobDetails details) {
        int numRunning = details.getTasksPerState()[ExecutionState.RUNNING.ordinal()];
        int numFinished = details.getTasksPerState()[ExecutionState.FINISHED.ordinal()];
        boolean allRunningOrFinished = details.getNumTasks() == (numRunning + numFinished);
        JobStatus effectiveStatus = details.getStatus();
        if (JobStatus.RUNNING.equals(effectiveStatus) && !allRunningOrFinished) {
            LOG.debug("Adjusting job state from {} to {}", JobStatus.RUNNING, JobStatus.CREATED);
            return JobStatus.CREATED;
        }
        return effectiveStatus;
    }
}
