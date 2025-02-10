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

package org.apache.flink.kubernetes.operator.observer.sessionjob;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.kubernetes.operator.api.FlinkSessionJob;
import org.apache.flink.kubernetes.operator.api.status.FlinkSessionJobStatus;
import org.apache.flink.kubernetes.operator.controller.FlinkResourceContext;
import org.apache.flink.kubernetes.operator.observer.AbstractFlinkResourceObserver;
import org.apache.flink.kubernetes.operator.observer.JobStatusObserver;
import org.apache.flink.kubernetes.operator.observer.SnapshotObserver;
import org.apache.flink.kubernetes.operator.utils.EventRecorder;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** The observer of {@link FlinkSessionJob}. */
public class FlinkSessionJobObserver extends AbstractFlinkResourceObserver<FlinkSessionJob> {

    private static final Logger LOG = LoggerFactory.getLogger(FlinkSessionJobObserver.class);

    private final JobStatusObserver<FlinkSessionJob> jobStatusObserver;
    private final SnapshotObserver<FlinkSessionJob, FlinkSessionJobStatus> savepointObserver;

    public FlinkSessionJobObserver(EventRecorder eventRecorder) {
        super(eventRecorder);
        this.jobStatusObserver = new JobStatusObserver<>(eventRecorder);
        this.savepointObserver = new SnapshotObserver<>(eventRecorder);
    }

    @Override
    protected boolean isResourceReadyToBeObserved(FlinkResourceContext<FlinkSessionJob> ctx) {
        return super.isResourceReadyToBeObserved(ctx) && ctx.getFlinkService() != null;
    }

    @Override
    protected void observeInternal(FlinkResourceContext<FlinkSessionJob> ctx) {
        var jobFound = jobStatusObserver.observe(ctx);
        if (jobFound) {
            savepointObserver.observeSavepointStatus(ctx);
            savepointObserver.observeCheckpointStatus(ctx);
        }
    }

    @Override
    protected boolean checkIfAlreadyUpgraded(FlinkResourceContext<FlinkSessionJob> ctx) {
        var flinkSessionJob = ctx.getResource();
        try {
            var jobStatus = flinkSessionJob.getStatus().getJobStatus();
            if (jobStatus.getJobId() == null) {
                // No job was submitted
                return false;
            }
            var jobId = JobID.fromHexString(jobStatus.getJobId());
            var deployed =
                    ctx.getFlinkService()
                            .getJobStatus(ctx.getObserveConfig(), jobId)
                            .map(
                                    jsm ->
                                            !JobStatus.CANCELLING.equals(jsm.getJobState())
                                                    && !JobStatus.CANCELED.equals(
                                                            jsm.getJobState()))
                            .orElse(false);
            if (deployed) {
                LOG.info("Job with id {} is already deployed.", jobId);
                return true;
            } else {
                return false;
            }
        } catch (Exception e) {
            throw new RuntimeException("Failed to list jobs", e);
        }
    }
}
