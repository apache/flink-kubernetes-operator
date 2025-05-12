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

package org.apache.flink.kubernetes.operator.api.utils;

import org.apache.flink.api.common.JobStatus;
import org.apache.flink.kubernetes.operator.api.status.JobManagerDeploymentStatus;

import io.fabric8.kubernetes.api.model.Condition;
import io.fabric8.kubernetes.api.model.ConditionBuilder;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;

/** Creates a condition object with the type, status, message and reason. */
public class ConditionUtils {
    public static final String CONDITION_TYPE_RUNNING = "Running";

    public static Condition crCondition(Condition condition) {
        return new ConditionBuilder(condition)
                .withLastTransitionTime(
                        new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'").format(new Date()))
                .build();
    }

    public static final Map<String, Condition> SESSION_MODE_CONDITION =
            Map.of(
                    JobManagerDeploymentStatus.READY.name(),
                    new ConditionBuilder()
                            .withType(CONDITION_TYPE_RUNNING)
                            .withStatus("True")
                            .withReason("Ready")
                            .withMessage(
                                    "JobManager is running and ready to receive REST API calls")
                            .build(),
                    JobManagerDeploymentStatus.MISSING.name(),
                    new ConditionBuilder()
                            .withType(CONDITION_TYPE_RUNNING)
                            .withStatus("False")
                            .withReason("Missing")
                            .withMessage("JobManager deployment not found")
                            .build(),
                    JobManagerDeploymentStatus.DEPLOYING.name(),
                    new ConditionBuilder()
                            .withType(CONDITION_TYPE_RUNNING)
                            .withStatus("False")
                            .withReason("Deploying")
                            .withMessage("JobManager process is starting up")
                            .build(),
                    JobManagerDeploymentStatus.DEPLOYED_NOT_READY.name(),
                    new ConditionBuilder()
                            .withType(CONDITION_TYPE_RUNNING)
                            .withStatus("False")
                            .withReason("DeployedNotReady")
                            .withMessage(
                                    "JobManager is running but not ready yet to receive REST API calls")
                            .build(),
                    JobManagerDeploymentStatus.ERROR.name(),
                    new ConditionBuilder()
                            .withType(CONDITION_TYPE_RUNNING)
                            .withStatus("False")
                            .withReason("Error")
                            .withMessage("JobManager deployment failed")
                            .build());

    public static final Map<String, Condition> APPLICATION_MODE_CONDITION =
            Map.of(
                    JobStatus.RECONCILING.name(),
                    new ConditionBuilder()
                            .withType(CONDITION_TYPE_RUNNING)
                            .withStatus("False")
                            .withReason("Reconciling")
                            .withMessage("Job is currently reconciling")
                            .build(),
                    JobStatus.CREATED.name(),
                    new ConditionBuilder()
                            .withType(CONDITION_TYPE_RUNNING)
                            .withStatus("False")
                            .withReason("JobCreated")
                            .withMessage("Job is created")
                            .build(),
                    JobStatus.RUNNING.name(),
                    new ConditionBuilder()
                            .withType(CONDITION_TYPE_RUNNING)
                            .withStatus("True")
                            .withReason("JobRunning")
                            .withMessage("Job is running")
                            .build(),
                    JobStatus.FAILING.name(),
                    new ConditionBuilder()
                            .withType(CONDITION_TYPE_RUNNING)
                            .withStatus("False")
                            .withReason("JobFailing")
                            .withMessage("Job has failed")
                            .build(),
                    JobStatus.RESTARTING.name(),
                    new ConditionBuilder()
                            .withType(CONDITION_TYPE_RUNNING)
                            .withStatus("False")
                            .withReason("JobRestarting")
                            .withMessage("The job is currently restarting")
                            .build(),
                    JobStatus.FAILED.name(),
                    new ConditionBuilder()
                            .withType(CONDITION_TYPE_RUNNING)
                            .withStatus("False")
                            .withReason("JobFailed")
                            .withMessage("The job has failed with a non-recoverable task failure")
                            .build(),
                    JobStatus.FINISHED.name(),
                    new ConditionBuilder()
                            .withType(CONDITION_TYPE_RUNNING)
                            .withStatus("False")
                            .withReason("JobFinished")
                            .withMessage("Job's tasks have successfully finished")
                            .build(),
                    JobStatus.CANCELED.name(),
                    new ConditionBuilder()
                            .withType(CONDITION_TYPE_RUNNING)
                            .withStatus("False")
                            .withReason("JobCancelled")
                            .withMessage("Job has been cancelled")
                            .build(),
                    JobStatus.SUSPENDED.name(),
                    new ConditionBuilder()
                            .withType(CONDITION_TYPE_RUNNING)
                            .withStatus("False")
                            .withReason("JobSuspended")
                            .withMessage("The job has been suspended")
                            .build());
}
