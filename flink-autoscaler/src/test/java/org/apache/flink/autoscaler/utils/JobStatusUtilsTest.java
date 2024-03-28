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

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.messages.webmonitor.JobDetails;

import org.junit.jupiter.api.Test;

import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertEquals;

/** Test for {@link JobStatusUtils}. */
class JobStatusUtilsTest {

    @Test
    void effectiveStatusTest() {
        var allRunning = getJobDetails(JobStatus.RUNNING, Tuple2.of(ExecutionState.RUNNING, 4));
        assertEquals(JobStatus.RUNNING, JobStatusUtils.getEffectiveStatus(allRunning));

        var allRunningOrFinished =
                getJobDetails(
                        JobStatus.RUNNING,
                        Tuple2.of(ExecutionState.RUNNING, 2),
                        Tuple2.of(ExecutionState.FINISHED, 2));
        assertEquals(JobStatus.RUNNING, JobStatusUtils.getEffectiveStatus(allRunningOrFinished));

        var allRunningOrScheduled =
                getJobDetails(
                        JobStatus.RUNNING,
                        Tuple2.of(ExecutionState.RUNNING, 2),
                        Tuple2.of(ExecutionState.SCHEDULED, 2));
        assertEquals(JobStatus.CREATED, JobStatusUtils.getEffectiveStatus(allRunningOrScheduled));

        var allFinished = getJobDetails(JobStatus.FINISHED, Tuple2.of(ExecutionState.FINISHED, 4));
        assertEquals(JobStatus.FINISHED, JobStatusUtils.getEffectiveStatus(allFinished));
    }

    @SafeVarargs
    private JobDetails getJobDetails(
            JobStatus status, Tuple2<ExecutionState, Integer>... tasksPerState) {
        int[] countPerState = new int[ExecutionState.values().length];
        for (var taskPerState : tasksPerState) {
            countPerState[taskPerState.f0.ordinal()] = taskPerState.f1;
        }
        int numTasks = Arrays.stream(countPerState).sum();
        return new JobDetails(
                new JobID(),
                "test-job",
                System.currentTimeMillis(),
                -1,
                0,
                status,
                System.currentTimeMillis(),
                countPerState,
                numTasks);
    }
}
