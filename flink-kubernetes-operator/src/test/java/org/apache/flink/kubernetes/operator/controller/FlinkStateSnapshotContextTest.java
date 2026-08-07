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

package org.apache.flink.kubernetes.operator.controller;

import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.kubernetes.operator.TestUtils;
import org.apache.flink.kubernetes.operator.api.AbstractFlinkResource;
import org.apache.flink.kubernetes.operator.api.FlinkDeployment;
import org.apache.flink.kubernetes.operator.api.FlinkSessionJob;
import org.apache.flink.kubernetes.operator.api.FlinkStateSnapshot;
import org.apache.flink.kubernetes.operator.api.spec.JobReference;
import org.apache.flink.kubernetes.operator.config.FlinkConfigManager;
import org.apache.flink.kubernetes.operator.reconciler.ReconciliationUtils;

import io.javaoperatorsdk.operator.api.reconciler.Context;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

/** Test class for {@link FlinkStateSnapshotContext}. */
public class FlinkStateSnapshotContextTest {

    private static final String DEPLOYMENT_SAVEPOINT_DIR = "test-savepoint-dir";
    private static final String SESSION_JOB_SAVEPOINT_DIR = "hdfs:///savepoints/per-job/";

    private final FlinkConfigManager configManager = new FlinkConfigManager(new Configuration());

    @Test
    public void testReferencedJobObserveConfigHonorsSessionJobSavepointDir() {
        var deployment = deployedSessionCluster();
        var sessionJob = TestUtils.buildSessionJob();
        sessionJob
                .getSpec()
                .getFlinkConfiguration()
                .put(CheckpointingOptions.SAVEPOINT_DIRECTORY.key(), SESSION_JOB_SAVEPOINT_DIR);

        var conf = contextFor(sessionJob, deployment, sessionJob).getReferencedJobObserveConfig();

        assertThat(conf.get(CheckpointingOptions.SAVEPOINT_DIRECTORY))
                .isEqualTo(SESSION_JOB_SAVEPOINT_DIR);
    }

    @Test
    public void testReferencedJobObserveConfigFallsBackToDeploymentSavepointDir() {
        var deployment = deployedSessionCluster();

        var conf = contextFor(deployment, deployment, null).getReferencedJobObserveConfig();

        assertThat(conf.get(CheckpointingOptions.SAVEPOINT_DIRECTORY))
                .isEqualTo(DEPLOYMENT_SAVEPOINT_DIR);
    }

    private FlinkDeployment deployedSessionCluster() {
        var deployment = TestUtils.buildSessionCluster();
        ReconciliationUtils.updateStatusForDeployedSpec(deployment, new Configuration());
        return deployment;
    }

    private FlinkStateSnapshotContext contextFor(
            AbstractFlinkResource<?, ?> referencedJob,
            FlinkDeployment deployment,
            FlinkSessionJob sessionJob) {
        var snapshot =
                TestUtils.buildFlinkStateSnapshotSavepoint(
                        false, JobReference.fromFlinkResource(referencedJob));
        return new FlinkStateSnapshotContext(
                snapshot,
                null,
                null,
                secondaryResourceContext(deployment, sessionJob),
                configManager);
    }

    private Context<FlinkStateSnapshot> secondaryResourceContext(
            FlinkDeployment deployment, FlinkSessionJob sessionJob) {
        return new TestUtils.TestingContext<>() {
            @Override
            public <T> Optional<T> getSecondaryResource(Class<T> expectedType, String eventName) {
                if (FlinkSessionJob.class.equals(expectedType)) {
                    return Optional.ofNullable(sessionJob).map(expectedType::cast);
                }
                if (FlinkDeployment.class.equals(expectedType)) {
                    return Optional.ofNullable(deployment).map(expectedType::cast);
                }
                return Optional.empty();
            }
        };
    }
}
