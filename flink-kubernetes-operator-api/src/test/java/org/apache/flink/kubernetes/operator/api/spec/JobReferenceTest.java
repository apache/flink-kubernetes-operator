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

package org.apache.flink.kubernetes.operator.api.spec;

import org.apache.flink.kubernetes.operator.api.utils.BaseTestUtils;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link JobReference}. */
class JobReferenceTest {

    @Test
    void testFromFlinkResource() {
        var deployment = JobReference.fromFlinkResource(BaseTestUtils.buildApplicationCluster());
        assertThat(deployment.getKind()).isEqualTo(JobKind.FLINK_DEPLOYMENT);
        assertThat(deployment.getName()).isEqualTo(BaseTestUtils.TEST_DEPLOYMENT_NAME);
        assertThat(deployment).hasToString("test-cluster (FlinkDeployment)");

        var sessionJob =
                JobReference.fromFlinkResource(
                        BaseTestUtils.buildSessionJob(
                                BaseTestUtils.TEST_SESSION_JOB_NAME, BaseTestUtils.TEST_NAMESPACE));
        assertThat(sessionJob.getKind()).isEqualTo(JobKind.FLINK_SESSION_JOB);
        assertThat(sessionJob).hasToString("test-session-job (FlinkSessionJob)");

        var miniCluster = JobReference.fromFlinkResource(BaseTestUtils.buildMiniCluster());
        assertThat(miniCluster.getKind()).isEqualTo(JobKind.FLINK_MINI_CLUSTER);
        assertThat(miniCluster.getName()).isEqualTo(BaseTestUtils.TEST_MINI_CLUSTER_NAME);
        assertThat(miniCluster).hasToString("test-minicluster (FlinkMiniCluster)");
    }

    @Test
    void testKindIsSerializedAsResourceKind() throws Exception {
        var mapper = new ObjectMapper();
        var reference = JobReference.builder().kind(JobKind.FLINK_MINI_CLUSTER).name("mc").build();

        var json = mapper.writeValueAsString(reference);

        assertThat(json).contains("\"kind\":\"FlinkMiniCluster\"");
        assertThat(mapper.readValue(json, JobReference.class)).isEqualTo(reference);
    }
}
