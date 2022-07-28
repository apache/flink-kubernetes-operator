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

package org.apache.flink.kubernetes.operator.reconciler.diff;

import org.apache.flink.configuration.CoreOptions;
import org.apache.flink.kubernetes.operator.TestUtils;
import org.apache.flink.kubernetes.operator.config.KubernetesOperatorConfigOptions;
import org.apache.flink.kubernetes.operator.crd.spec.FlinkDeploymentSpec;
import org.apache.flink.kubernetes.operator.crd.spec.FlinkSessionJobSpec;
import org.apache.flink.kubernetes.operator.crd.spec.FlinkVersion;
import org.apache.flink.kubernetes.operator.crd.spec.IngressSpec;
import org.apache.flink.kubernetes.operator.crd.spec.JobState;
import org.apache.flink.kubernetes.operator.crd.spec.KubernetesDeploymentMode;
import org.apache.flink.kubernetes.operator.crd.spec.UpgradeMode;
import org.apache.flink.kubernetes.operator.reconciler.ReconciliationUtils;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.apache.flink.kubernetes.operator.config.KubernetesOperatorConfigOptions.OPERATOR_RECONCILE_INTERVAL;
import static org.apache.flink.kubernetes.operator.metrics.KubernetesOperatorMetricOptions.SCOPE_NAMING_KUBERNETES_OPERATOR;
import static org.junit.jupiter.api.Assertions.assertEquals;

/** Spec diff test. */
public class SpecDiffTest {

    @Test
    public void testFlinkDeploymentSpecChanges() {
        var left = new FlinkDeploymentSpec();
        var right = ReconciliationUtils.clone(left);
        var diff = left.diff(right);
        assertEquals(DiffType.IGNORE, diff.getType());
        assertEquals(0, diff.getNumDiffs());

        left = TestUtils.buildSessionCluster().getSpec();
        right = ReconciliationUtils.clone(left);
        diff = left.diff(right);
        assertEquals(DiffType.IGNORE, diff.getType());
        assertEquals(0, diff.getNumDiffs());

        left = TestUtils.buildApplicationCluster().getSpec();
        left.setPodTemplate(TestUtils.getTestPod("localhost", "v1", List.of()));
        left.setIngress(IngressSpec.builder().template("template").build());

        right = ReconciliationUtils.clone(left);
        diff = left.diff(right);
        assertEquals(DiffType.IGNORE, diff.getType());
        assertEquals(0, diff.getNumDiffs());

        assertEquals(0, diff.getNumDiffs());
        right.getJob().setUpgradeMode(UpgradeMode.LAST_STATE);
        right.getJob().setAllowNonRestoredState(true);
        right.getJob().setInitialSavepointPath("local:///tmp");
        right.getJob().setSavepointTriggerNonce(123L);
        right.getFlinkConfiguration().put(OPERATOR_RECONCILE_INTERVAL.key(), "100 SECONDS");
        right.getFlinkConfiguration().put(SCOPE_NAMING_KUBERNETES_OPERATOR.key(), "foo.bar");
        right.getFlinkConfiguration().put(CoreOptions.DEFAULT_PARALLELISM.key(), "100");

        diff = left.diff(right);
        assertEquals(DiffType.IGNORE, diff.getType());
        assertEquals(7, diff.getNumDiffs());

        right.getFlinkConfiguration().remove(SCOPE_NAMING_KUBERNETES_OPERATOR.key());

        diff = left.diff(right);
        assertEquals(DiffType.IGNORE, diff.getType());
        assertEquals(6, diff.getNumDiffs());

        right.getJob().setParallelism(100);

        diff = left.diff(right);
        assertEquals(DiffType.SCALE, diff.getType());
        assertEquals(7, diff.getNumDiffs());

        right.setImage("flink:greatest");
        right.setImagePullPolicy("never:pull");
        right.setServiceAccount("anonymous");
        right.setFlinkVersion(FlinkVersion.v1_13);
        right.setMode(KubernetesDeploymentMode.STANDALONE);
        right.setLogConfiguration(Map.of("foo", "bar"));

        diff = left.diff(right);
        assertEquals(DiffType.UPGRADE, diff.getType());
        assertEquals(13, diff.getNumDiffs());

        right.getJobManager().getResource().setMemory("999m");
        right.getTaskManager().setReplicas(999);
        right.getPodTemplate().setApiVersion("v2");
        right.getIngress().setTemplate("none");

        diff = left.diff(right);
        assertEquals(DiffType.UPGRADE, diff.getType());
        assertEquals(17, diff.getNumDiffs());

        right.getJob().setJarURI("missing.jar");
        right.getJob().setEntryClass("missing.Class");
        right.getJob().setArgs(new String[] {"foo", "bar"});
        right.getJob().setState(JobState.SUSPENDED);

        diff = left.diff(right);
        assertEquals(DiffType.UPGRADE, diff.getType());
        assertEquals(21, diff.getNumDiffs());

        right.getFlinkConfiguration().put(CoreOptions.FLINK_TM_JVM_OPTIONS.key(), "-Dfoo=bar");

        diff = left.diff(right);
        assertEquals(DiffType.UPGRADE, diff.getType());
        assertEquals(22, diff.getNumDiffs());
    }

    @Test
    public void testFlinkSessionJobSpecChanges() {
        var left = new FlinkSessionJobSpec();
        var right = ReconciliationUtils.clone(left);
        var diff = left.diff(right);
        assertEquals(DiffType.IGNORE, diff.getType());
        assertEquals(0, diff.getNumDiffs());

        left = TestUtils.buildSessionJob().getSpec();
        right = ReconciliationUtils.clone(left);
        diff = left.diff(right);
        assertEquals(DiffType.IGNORE, diff.getType());
        assertEquals(0, diff.getNumDiffs());

        assertEquals(0, diff.getNumDiffs());
        right.getJob().setUpgradeMode(UpgradeMode.LAST_STATE);
        right.getJob().setAllowNonRestoredState(true);
        right.getJob().setInitialSavepointPath("local:///tmp");
        right.getJob().setSavepointTriggerNonce(123L);
        right.getFlinkConfiguration()
                .put(KubernetesOperatorConfigOptions.JAR_ARTIFACT_HTTP_HEADER.key(), "changed");

        diff = left.diff(right);
        assertEquals(DiffType.IGNORE, diff.getType());
        assertEquals(5, diff.getNumDiffs());

        right.getJob().setParallelism(100);

        diff = left.diff(right);
        assertEquals(DiffType.SCALE, diff.getType());
        assertEquals(6, diff.getNumDiffs());

        right.setDeploymentName("missing");
        right.getJob().setJarURI("missing.jar");
        right.getJob().setEntryClass("missing.Class");
        right.getJob().setArgs(new String[] {"foo", "bar"});
        right.getJob().setState(JobState.SUSPENDED);

        diff = left.diff(right);
        assertEquals(DiffType.UPGRADE, diff.getType());
        assertEquals(11, diff.getNumDiffs());
    }
}
