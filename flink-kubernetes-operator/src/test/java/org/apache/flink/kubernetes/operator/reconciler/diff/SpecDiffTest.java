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
import org.apache.flink.configuration.PipelineOptions;
import org.apache.flink.kubernetes.operator.TestUtils;
import org.apache.flink.kubernetes.operator.api.diff.DiffType;
import org.apache.flink.kubernetes.operator.api.spec.FlinkDeploymentSpec;
import org.apache.flink.kubernetes.operator.api.spec.FlinkSessionJobSpec;
import org.apache.flink.kubernetes.operator.api.spec.FlinkVersion;
import org.apache.flink.kubernetes.operator.api.spec.IngressSpec;
import org.apache.flink.kubernetes.operator.api.spec.JobState;
import org.apache.flink.kubernetes.operator.api.spec.KubernetesDeploymentMode;
import org.apache.flink.kubernetes.operator.api.spec.UpgradeMode;
import org.apache.flink.kubernetes.operator.api.utils.BaseTestUtils;
import org.apache.flink.kubernetes.operator.api.utils.SpecUtils;
import org.apache.flink.kubernetes.operator.config.KubernetesOperatorConfigOptions;

import io.fabric8.kubernetes.api.model.HostAlias;
import lombok.Value;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.flink.kubernetes.operator.config.KubernetesOperatorConfigOptions.OPERATOR_RECONCILE_INTERVAL;
import static org.apache.flink.kubernetes.operator.metrics.KubernetesOperatorMetricOptions.SCOPE_NAMING_KUBERNETES_OPERATOR;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Spec diff test. */
public class SpecDiffTest {

    @Test
    public void testFlinkDeploymentSpecChanges() {
        var left = new FlinkDeploymentSpec();
        var right = SpecUtils.clone(left);
        var diff =
                new ReflectiveDiffBuilder<>(KubernetesDeploymentMode.NATIVE, left, right).build();
        assertEquals(DiffType.IGNORE, diff.getType());
        assertEquals(0, diff.getNumDiffs());

        left = BaseTestUtils.buildSessionCluster().getSpec();
        right = SpecUtils.clone(left);
        diff = new ReflectiveDiffBuilder<>(KubernetesDeploymentMode.NATIVE, left, right).build();
        assertEquals(DiffType.IGNORE, diff.getType());
        assertEquals(0, diff.getNumDiffs());

        left = BaseTestUtils.buildApplicationCluster().getSpec();
        left.setPodTemplate(BaseTestUtils.getTestPod("localhost", "v1", List.of()));
        left.setIngress(IngressSpec.builder().template("template").build());

        right = SpecUtils.clone(left);
        diff = new ReflectiveDiffBuilder<>(KubernetesDeploymentMode.NATIVE, left, right).build();
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

        diff = new ReflectiveDiffBuilder<>(KubernetesDeploymentMode.NATIVE, left, right).build();
        assertEquals(DiffType.IGNORE, diff.getType());
        assertEquals(7, diff.getNumDiffs());

        right.getFlinkConfiguration().remove(SCOPE_NAMING_KUBERNETES_OPERATOR.key());

        diff = new ReflectiveDiffBuilder<>(KubernetesDeploymentMode.NATIVE, left, right).build();
        assertEquals(DiffType.IGNORE, diff.getType());
        assertEquals(6, diff.getNumDiffs());

        right.getJob().setParallelism(100);

        diff = new ReflectiveDiffBuilder<>(KubernetesDeploymentMode.NATIVE, left, right).build();
        assertEquals(DiffType.UPGRADE, diff.getType());
        assertEquals(7, diff.getNumDiffs());

        diff =
                new ReflectiveDiffBuilder<>(KubernetesDeploymentMode.STANDALONE, left, right)
                        .build();
        assertEquals(DiffType.SCALE, diff.getType());
        assertEquals(7, diff.getNumDiffs());

        right.setImage("flink:greatest");
        right.setImagePullPolicy("never:pull");
        right.setServiceAccount("anonymous");
        right.setFlinkVersion(FlinkVersion.v1_13);
        right.setMode(KubernetesDeploymentMode.STANDALONE);
        right.setLogConfiguration(Map.of("foo", "bar"));

        diff = new ReflectiveDiffBuilder<>(KubernetesDeploymentMode.NATIVE, left, right).build();
        assertEquals(DiffType.UPGRADE, diff.getType());
        assertEquals(13, diff.getNumDiffs());

        right.getJobManager().getResource().setMemory("999m");
        right.getTaskManager().setReplicas(999);
        right.getPodTemplate().setApiVersion("v2");
        right.getIngress().setTemplate("none");

        diff = new ReflectiveDiffBuilder<>(KubernetesDeploymentMode.NATIVE, left, right).build();
        assertEquals(DiffType.UPGRADE, diff.getType());
        assertEquals(17, diff.getNumDiffs());

        right.getJob().setJarURI("missing.jar");
        right.getJob().setEntryClass("missing.Class");
        right.getJob().setArgs(new String[] {"foo", "bar"});
        right.getJob().setState(JobState.SUSPENDED);

        diff = new ReflectiveDiffBuilder<>(KubernetesDeploymentMode.NATIVE, left, right).build();
        assertEquals(DiffType.UPGRADE, diff.getType());
        assertEquals(21, diff.getNumDiffs());

        right.getFlinkConfiguration().put(CoreOptions.FLINK_TM_JVM_OPTIONS.key(), "-Dfoo=bar");

        diff = new ReflectiveDiffBuilder<>(KubernetesDeploymentMode.NATIVE, left, right).build();
        assertEquals(DiffType.UPGRADE, diff.getType());
        assertEquals(22, diff.getNumDiffs());
        left.setMode(KubernetesDeploymentMode.STANDALONE);
        left.getTaskManager().setReplicas(2);
        left.getTaskManager().getResource().setMemory("1024");
        right = SpecUtils.clone(left);
        right.getTaskManager().setReplicas(3);
        diff = new ReflectiveDiffBuilder<>(KubernetesDeploymentMode.NATIVE, left, right).build();
        assertEquals(DiffType.UPGRADE, diff.getType());
        diff =
                new ReflectiveDiffBuilder<>(KubernetesDeploymentMode.STANDALONE, left, right)
                        .build();
        assertEquals(DiffType.SCALE, diff.getType());
        assertEquals(1, diff.getNumDiffs());
        right.getTaskManager().getResource().setMemory("2048");
        right.getTaskManager().setReplicas(4);
        diff = new ReflectiveDiffBuilder<>(KubernetesDeploymentMode.NATIVE, left, right).build();
        assertEquals(DiffType.UPGRADE, diff.getType());
        assertEquals(2, diff.getNumDiffs());

        // verify parallelism override handling for native/standalone
        left = TestUtils.buildApplicationCluster().getSpec();
        right = TestUtils.buildApplicationCluster().getSpec();
        left.getFlinkConfiguration().put(PipelineOptions.PARALLELISM_OVERRIDES.key(), "new");

        diff = new ReflectiveDiffBuilder<>(KubernetesDeploymentMode.NATIVE, left, right).build();
        assertEquals(DiffType.SCALE, diff.getType());
        assertEquals(1, diff.getNumDiffs());

        diff =
                new ReflectiveDiffBuilder<>(KubernetesDeploymentMode.STANDALONE, left, right)
                        .build();
        assertEquals(DiffType.UPGRADE, diff.getType());
        assertEquals(1, diff.getNumDiffs());
    }

    @Test
    public void testFlinkSessionJobSpecChanges() {
        var left = new FlinkSessionJobSpec();
        var right = SpecUtils.clone(left);
        var diff =
                new ReflectiveDiffBuilder<>(KubernetesDeploymentMode.NATIVE, left, right).build();
        assertEquals(DiffType.IGNORE, diff.getType());
        assertEquals(0, diff.getNumDiffs());

        left = BaseTestUtils.buildSessionJob().getSpec();
        right = SpecUtils.clone(left);
        diff = new ReflectiveDiffBuilder<>(KubernetesDeploymentMode.NATIVE, left, right).build();
        assertEquals(DiffType.IGNORE, diff.getType());
        assertEquals(0, diff.getNumDiffs());

        assertEquals(0, diff.getNumDiffs());
        right.getJob().setUpgradeMode(UpgradeMode.LAST_STATE);
        right.getJob().setAllowNonRestoredState(true);
        right.getJob().setInitialSavepointPath("local:///tmp");
        right.getJob().setSavepointTriggerNonce(123L);
        right.getFlinkConfiguration()
                .put(KubernetesOperatorConfigOptions.JAR_ARTIFACT_HTTP_HEADER.key(), "changed");

        diff = new ReflectiveDiffBuilder<>(KubernetesDeploymentMode.NATIVE, left, right).build();
        assertEquals(DiffType.IGNORE, diff.getType());
        assertEquals(5, diff.getNumDiffs());

        right.getJob().setParallelism(100);

        diff = new ReflectiveDiffBuilder<>(KubernetesDeploymentMode.NATIVE, left, right).build();
        assertEquals(DiffType.UPGRADE, diff.getType());
        assertEquals(6, diff.getNumDiffs());

        diff =
                new ReflectiveDiffBuilder<>(KubernetesDeploymentMode.STANDALONE, left, right)
                        .build();
        assertEquals(DiffType.SCALE, diff.getType());
        assertEquals(6, diff.getNumDiffs());

        right.setDeploymentName("missing");
        right.getJob().setJarURI("missing.jar");
        right.getJob().setEntryClass("missing.Class");
        right.getJob().setArgs(new String[] {"foo", "bar"});
        right.getJob().setState(JobState.SUSPENDED);

        diff = new ReflectiveDiffBuilder<>(KubernetesDeploymentMode.NATIVE, left, right).build();
        assertEquals(DiffType.UPGRADE, diff.getType());
        assertEquals(11, diff.getNumDiffs());
    }

    @Test
    public void testPodTemplateChanges() {
        var left = BaseTestUtils.buildApplicationCluster().getSpec();
        left.setPodTemplate(BaseTestUtils.getTestPod("localhost1", "v1", List.of()));
        left.getPodTemplate()
                .getSpec()
                .getHostAliases()
                .add(new HostAlias(List.of("host1", "host2"), "ip"));
        left.setImage("img1");
        IngressSpec ingressSpec = new IngressSpec();
        ingressSpec.setTemplate("temp");
        left.setIngress(ingressSpec);
        var right = BaseTestUtils.buildApplicationCluster().getSpec();
        right.setPodTemplate(BaseTestUtils.getTestPod("localhost2", "v2", List.of()));
        right.getPodTemplate()
                .getSpec()
                .getHostAliases()
                .add(new HostAlias(List.of("host1"), "ip"));
        right.setImage("img2");
        right.setRestartNonce(1L);

        var diff =
                new ReflectiveDiffBuilder<>(KubernetesDeploymentMode.NATIVE, left, right).build();
        assertEquals(DiffType.UPGRADE, diff.getType());
        assertEquals(
                "Diff: FlinkDeploymentSpec[image : img1 -> img2, "
                        + "ingress : {..} -> null, "
                        + "podTemplate.apiVersion : v1 -> v2, "
                        + "podTemplate.spec.hostAliases.0.hostnames.1 : host2 -> null, "
                        + "podTemplate.spec.hostname : localhost1 -> localhost2, "
                        + "restartNonce : null -> 1]",
                diff.toString());
    }

    @Test
    public void testArrayDiffs() {
        var left =
                new TestClass(
                        new boolean[] {true},
                        new byte[] {0},
                        new char[] {'a'},
                        new double[] {0.},
                        new float[] {0f},
                        new int[] {0},
                        new long[] {0L},
                        new short[] {2},
                        new Object[] {"a"});
        var right =
                new TestClass(
                        new boolean[] {true},
                        new byte[] {0},
                        new char[] {'a'},
                        new double[] {0.},
                        new float[] {0f},
                        new int[] {0},
                        new long[] {0L},
                        new short[] {2},
                        new Object[] {"a"});

        var diff =
                new ReflectiveDiffBuilder<>(KubernetesDeploymentMode.NATIVE, left, right).build();
        assertTrue(diff.getDiffList().isEmpty());

        right =
                new TestClass(
                        new boolean[] {false},
                        new byte[] {0},
                        new char[] {'a'},
                        new double[] {0.},
                        new float[] {0f},
                        new int[] {0},
                        new long[] {0L},
                        new short[] {2},
                        new Object[] {"b"});
        diff = new ReflectiveDiffBuilder<>(KubernetesDeploymentMode.NATIVE, left, right).build();
        assertEquals(2, diff.getNumDiffs());
        assertEquals(
                Map.of("f0", DiffType.UPGRADE, "f8", DiffType.UPGRADE),
                diff.getDiffList().stream()
                        .collect(Collectors.toMap(Diff::getFieldName, Diff::getType)));
    }

    @Value
    private static class TestClass {
        boolean[] f0;
        byte[] f1;
        char[] f2;
        double[] f3;
        float[] f4;
        int[] f5;
        long[] f6;
        short[] f7;
        Object[] f8;
    }
}
