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

import org.apache.flink.configuration.MemorySize;
import org.apache.flink.kubernetes.operator.api.bluegreen.BlueGreenDiffType;
import org.apache.flink.kubernetes.operator.api.spec.ConfigObjectNode;
import org.apache.flink.kubernetes.operator.api.spec.FlinkBlueGreenDeploymentSpec;
import org.apache.flink.kubernetes.operator.api.spec.FlinkDeploymentSpec;
import org.apache.flink.kubernetes.operator.api.spec.FlinkDeploymentTemplateSpec;
import org.apache.flink.kubernetes.operator.api.spec.FlinkVersion;
import org.apache.flink.kubernetes.operator.api.spec.JobManagerSpec;
import org.apache.flink.kubernetes.operator.api.spec.JobSpec;
import org.apache.flink.kubernetes.operator.api.spec.JobState;
import org.apache.flink.kubernetes.operator.api.spec.KubernetesDeploymentMode;
import org.apache.flink.kubernetes.operator.api.spec.Resource;
import org.apache.flink.kubernetes.operator.api.spec.TaskManagerSpec;
import org.apache.flink.kubernetes.operator.api.spec.UpgradeMode;

import io.fabric8.kubernetes.api.model.ObjectMeta;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** Tests for FlinkBlueGreenDeploymentSpecDiff. */
public class FlinkBlueGreenDeploymentSpecDiffTest {

    private static final KubernetesDeploymentMode DEPLOYMENT_MODE = KubernetesDeploymentMode.NATIVE;

    @Test
    public void testNullValidation() {
        FlinkBlueGreenDeploymentSpec validSpec = createBasicSpec();

        // Test null left spec
        assertThrows(
                NullPointerException.class,
                () -> new FlinkBlueGreenDeploymentSpecDiff(DEPLOYMENT_MODE, null, validSpec));

        // Test null right spec
        assertThrows(
                NullPointerException.class,
                () -> new FlinkBlueGreenDeploymentSpecDiff(DEPLOYMENT_MODE, validSpec, null));

        // Test null template in left spec
        FlinkBlueGreenDeploymentSpec specWithNullTemplate = new FlinkBlueGreenDeploymentSpec();
        specWithNullTemplate.setTemplate(null);

        assertThrows(
                IllegalArgumentException.class,
                () ->
                        new FlinkBlueGreenDeploymentSpecDiff(
                                DEPLOYMENT_MODE, specWithNullTemplate, validSpec));

        // Test null template in right spec
        assertThrows(
                IllegalArgumentException.class,
                () ->
                        new FlinkBlueGreenDeploymentSpecDiff(
                                DEPLOYMENT_MODE, validSpec, specWithNullTemplate));

        // Test null nested spec in template
        FlinkBlueGreenDeploymentSpec specWithNullNestedSpec = new FlinkBlueGreenDeploymentSpec();
        FlinkDeploymentTemplateSpec templateWithNullSpec = new FlinkDeploymentTemplateSpec();
        templateWithNullSpec.setSpec(null);
        specWithNullNestedSpec.setTemplate(templateWithNullSpec);

        assertThrows(
                IllegalArgumentException.class,
                () ->
                        new FlinkBlueGreenDeploymentSpecDiff(
                                DEPLOYMENT_MODE, specWithNullNestedSpec, validSpec));
    }

    @Test
    public void testIgnoreForIdenticalSpecs() {
        FlinkBlueGreenDeploymentSpec spec1 = createBasicSpec();
        FlinkBlueGreenDeploymentSpec spec2 = createBasicSpec();

        FlinkBlueGreenDeploymentSpecDiff diff =
                new FlinkBlueGreenDeploymentSpecDiff(DEPLOYMENT_MODE, spec1, spec2);

        assertEquals(BlueGreenDiffType.IGNORE, diff.compare());
    }

    @Test
    public void testIgnoreForMetadataDifference() {
        FlinkBlueGreenDeploymentSpec spec1 = createBasicSpec();
        FlinkBlueGreenDeploymentSpec spec2 = createBasicSpec();

        // Change metadata in spec2 - should be ignored since only FlinkDeploymentSpec matters
        ObjectMeta metadata = new ObjectMeta();
        metadata.setName("different-name");
        spec2.getTemplate().setMetadata(metadata);

        FlinkBlueGreenDeploymentSpecDiff diff =
                new FlinkBlueGreenDeploymentSpecDiff(DEPLOYMENT_MODE, spec1, spec2);

        assertEquals(BlueGreenDiffType.IGNORE, diff.compare());
    }

    @Test
    public void testIgnoreForConfigurationDifference() {
        FlinkBlueGreenDeploymentSpec spec1 = createBasicSpec();
        FlinkBlueGreenDeploymentSpec spec2 = createBasicSpec();

        // Change configuration in spec2 - should be ignored since only FlinkDeploymentSpec matters
        Map<String, String> config = new HashMap<>();
        config.put("custom.config", "different-value");
        spec2.setConfiguration(config);

        FlinkBlueGreenDeploymentSpecDiff diff =
                new FlinkBlueGreenDeploymentSpecDiff(DEPLOYMENT_MODE, spec1, spec2);

        assertEquals(BlueGreenDiffType.IGNORE, diff.compare());
    }

    @Test
    public void testTransitionForNestedSpecDifference() {
        FlinkBlueGreenDeploymentSpec spec1 = createBasicSpec();
        FlinkBlueGreenDeploymentSpec spec2 = createBasicSpec();

        // Change nested spec property - setSavepointRedeployNonce triggers TRANSITION
        spec2.getTemplate().getSpec().getJob().setSavepointRedeployNonce(12345L);

        FlinkBlueGreenDeploymentSpecDiff diff =
                new FlinkBlueGreenDeploymentSpecDiff(DEPLOYMENT_MODE, spec1, spec2);

        assertEquals(BlueGreenDiffType.TRANSITION, diff.compare());
    }

    @Test
    public void testPatchChildForIgnorableNestedChanges() {
        FlinkBlueGreenDeploymentSpec spec1 = createBasicSpec();
        FlinkBlueGreenDeploymentSpec spec2 = createBasicSpec();

        // Make changes that ReflectiveDiffBuilder considers IGNORE but specs are different
        // Based on SpecDiffTest, these changes result in DiffType.IGNORE
        spec2.getTemplate().getSpec().getJob().setUpgradeMode(UpgradeMode.LAST_STATE);
        spec2.getTemplate().getSpec().getJob().setAllowNonRestoredState(true);
        spec2.getTemplate().getSpec().getJob().setInitialSavepointPath("local:///tmp");
        spec2.getTemplate().getSpec().getJob().setSavepointTriggerNonce(123L);

        FlinkBlueGreenDeploymentSpecDiff diff =
                new FlinkBlueGreenDeploymentSpecDiff(DEPLOYMENT_MODE, spec1, spec2);

        assertEquals(BlueGreenDiffType.PATCH_CHILD, diff.compare());
    }

    @Test
    public void testPatchChildForIgnorableConfigChanges() {
        FlinkBlueGreenDeploymentSpec spec1 = createBasicSpec();
        FlinkBlueGreenDeploymentSpec spec2 = createBasicSpec();

        spec2.getTemplate().getSpec().setFlinkConfiguration(new HashMap<>());

        // Add configuration changes that ReflectiveDiffBuilder considers ignorable
        spec2.getTemplate()
                .getSpec()
                .getFlinkConfiguration()
                .put("kubernetes.operator.reconcile.interval", "100 SECONDS");

        FlinkBlueGreenDeploymentSpecDiff diff =
                new FlinkBlueGreenDeploymentSpecDiff(DEPLOYMENT_MODE, spec1, spec2);

        assertEquals(BlueGreenDiffType.PATCH_CHILD, diff.compare());
    }

    @Test
    public void testPatchChildWithTopLevelAndIgnorableNestedChanges() {
        FlinkBlueGreenDeploymentSpec spec1 = createBasicSpec();
        FlinkBlueGreenDeploymentSpec spec2 = createBasicSpec();

        // Change both top-level (which should be ignored) and nested spec (ignorable changes)
        Map<String, String> config = new HashMap<>();
        config.put("custom.config", "different-value");
        spec2.setConfiguration(config);

        // Add nested changes that are ignorable by ReflectiveDiffBuilder
        spec2.getTemplate().getSpec().getJob().setUpgradeMode(UpgradeMode.SAVEPOINT);
        spec2.getTemplate().getSpec().getJob().setInitialSavepointPath("local:///tmp/savepoints");

        FlinkBlueGreenDeploymentSpecDiff diff =
                new FlinkBlueGreenDeploymentSpecDiff(DEPLOYMENT_MODE, spec1, spec2);

        // Should return PATCH_CHILD since only FlinkDeploymentSpec matters and it has ignorable
        // changes
        assertEquals(BlueGreenDiffType.PATCH_CHILD, diff.compare());
    }

    // Note: Case 2 (ReflectiveDiffBuilder returns IGNORE for different FlinkDeploymentSpecs)
    // may not occur in practice with current ReflectiveDiffBuilder implementation.
    // If FlinkDeploymentSpecs are different, ReflectiveDiffBuilder typically returns
    // a meaningful diff type (SCALE, UPGRADE, etc.) rather than IGNORE.
    // The logic is preserved in the implementation for completeness.

    @Test
    public void testTransitionForTopLevelAndNestedDifferences() {
        FlinkBlueGreenDeploymentSpec spec1 = createBasicSpec();
        FlinkBlueGreenDeploymentSpec spec2 = createBasicSpec();

        // Change both top-level (configuration) and nested spec
        // With new logic, only nested spec changes matter - setSavepointRedeployNonce triggers
        // TRANSITION
        Map<String, String> config = new HashMap<>();
        config.put("custom.config", "different-value");
        spec2.setConfiguration(config);
        spec2.getTemplate().getSpec().getJob().setSavepointRedeployNonce(12345L);

        FlinkBlueGreenDeploymentSpecDiff diff =
                new FlinkBlueGreenDeploymentSpecDiff(DEPLOYMENT_MODE, spec1, spec2);

        assertEquals(BlueGreenDiffType.TRANSITION, diff.compare());
    }

    @Test
    public void testTransitionForScaleDifference() {
        FlinkBlueGreenDeploymentSpec spec1 = createBasicSpec();
        FlinkBlueGreenDeploymentSpec spec2 = createBasicSpec();

        // Change parallelism - this should trigger SCALE in ReflectiveDiffBuilder
        spec2.getTemplate().getSpec().getJob().setParallelism(10);

        FlinkBlueGreenDeploymentSpecDiff diff =
                new FlinkBlueGreenDeploymentSpecDiff(DEPLOYMENT_MODE, spec1, spec2);

        assertEquals(BlueGreenDiffType.TRANSITION, diff.compare());
    }

    @Test
    public void testTransitionForUpgradeDifference() {
        FlinkBlueGreenDeploymentSpec spec1 = createBasicSpec();
        FlinkBlueGreenDeploymentSpec spec2 = createBasicSpec();

        // Change Flink version - this should trigger UPGRADE in ReflectiveDiffBuilder
        spec2.getTemplate().getSpec().setFlinkVersion(FlinkVersion.v1_17);

        FlinkBlueGreenDeploymentSpecDiff diff =
                new FlinkBlueGreenDeploymentSpecDiff(DEPLOYMENT_MODE, spec1, spec2);

        assertEquals(BlueGreenDiffType.TRANSITION, diff.compare());
    }

    @Test
    public void testTransitionOverridesPatchChild() {
        FlinkBlueGreenDeploymentSpec spec1 = createBasicSpec();
        FlinkBlueGreenDeploymentSpec spec2 = createBasicSpec();

        // Change both top-level and nested spec, but nested change should trigger TRANSITION
        // With new logic, only the nested FlinkDeploymentSpec change matters
        Map<String, String> config = new HashMap<>();
        config.put("custom.config", "different-value");
        spec2.setConfiguration(config);
        spec2.getTemplate().getSpec().getJob().setParallelism(10); // This triggers SCALE

        FlinkBlueGreenDeploymentSpecDiff diff =
                new FlinkBlueGreenDeploymentSpecDiff(DEPLOYMENT_MODE, spec1, spec2);

        // Should return TRANSITION since parallelism change triggers SCALE
        assertEquals(BlueGreenDiffType.TRANSITION, diff.compare());
    }

    private FlinkBlueGreenDeploymentSpec createBasicSpec() {
        // Create a basic FlinkDeploymentSpec
        FlinkDeploymentSpec deploymentSpec =
                FlinkDeploymentSpec.builder()
                        .image("flink:1.16")
                        .flinkVersion(FlinkVersion.v1_16)
                        .serviceAccount("flink")
                        .flinkConfiguration(new ConfigObjectNode())
                        .jobManager(
                                JobManagerSpec.builder()
                                        .resource(
                                                new Resource(
                                                        1.0,
                                                        MemorySize.parse("1024m").toString(),
                                                        null))
                                        .replicas(1)
                                        .build())
                        .taskManager(
                                TaskManagerSpec.builder()
                                        .resource(
                                                new Resource(
                                                        1.0,
                                                        MemorySize.parse("1024m").toString(),
                                                        null))
                                        .build())
                        .job(
                                JobSpec.builder()
                                        .jarURI(
                                                "local:///opt/flink/examples/streaming/StateMachineExample.jar")
                                        .parallelism(2)
                                        .upgradeMode(UpgradeMode.STATELESS)
                                        .state(JobState.RUNNING)
                                        .build())
                        .build();

        // Create template
        FlinkDeploymentTemplateSpec template =
                FlinkDeploymentTemplateSpec.builder().spec(deploymentSpec).build();

        // Create Blue/Green spec
        FlinkBlueGreenDeploymentSpec blueGreenSpec = new FlinkBlueGreenDeploymentSpec();
        blueGreenSpec.setTemplate(template);

        return blueGreenSpec;
    }
}
