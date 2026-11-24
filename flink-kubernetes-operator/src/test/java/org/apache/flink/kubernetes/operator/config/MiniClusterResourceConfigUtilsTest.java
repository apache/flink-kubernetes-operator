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

package org.apache.flink.kubernetes.operator.config;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.CoreOptions;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.kubernetes.utils.Constants;

import io.fabric8.kubernetes.api.model.Container;
import io.fabric8.kubernetes.api.model.PodTemplateSpec;
import io.fabric8.kubernetes.api.model.PodTemplateSpecBuilder;
import io.fabric8.kubernetes.api.model.Quantity;
import io.fabric8.kubernetes.api.model.ResourceRequirements;
import io.fabric8.kubernetes.api.model.ResourceRequirementsBuilder;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

import static org.apache.flink.kubernetes.operator.config.MiniClusterResourceConfigUtils.DEFAULT_PROCESSOR_COUNT;
import static org.apache.flink.kubernetes.operator.config.MiniClusterResourceConfigUtils.DIRECT_MEMORY_SIZE_FLAG;
import static org.apache.flink.kubernetes.operator.config.MiniClusterResourceConfigUtils.HEADROOM;
import static org.apache.flink.kubernetes.operator.config.MiniClusterResourceConfigUtils.HEAP_SIZE_FLAG;
import static org.apache.flink.kubernetes.operator.config.MiniClusterResourceConfigUtils.JVM_ARGS_ENV;
import static org.apache.flink.kubernetes.operator.config.MiniClusterResourceConfigUtils.JVM_ARGS_KEY;
import static org.apache.flink.kubernetes.operator.config.MiniClusterResourceConfigUtils.MALLOC_CONF_ENV;
import static org.apache.flink.kubernetes.operator.config.MiniClusterResourceConfigUtils.MALLOC_CONF_KEY;
import static org.apache.flink.kubernetes.operator.config.MiniClusterResourceConfigUtils.MANAGED_MEMORY_FRACTION;
import static org.apache.flink.kubernetes.operator.config.MiniClusterResourceConfigUtils.MAX_HEAP_FRACTION;
import static org.apache.flink.kubernetes.operator.config.MiniClusterResourceConfigUtils.MAX_MANAGED_MEMORY;
import static org.apache.flink.kubernetes.operator.config.MiniClusterResourceConfigUtils.MAX_NETWORK_MEMORY;
import static org.apache.flink.kubernetes.operator.config.MiniClusterResourceConfigUtils.MIN_HEAP;
import static org.apache.flink.kubernetes.operator.config.MiniClusterResourceConfigUtils.MIN_MANAGED_MEMORY;
import static org.apache.flink.kubernetes.operator.config.MiniClusterResourceConfigUtils.MIN_NETWORK_MEMORY;
import static org.apache.flink.kubernetes.operator.config.MiniClusterResourceConfigUtils.NATIVE_MEMORY_PER_EXTRA_PROCESSOR;
import static org.apache.flink.kubernetes.operator.config.MiniClusterResourceConfigUtils.NATIVE_MEMORY_RESERVE;
import static org.apache.flink.kubernetes.operator.config.MiniClusterResourceConfigUtils.NETWORK_MEMORY_FRACTION;
import static org.apache.flink.kubernetes.operator.config.MiniClusterResourceConfigUtils.PROCESSOR_COUNT_FLAG;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.within;

/** Tests for {@link MiniClusterResourceConfigUtils}. */
class MiniClusterResourceConfigUtilsTest {

    @Test
    void testOneProcessorDerivedWithoutResources() {
        assertThat(apply(null).toMap()).isEqualTo(oneProcessorConfig());
        assertThat(apply(new ResourceRequirements()).toMap()).isEqualTo(oneProcessorConfig());
    }

    @Test
    void testOneProcessorDerivedWithoutCpu() {
        var withoutCpu = apply(requestAndLimit("512Mi"));
        var oneCpu = apply(withCpu(requestAndLimit("512Mi"), "1"));

        assertThat(withoutCpu.toMap()).isEqualTo(oneCpu.toMap());
        assertThat(withoutCpu.getString(MALLOC_CONF_KEY, null)).isEqualTo("narenas:1,tcache:false");
        assertThat(withoutCpu.get(CoreOptions.FLINK_JVM_OPTIONS))
                .startsWith(PROCESSOR_COUNT_FLAG + DEFAULT_PROCESSOR_COUNT + " ");
    }

    @Test
    void testOnlyProcessorCountDerivedWithoutMemory() {
        var conf = apply(cpu(new Quantity("2"), null));

        assertThat(conf.toMap())
                .containsOnlyKeys(CoreOptions.FLINK_JVM_OPTIONS.key(), MALLOC_CONF_KEY)
                .containsEntry(CoreOptions.FLINK_JVM_OPTIONS.key(), PROCESSOR_COUNT_FLAG + 2);
    }

    @ParameterizedTest
    @CsvSource({"100m, 1", "0.5, 1", "1, 1", "1500m, 2", "2, 2", "3.2, 4"})
    void testProcessorCountRoundsUpCpu(String cpu, int processors) {
        var conf = apply(cpu(new Quantity(cpu), null));

        assertThat(conf.get(CoreOptions.FLINK_JVM_OPTIONS))
                .isEqualTo(PROCESSOR_COUNT_FLAG + processors);
    }

    @Test
    void testProcessorCountPrefersRequestOverLimit() {
        assertThat(
                        apply(cpu(new Quantity("1"), new Quantity("4")))
                                .get(CoreOptions.FLINK_JVM_OPTIONS))
                .isEqualTo(PROCESSOR_COUNT_FLAG + 1);
        assertThat(apply(cpu(null, new Quantity("4"))).get(CoreOptions.FLINK_JVM_OPTIONS))
                .isEqualTo(PROCESSOR_COUNT_FLAG + 4);
    }

    @Test
    void testCpuInPodTemplateIsUsedWithoutCpuInResources() {
        // The container keeps the pod template's CPU when spec.resources doesn't name one
        var conf = new Configuration();
        MiniClusterResourceConfigUtils.applyResourceConfig(
                conf,
                requestAndLimit("512Mi"),
                podTemplateWithResources(
                        Constants.MAIN_CONTAINER_NAME, cpu(null, new Quantity("4"))));

        assertThat(conf.get(CoreOptions.FLINK_JVM_OPTIONS)).startsWith(PROCESSOR_COUNT_FLAG + 4);
        assertThat(conf.getString(MALLOC_CONF_KEY, null)).isEqualTo("narenas:16,tcache:false");
    }

    @Test
    void testCpuInResourcesReplacesCpuInPodTemplate() {
        // A CPU limit in spec.resources replaces the pod template's CPU request as well
        var conf = new Configuration();
        MiniClusterResourceConfigUtils.applyResourceConfig(
                conf,
                cpu(null, new Quantity("2")),
                podTemplateWithResources(
                        Constants.MAIN_CONTAINER_NAME, cpu(new Quantity("4"), null)));

        assertThat(conf.get(CoreOptions.FLINK_JVM_OPTIONS)).isEqualTo(PROCESSOR_COUNT_FLAG + 2);
    }

    @Test
    void testCpuInLastMainContainerIsUsed() {
        var podTemplate =
                new PodTemplateSpecBuilder()
                        .withNewSpec()
                        .addNewContainer()
                        .withName(Constants.MAIN_CONTAINER_NAME)
                        .withResources(cpu(new Quantity("4"), null))
                        .endContainer()
                        .addNewContainer()
                        .withName(Constants.MAIN_CONTAINER_NAME)
                        .withResources(cpu(new Quantity("2"), null))
                        .endContainer()
                        .endSpec()
                        .build();
        var conf = new Configuration();
        MiniClusterResourceConfigUtils.applyResourceConfig(conf, null, podTemplate);

        // The pod factory keeps only the last main container
        assertThat(conf.get(CoreOptions.FLINK_JVM_OPTIONS)).isEqualTo(PROCESSOR_COUNT_FLAG + 2);
    }

    @Test
    void testEnvInEarlierMainContainerIsIgnored() {
        var podTemplate =
                new PodTemplateSpecBuilder()
                        .withNewSpec()
                        .addNewContainer()
                        .withName(Constants.MAIN_CONTAINER_NAME)
                        .addNewEnv()
                        .withName(MALLOC_CONF_ENV)
                        .withValue("narenas:2")
                        .endEnv()
                        .endContainer()
                        .addNewContainer()
                        .withName(Constants.MAIN_CONTAINER_NAME)
                        .endContainer()
                        .endSpec()
                        .build();
        var conf = new Configuration();
        MiniClusterResourceConfigUtils.applyResourceConfig(
                conf, cpu(new Quantity("1"), null), podTemplate);

        // The pod factory keeps only the last main container, which doesn't set it
        assertThat(conf.getString(MALLOC_CONF_KEY, null)).isEqualTo("narenas:1,tcache:false");
    }

    @Test
    void testCpuInOtherContainerIsIgnored() {
        var conf = new Configuration();
        MiniClusterResourceConfigUtils.applyResourceConfig(
                conf, null, podTemplateWithResources("sidecar", cpu(new Quantity("4"), null)));

        assertThat(conf.toMap()).isEqualTo(oneProcessorConfig());
    }

    @ParameterizedTest
    @ValueSource(strings = {"not-a-quantity", "5000000000"})
    void testOneProcessorDerivedFromUnusableCpu(String cpu) {
        assertThat(apply(cpu(new Quantity(cpu), null)).toMap()).isEqualTo(oneProcessorConfig());
    }

    @ParameterizedTest
    @MethodSource("jvmOptions")
    void testUserProcessorCountIsKept(ConfigOption<String> option) {
        var conf = new Configuration();
        conf.set(option, "-XX:ActiveProcessorCount=8");
        MiniClusterResourceConfigUtils.applyResourceConfig(
                conf, cpu(new Quantity("1"), null), null);

        assertThat(conf.toMap())
                .containsOnlyKeys(option.key(), MALLOC_CONF_KEY)
                .containsEntry(option.key(), "-XX:ActiveProcessorCount=8")
                // The arenas still follow the pod's CPU, which the user's flag doesn't change
                .containsEntry(MALLOC_CONF_KEY, "narenas:1,tcache:false");
    }

    @Test
    void testUserProcessorCountInPodTemplateJvmArgsIsKept() {
        var conf = new Configuration();
        MiniClusterResourceConfigUtils.applyResourceConfig(
                conf,
                cpu(new Quantity("1"), null),
                podTemplateWithEnv(
                        Constants.MAIN_CONTAINER_NAME,
                        JVM_ARGS_ENV,
                        "-Xss1m -XX:ActiveProcessorCount=8"));

        assertThat(conf.toMap())
                .containsOnlyKeys(MALLOC_CONF_KEY)
                .containsEntry(MALLOC_CONF_KEY, "narenas:1,tcache:false");
    }

    @Test
    void testUserProcessorCountInConfiguredJvmArgsIsKept() {
        var conf = new Configuration();
        conf.setString(JVM_ARGS_KEY, PROCESSOR_COUNT_FLAG + 8);
        MiniClusterResourceConfigUtils.applyResourceConfig(
                conf, cpu(new Quantity("1"), null), null);

        assertThat(conf.toMap()).containsOnlyKeys(JVM_ARGS_KEY, MALLOC_CONF_KEY);
    }

    @Test
    void testJvmArgsInOtherContainerIsIgnored() {
        var conf = new Configuration();
        MiniClusterResourceConfigUtils.applyResourceConfig(
                conf,
                cpu(new Quantity("1"), null),
                podTemplateWithEnv("sidecar", JVM_ARGS_ENV, PROCESSOR_COUNT_FLAG + 8));

        assertThat(conf.toMap()).isEqualTo(oneProcessorConfig());
    }

    @Test
    void testHeapSizeInJvmArgsIsOverridden() {
        // As for a FlinkDeployment, whose derived heap Flink's scripts also pass after JVM_ARGS
        var conf = new Configuration();
        conf.setString(JVM_ARGS_KEY, "-Xmx1g");
        MiniClusterResourceConfigUtils.applyResourceConfig(conf, requestAndLimit("512Mi"), null);

        assertThat(heapBytes(conf)).isEqualTo(heapBytes(apply(requestAndLimit("512Mi"))));
    }

    @ParameterizedTest
    @ValueSource(strings = {"-Xms512m", "-XX:InitialHeapSize=512m", "-XX:MinHeapSize=512m"})
    void testMinHeapSizeInConfiguredJvmArgsIsKept(String jvmArgs) {
        // The JVM won't start with a derived maximum below it
        var conf = new Configuration();
        conf.setString(JVM_ARGS_KEY, jvmArgs);
        MiniClusterResourceConfigUtils.applyResourceConfig(conf, requestAndLimit("512Mi"), null);

        assertThat(conf.get(CoreOptions.FLINK_JVM_OPTIONS)).doesNotContain("-Xmx");
    }

    @Test
    void testMinHeapSizeInPodTemplateJvmArgsIsKept() {
        var conf = new Configuration();
        MiniClusterResourceConfigUtils.applyResourceConfig(
                conf,
                requestAndLimit("512Mi"),
                podTemplateWithEnv(Constants.MAIN_CONTAINER_NAME, JVM_ARGS_ENV, "-Xms512m"));

        assertThat(conf.get(CoreOptions.FLINK_JVM_OPTIONS)).doesNotContain("-Xmx");
    }

    @ParameterizedTest
    @ValueSource(strings = {"-XX:InitialRAMPercentage=90", "-XX:MinRAMPercentage=90"})
    void testHeapPercentageInJvmArgsIsOverridden(String jvmArgs) {
        // The JVM fits these to the derived maximum, so they don't stop it starting
        var conf = new Configuration();
        conf.setString(JVM_ARGS_KEY, jvmArgs);
        MiniClusterResourceConfigUtils.applyResourceConfig(conf, requestAndLimit("512Mi"), null);

        assertThat(heapBytes(conf)).isEqualTo(heapBytes(apply(requestAndLimit("512Mi"))));
    }

    @ParameterizedTest
    @CsvSource({"100m, 1", "1, 1", "1500m, 8", "2, 8", "4, 16"})
    void testMallocConfFollowsCpu(String cpu, int arenas) {
        // 1 arena for 1 CPU and 4 per CPU otherwise, as the image's entrypoint does for a limit
        var conf = apply(cpu(new Quantity(cpu), null));

        assertThat(conf.getString(MALLOC_CONF_KEY, null))
                .isEqualTo("narenas:" + arenas + ",tcache:false");
    }

    @Test
    void testUserMallocConfInConfigurationIsKept() {
        var conf = new Configuration();
        conf.setString(MALLOC_CONF_KEY, "dirty_decay_ms:1000");
        MiniClusterResourceConfigUtils.applyResourceConfig(
                conf, cpu(new Quantity("2"), null), null);

        assertThat(conf.getString(MALLOC_CONF_KEY, null)).isEqualTo("dirty_decay_ms:1000");
    }

    @Test
    void testUserMallocConfInPodTemplateIsKept() {
        // An environment variable set from the configuration would override the pod template's
        var conf = new Configuration();
        MiniClusterResourceConfigUtils.applyResourceConfig(
                conf, cpu(new Quantity("2"), null), podTemplateWithEnv("flink-main-container"));

        assertThat(conf.containsKey(MALLOC_CONF_KEY)).isFalse();
    }

    @Test
    void testMallocConfInOtherContainerIsIgnored() {
        var conf = new Configuration();
        MiniClusterResourceConfigUtils.applyResourceConfig(
                conf, cpu(new Quantity("2"), null), podTemplateWithEnv("sidecar"));

        assertThat(conf.getString(MALLOC_CONF_KEY, null)).isEqualTo("narenas:8,tcache:false");
    }

    @Test
    void testPodTemplateWithoutContainers() {
        // The API server drops a null list, but a template built in code can still have one
        var podTemplate = podTemplateWithEnv(Constants.MAIN_CONTAINER_NAME);
        podTemplate.getSpec().setContainers(null);
        var conf = new Configuration();
        MiniClusterResourceConfigUtils.applyResourceConfig(
                conf, cpu(new Quantity("2"), null), podTemplate);

        assertThat(conf.getString(MALLOC_CONF_KEY, null)).isEqualTo("narenas:8,tcache:false");
    }

    @Test
    void testPodTemplateWithNullContainer() {
        var podTemplate = podTemplateWithEnv(Constants.MAIN_CONTAINER_NAME);
        var containers = new ArrayList<Container>();
        containers.add(null);
        containers.addAll(podTemplate.getSpec().getContainers());
        podTemplate.getSpec().setContainers(containers);
        var conf = new Configuration();
        MiniClusterResourceConfigUtils.applyResourceConfig(
                conf, cpu(new Quantity("2"), null), podTemplate);

        assertThat(conf.containsKey(MALLOC_CONF_KEY)).isFalse();
    }

    @Test
    void testDeprecatedJvmOptionsKeyIsReplaced() {
        // A Flink 2 config file can't have both env.java.opts and env.java.opts.all
        var conf = new Configuration();
        conf.setString("env.java.opts", "-Dlegacy=true");
        MiniClusterResourceConfigUtils.applyResourceConfig(
                conf, cpu(new Quantity("1"), null), null);

        assertThat(conf.toMap())
                .doesNotContainKey("env.java.opts")
                .containsEntry(
                        CoreOptions.FLINK_JVM_OPTIONS.key(),
                        "-Dlegacy=true " + PROCESSOR_COUNT_FLAG + 1);
    }

    @Test
    void testProcessorCountAndHeapSizeAreBothAppended() {
        var conf = new Configuration();
        conf.set(CoreOptions.FLINK_JVM_OPTIONS, "-Dfoo=bar");
        MiniClusterResourceConfigUtils.applyResourceConfig(
                conf,
                new ResourceRequirementsBuilder(requestAndLimit("64Gi"))
                        .addToRequests("cpu", new Quantity("2"))
                        .build(),
                null);

        assertThat(conf.get(CoreOptions.FLINK_JVM_OPTIONS))
                .startsWith("-Dfoo=bar " + PROCESSOR_COUNT_FLAG + "2 " + HEAP_SIZE_FLAG);
    }

    @Test
    void testNoMemoryDerivedFromUnparseableMemory() {
        assertThat(apply(requestAndLimit("not-a-quantity")).toMap())
                .isEqualTo(oneProcessorConfig());
    }

    @Test
    void testSmallContainerGetsMinimums() {
        var conf = apply(requestAndLimit("128Mi"));

        assertThat(conf.get(TaskManagerOptions.MANAGED_MEMORY_SIZE)).isEqualTo(MIN_MANAGED_MEMORY);
        assertThat(conf.get(TaskManagerOptions.NETWORK_MEMORY_MIN)).isEqualTo(MIN_NETWORK_MEMORY);
        assertThat(conf.get(TaskManagerOptions.NETWORK_MEMORY_MAX)).isEqualTo(MIN_NETWORK_MEMORY);
        // The heap is set to the minimum, although the rest of the budget doesn't fit
        assertThat(heapBytes(conf)).isEqualTo(MIN_HEAP.getBytes());
    }

    @Test
    void testTinyContainerHeapIsCappedBelowMinimum() {
        var memory = MiniClusterResourceConfigUtils.MIN_MEMORY;
        var conf = apply(requestAndLimit(memory.getMebiBytes() + "Mi"));

        assertThat(maxHeapBytes(memory)).isLessThan(MIN_HEAP.getBytes());
        assertThat(heapBytes(conf)).isEqualTo(maxHeapBytes(memory));
    }

    @Test
    void testLargeContainerGetsMaximums() {
        var conf = apply(requestAndLimit("64Gi"));

        assertThat(conf.get(TaskManagerOptions.MANAGED_MEMORY_SIZE)).isEqualTo(MAX_MANAGED_MEMORY);
        assertThat(conf.get(TaskManagerOptions.NETWORK_MEMORY_MIN)).isEqualTo(MAX_NETWORK_MEMORY);
        assertThat(conf.get(TaskManagerOptions.NETWORK_MEMORY_MAX)).isEqualTo(MAX_NETWORK_MEMORY);
        assertThat(heapBytes(conf)).isEqualTo(maxHeapBytes(MemorySize.parse("64g")));
    }

    @Test
    void testManagedAndNetworkMemoryScaleWithContainer() {
        var memory = MemorySize.parse("384m");
        var conf = apply(requestAndLimit("384Mi"));

        assertThat(conf.get(TaskManagerOptions.MANAGED_MEMORY_SIZE))
                .isEqualTo(
                        clamp(
                                memory,
                                MANAGED_MEMORY_FRACTION,
                                MIN_MANAGED_MEMORY,
                                MAX_MANAGED_MEMORY));
        assertThat(conf.get(TaskManagerOptions.NETWORK_MEMORY_MAX))
                .isEqualTo(
                        clamp(
                                memory,
                                NETWORK_MEMORY_FRACTION,
                                MIN_NETWORK_MEMORY,
                                MAX_NETWORK_MEMORY));
    }

    @Test
    void testHeapIsWhatRemainsOfTheContainer() {
        var memory = MemorySize.parse("768m");
        var conf = apply(withCpu(requestAndLimit("768Mi"), "1"));

        var remaining =
                memory.subtract(conf.get(TaskManagerOptions.MANAGED_MEMORY_SIZE))
                        .subtract(conf.get(TaskManagerOptions.NETWORK_MEMORY_MAX))
                        .subtract(NATIVE_MEMORY_RESERVE)
                        .subtract(HEADROOM);
        assertThat(heapBytes(conf)).isLessThan(maxHeapBytes(memory));
        assertThat(heapBytes(conf)).isBetween(withRounding(remaining), remaining.getBytes());
    }

    @Test
    void testNativeMemoryReserveGrowsWithProcessors() {
        var oneProcessor = heapBytes(apply(withCpu(requestAndLimit("768Mi"), "1")));
        var threeProcessors = heapBytes(apply(withCpu(requestAndLimit("768Mi"), "3")));

        var rounding = MemorySize.ofMebiBytes(1).getBytes();
        assertThat(oneProcessor - threeProcessors)
                .isCloseTo(
                        NATIVE_MEMORY_PER_EXTRA_PROCESSOR.multiply(2).getBytes(), within(rounding));
    }

    @Test
    void testNativeMemoryReserveFollowsUserProcessorCount() {
        // The JVM starts threads for the processors it is told it has, not for the pod's CPU
        var conf = new Configuration();
        conf.set(CoreOptions.FLINK_JVM_OPTIONS, PROCESSOR_COUNT_FLAG + 8);
        MiniClusterResourceConfigUtils.applyResourceConfig(conf, requestAndLimit("512Mi"), null);

        assertThat(heapBytes(conf))
                .isEqualTo(heapBytes(apply(withCpu(requestAndLimit("512Mi"), "8"))));
    }

    @Test
    void testNativeMemoryReserveFollowsJvmArgsProcessorCount() {
        var conf = new Configuration();
        MiniClusterResourceConfigUtils.applyResourceConfig(
                conf,
                requestAndLimit("512Mi"),
                podTemplateWithEnv(
                        Constants.MAIN_CONTAINER_NAME, JVM_ARGS_ENV, PROCESSOR_COUNT_FLAG + 8));

        assertThat(heapBytes(conf))
                .isEqualTo(heapBytes(apply(withCpu(requestAndLimit("512Mi"), "8"))));
    }

    @Test
    void testNativeMemoryReserveFollowsLastProcessorCount() {
        // Flink's scripts pass JVM_ARGS first, so the JVM uses the count in the JVM options
        var conf = new Configuration();
        conf.setString(JVM_ARGS_KEY, PROCESSOR_COUNT_FLAG + 8);
        conf.set(CoreOptions.FLINK_JVM_OPTIONS, PROCESSOR_COUNT_FLAG + 2);
        MiniClusterResourceConfigUtils.applyResourceConfig(conf, requestAndLimit("512Mi"), null);

        assertThat(heapBytes(conf))
                .isEqualTo(heapBytes(apply(withCpu(requestAndLimit("512Mi"), "2"))));
    }

    @Test
    void testHeapNeverShrinksAsContainerGrows() {
        long previousHeapBytes = 0;
        for (long mebibytes = 128; mebibytes <= 4096; mebibytes += 64) {
            long heapBytes = heapBytes(apply(requestAndLimit(mebibytes + "Mi")));

            assertThat(heapBytes)
                    .as("heap for a %dMi container", mebibytes)
                    .isGreaterThanOrEqualTo(previousHeapBytes);
            previousHeapBytes = heapBytes;
        }
    }

    @Test
    void testRequestPreferredOverLimit() {
        var conf =
                apply(
                        new ResourceRequirementsBuilder()
                                .addToRequests("memory", new Quantity("128Mi"))
                                .addToLimits("memory", new Quantity("64Gi"))
                                .build());

        assertThat(conf.get(TaskManagerOptions.MANAGED_MEMORY_SIZE)).isEqualTo(MIN_MANAGED_MEMORY);
        assertThat(conf.get(TaskManagerOptions.NETWORK_MEMORY_MAX)).isEqualTo(MIN_NETWORK_MEMORY);
    }

    @Test
    void testLimitUsedWithoutRequest() {
        var conf =
                apply(
                        new ResourceRequirementsBuilder()
                                .addToLimits("memory", new Quantity("64Gi"))
                                .build());

        assertThat(conf.get(TaskManagerOptions.MANAGED_MEMORY_SIZE)).isEqualTo(MAX_MANAGED_MEMORY);
        assertThat(heapBytes(conf)).isEqualTo(maxHeapBytes(MemorySize.parse("64g")));
    }

    @Test
    void testHeapSizeFromRequestWithoutLimit() {
        // Without a limit, the JVM would otherwise size the heap from the memory of the node
        var withLimit = apply(withCpu(requestAndLimit("768Mi"), "1"));
        var withoutLimit =
                apply(
                        new ResourceRequirementsBuilder()
                                .addToRequests("memory", new Quantity("768Mi"))
                                .addToRequests("cpu", new Quantity("1"))
                                .build());

        assertThat(heapBytes(withoutLimit)).isEqualTo(heapBytes(withLimit));
    }

    @Test
    void testHeapSizeFromRequestBelowLimit() {
        var requestOnly = apply(withCpu(requestAndLimit("768Mi"), "1"));
        var belowLimit =
                apply(
                        new ResourceRequirementsBuilder()
                                .addToRequests("memory", new Quantity("768Mi"))
                                .addToLimits("memory", new Quantity("2Gi"))
                                .addToRequests("cpu", new Quantity("1"))
                                .build());

        assertThat(heapBytes(belowLimit)).isEqualTo(heapBytes(requestOnly));
    }

    @Test
    void testUserManagedMemoryIsKept() {
        var conf = new Configuration();
        conf.set(TaskManagerOptions.MANAGED_MEMORY_SIZE, MemorySize.parse("1g"));
        MiniClusterResourceConfigUtils.applyResourceConfig(conf, requestAndLimit("64Gi"), null);

        assertThat(conf.get(TaskManagerOptions.MANAGED_MEMORY_SIZE))
                .isEqualTo(MemorySize.parse("1g"));
        assertThat(conf.get(TaskManagerOptions.NETWORK_MEMORY_MAX)).isEqualTo(MAX_NETWORK_MEMORY);
    }

    @Test
    void testUserNetworkMemoryIsKept() {
        var conf = new Configuration();
        conf.set(TaskManagerOptions.NETWORK_MEMORY_MIN, MemorySize.parse("16m"));
        MiniClusterResourceConfigUtils.applyResourceConfig(conf, requestAndLimit("64Gi"), null);

        assertThat(conf.get(TaskManagerOptions.NETWORK_MEMORY_MIN))
                .isEqualTo(MemorySize.parse("16m"));
        assertThat(conf.contains(TaskManagerOptions.NETWORK_MEMORY_MAX)).isFalse();
        assertThat(conf.get(TaskManagerOptions.MANAGED_MEMORY_SIZE)).isEqualTo(MAX_MANAGED_MEMORY);
    }

    @ParameterizedTest
    @ValueSource(
            strings = {
                "taskmanager.memory.managed.size",
                "taskmanager.memory.network.min",
                "taskmanager.memory.network.max"
            })
    void testUnparseableUserMemoryIsKeptAndBudgetedAsDerived(String key) {
        var conf = new Configuration();
        conf.setString(key, "lots");
        MiniClusterResourceConfigUtils.applyResourceConfig(conf, requestAndLimit("512Mi"), null);

        // Left for Flink to reject at startup with an error naming it, as for a FlinkDeployment
        assertThat(conf.getString(key, null)).isEqualTo("lots");
        var derived = apply(requestAndLimit("512Mi"));
        assertThat(heapBytes(conf)).isEqualTo(heapBytes(derived));
        assertThat(directMemoryBytes(conf)).isEqualTo(directMemoryBytes(derived));
        assertThat(
                        MiniClusterResourceConfigUtils.checkMemoryBudget(
                                conf, requestAndLimit("512Mi"), null))
                .isEqualTo(
                        MiniClusterResourceConfigUtils.checkMemoryBudget(
                                derived, requestAndLimit("512Mi"), null));
    }

    @Test
    void testUnparseableTaskOffHeapMemoryIsKeptAndLeftOutOfDirectMemoryLimit() {
        var conf = new Configuration();
        conf.setString(TaskManagerOptions.TASK_OFF_HEAP_MEMORY.key(), "lots");
        MiniClusterResourceConfigUtils.applyResourceConfig(conf, requestAndLimit("512Mi"), null);

        assertThat(conf.getString(TaskManagerOptions.TASK_OFF_HEAP_MEMORY.key(), null))
                .isEqualTo("lots");
        assertThat(directMemoryBytes(conf))
                .isEqualTo(directMemoryBytes(apply(requestAndLimit("512Mi"))));
    }

    @Test
    void testDirectMemoryLimitCoversNetworkMemoryLargerThanHeap() {
        var conf = new Configuration();
        conf.set(TaskManagerOptions.NETWORK_MEMORY_MIN, MemorySize.parse("128m"));
        conf.set(TaskManagerOptions.NETWORK_MEMORY_MAX, MemorySize.parse("128m"));
        MiniClusterResourceConfigUtils.applyResourceConfig(conf, requestAndLimit("512Mi"), null);

        // Without a limit, the JVM would limit direct memory to the heap, below the network memory
        assertThat(heapBytes(conf)).isLessThan(MemorySize.parse("128m").getBytes());
        assertThat(directMemoryBytes(conf))
                .isEqualTo(
                        TaskManagerOptions.FRAMEWORK_OFF_HEAP_MEMORY
                                .defaultValue()
                                .add(MemorySize.parse("128m"))
                                .getBytes());
    }

    @Test
    void testDirectMemoryLimitFollowsDerivedNetworkMemory() {
        var conf = apply(requestAndLimit("64Gi"));

        assertThat(directMemoryBytes(conf))
                .isEqualTo(
                        TaskManagerOptions.FRAMEWORK_OFF_HEAP_MEMORY
                                .defaultValue()
                                .add(MAX_NETWORK_MEMORY)
                                .getBytes());
    }

    @Test
    void testDirectMemoryLimitIncludesTaskOffHeapMemory() {
        var conf = new Configuration();
        conf.set(TaskManagerOptions.TASK_OFF_HEAP_MEMORY, MemorySize.parse("256m"));
        MiniClusterResourceConfigUtils.applyResourceConfig(conf, requestAndLimit("64Gi"), null);

        assertThat(directMemoryBytes(conf))
                .isEqualTo(
                        TaskManagerOptions.FRAMEWORK_OFF_HEAP_MEMORY
                                .defaultValue()
                                .add(MemorySize.parse("256m"))
                                .add(MAX_NETWORK_MEMORY)
                                .getBytes());
    }

    @Test
    void testDirectMemoryLimitSetWithUserHeapSize() {
        var conf = new Configuration();
        conf.set(CoreOptions.FLINK_JVM_OPTIONS, "-Xmx2g");
        MiniClusterResourceConfigUtils.applyResourceConfig(conf, requestAndLimit("4Gi"), null);

        assertThat(directMemoryBytes(conf))
                .isEqualTo(
                        TaskManagerOptions.FRAMEWORK_OFF_HEAP_MEMORY
                                .defaultValue()
                                .add(MAX_NETWORK_MEMORY)
                                .getBytes());
    }

    @ParameterizedTest
    @MethodSource("jvmOptions")
    void testUserDirectMemoryLimitIsKept(ConfigOption<String> option) {
        var conf = new Configuration();
        conf.set(option, DIRECT_MEMORY_SIZE_FLAG + "1g");
        MiniClusterResourceConfigUtils.applyResourceConfig(conf, requestAndLimit("512Mi"), null);

        assertThat(conf.get(option)).contains(DIRECT_MEMORY_SIZE_FLAG + "1g");
        assertThat(String.join(" ", jvmOptions().map(conf::get).toList()))
                .containsOnlyOnce(DIRECT_MEMORY_SIZE_FLAG);
    }

    @Test
    void testUserDirectMemoryLimitInPodTemplateJvmArgsIsKept() {
        var conf = new Configuration();
        MiniClusterResourceConfigUtils.applyResourceConfig(
                conf,
                requestAndLimit("512Mi"),
                podTemplateWithEnv(
                        Constants.MAIN_CONTAINER_NAME,
                        JVM_ARGS_ENV,
                        DIRECT_MEMORY_SIZE_FLAG + "1g"));

        assertThat(conf.get(CoreOptions.FLINK_JVM_OPTIONS)).doesNotContain(DIRECT_MEMORY_SIZE_FLAG);
    }

    @Test
    void testUserDirectMemoryLimitInConfiguredJvmArgsIsKept() {
        var conf = new Configuration();
        conf.setString(JVM_ARGS_KEY, DIRECT_MEMORY_SIZE_FLAG + "1g");
        MiniClusterResourceConfigUtils.applyResourceConfig(conf, requestAndLimit("512Mi"), null);

        assertThat(conf.get(CoreOptions.FLINK_JVM_OPTIONS)).doesNotContain(DIRECT_MEMORY_SIZE_FLAG);
    }

    private static Stream<Arguments> userHeapSizes() {
        return Stream.of(
                Arguments.of(CoreOptions.FLINK_JVM_OPTIONS, "-Xmx1g"),
                Arguments.of(CoreOptions.FLINK_JVM_OPTIONS, "-XX:MaxRAMPercentage=60"),
                Arguments.of(CoreOptions.FLINK_JVM_OPTIONS, "-XX:MinRAMPercentage=60"),
                Arguments.of(CoreOptions.FLINK_DEFAULT_JVM_OPTIONS, "-XX:MaxRAM=4g"),
                Arguments.of(CoreOptions.FLINK_JM_JVM_OPTIONS, "-XX:MaxHeapSize=1g"),
                Arguments.of(CoreOptions.FLINK_DEFAULT_JM_JVM_OPTIONS, "-XX:MaxRAMFraction=2"),
                Arguments.of(CoreOptions.FLINK_JVM_OPTIONS, "-XX:MinRAMFraction=2"),
                // A derived maximum below these would stop the JVM starting
                Arguments.of(CoreOptions.FLINK_JVM_OPTIONS, "-Xms512m"),
                Arguments.of(CoreOptions.FLINK_JM_JVM_OPTIONS, "-XX:InitialHeapSize=512m"),
                Arguments.of(CoreOptions.FLINK_JVM_OPTIONS, "-XX:MinHeapSize=512m"),
                Arguments.of(CoreOptions.FLINK_JVM_OPTIONS, "-XX:InitialRAMPercentage=50"),
                Arguments.of(CoreOptions.FLINK_JVM_OPTIONS, "-XX:InitialRAMFraction=2"));
    }

    @ParameterizedTest
    @ValueSource(
            strings = {
                // The JVM fits the young generation to the heap
                "-Xmn32m",
                "-XX:NewSize=32m",
                "-XX:MaxNewSize=32m",
                "-Xss512k",
                // Not a JVM flag, only a value that looks like one
                "-Dfoo=-Xmx1g",
                // The JVM ignores a flag in the wrong case, as Flink adds
                // -XX:+IgnoreUnrecognizedVMOptions
                "-XX:maxheapsize=1g"
            })
    void testOtherJvmOptionsKeepDerivedHeapSize(String jvmOption) {
        var conf = new Configuration();
        conf.set(CoreOptions.FLINK_JVM_OPTIONS, jvmOption);
        MiniClusterResourceConfigUtils.applyResourceConfig(conf, requestAndLimit("64Gi"), null);

        assertThat(heapBytes(conf)).isEqualTo(maxHeapBytes(MemorySize.parse("64g")));
    }

    @ParameterizedTest
    @MethodSource("userHeapSizes")
    void testUserHeapSizeIsKept(ConfigOption<String> option, String jvmOptions) {
        var conf = new Configuration();
        conf.set(option, jvmOptions);
        MiniClusterResourceConfigUtils.applyResourceConfig(conf, requestAndLimit("64Gi"), null);

        // The only JVM options are the user's, the processor count and the direct memory limit
        var derived =
                PROCESSOR_COUNT_FLAG
                        + DEFAULT_PROCESSOR_COUNT
                        + " "
                        + DIRECT_MEMORY_SIZE_FLAG
                        + TaskManagerOptions.FRAMEWORK_OFF_HEAP_MEMORY
                                .defaultValue()
                                .add(MAX_NETWORK_MEMORY)
                                .getBytes();
        if (option.equals(CoreOptions.FLINK_JVM_OPTIONS)) {
            assertThat(conf.get(option)).isEqualTo(jvmOptions + " " + derived);
        } else {
            assertThat(conf.get(option)).isEqualTo(jvmOptions);
            assertThat(conf.get(CoreOptions.FLINK_JVM_OPTIONS)).isEqualTo(derived);
        }
    }

    @Test
    void testHeapSizeIsAppendedToUserJvmOptions() {
        var conf = new Configuration();
        conf.set(CoreOptions.FLINK_JVM_OPTIONS, "-Dfoo=bar");
        MiniClusterResourceConfigUtils.applyResourceConfig(conf, requestAndLimit("64Gi"), null);

        assertThat(conf.get(CoreOptions.FLINK_JVM_OPTIONS))
                .startsWith(
                        "-Dfoo=bar "
                                + PROCESSOR_COUNT_FLAG
                                + DEFAULT_PROCESSOR_COUNT
                                + " "
                                + HEAP_SIZE_FLAG);
    }

    @Test
    void testMemoryBudgetFitsLargeEnoughContainer() {
        var resources = withCpu(requestAndLimit("512Mi"), "1");

        assertThat(checkBudget(resources)).isEmpty();
    }

    @Test
    void testMemoryBudgetDoesNotFitSmallContainer() {
        // Simple jobs ran in 320Mi, but the budget includes managed memory they don't use
        var resources = withCpu(requestAndLimit("320Mi"), "1");

        assertThat(checkBudget(resources))
                .hasValueSatisfying(
                        message ->
                                assertThat(message)
                                        .contains("memory of 320Mi")
                                        .contains("the 368Mi needed")
                                        .contains("the heap (64Mi)")
                                        .contains(TaskManagerOptions.MANAGED_MEMORY_SIZE.key())
                                        // The derived heap is already the smallest
                                        .doesNotContain("heap size in the JVM options"));
    }

    @Test
    void testMemoryBudgetMessageShowsMemoryBelowBudget() {
        // In whole mebibytes, a memory just below the budget mustn't look the same as the budget
        var memory = MemorySize.ofMebiBytes(511).add(MemorySize.parse("512k"));
        var resources = withCpu(requestAndLimit(String.valueOf(memory.getBytes())), "1");
        var conf = new Configuration();
        conf.set(TaskManagerOptions.MANAGED_MEMORY_SIZE, MemorySize.ZERO);
        conf.set(TaskManagerOptions.NETWORK_MEMORY_MIN, MIN_NETWORK_MEMORY);
        conf.set(TaskManagerOptions.NETWORK_MEMORY_MAX, MIN_NETWORK_MEMORY);
        conf.set(CoreOptions.FLINK_JVM_OPTIONS, "-Xmx256m");
        MiniClusterResourceConfigUtils.applyResourceConfig(conf, resources, null);

        // 32Mi network memory, 160Mi native memory, 64Mi headroom and the 256Mi heap
        assertThat(MiniClusterResourceConfigUtils.checkMemoryBudget(conf, resources, null))
                .hasValueSatisfying(
                        message ->
                                assertThat(message)
                                        .contains("Its memory of 511Mi is less than the 512Mi"));
    }

    @Test
    void testMemoryBudgetFitsDerivedHeapInDecimalMemory() {
        // The derived heap is rounded to whole mebibytes, which mustn't take it over the budget
        assertThat(checkBudget(withCpu(requestAndLimit("1000000000"), "1"))).isEmpty();
    }

    @ParameterizedTest
    @CsvSource({"512m, 512Mi", "512M, 512Mi", "536870912, 512Mi", "1G, 1Gi", "1g, 1Gi"})
    void testMemoryFormats(String memory, String sameMemory) {
        // Read as for a FlinkDeployment, so e.g. 512m is 512 mebibytes rather than millibytes
        var resources = withCpu(requestAndLimit(memory), "1");
        var sameResources = withCpu(requestAndLimit(sameMemory), "1");

        assertThat(apply(resources).toMap()).isEqualTo(apply(sameResources).toMap());
        assertThat(checkBudget(resources).isPresent())
                .isEqualTo(checkBudget(sameResources).isPresent());
    }

    @Test
    void testMemoryBudgetUsesUserHeapSize() {
        // A heap the container can't hold is the user's to reduce
        var resources = withCpu(requestAndLimit("512Mi"), "1");

        assertThat(checkBudget(resources, Map.of(CoreOptions.FLINK_JVM_OPTIONS, "-Xmx1g")))
                .hasValueSatisfying(
                        message ->
                                assertThat(message)
                                        .contains("the heap (1024Mi)")
                                        .contains("heap size in the JVM options"));
    }

    @Test
    void testMemoryBudgetUsesSmallUserHeapSize() {
        // Too small for the derived heap, but the user's smaller heap fits
        var resources = withCpu(requestAndLimit("352Mi"), "1");

        assertThat(checkBudget(resources)).isPresent();
        assertThat(checkBudget(resources, Map.of(CoreOptions.FLINK_JVM_OPTIONS, "-Xmx32m")))
                .isEmpty();
    }

    @Test
    void testMemoryBudgetUsesSmallestHeapForUserHeapPercentage() {
        // The JVM's rules for a percentage are hard to predict, so the smallest heap is used
        var resources = withCpu(requestAndLimit("352Mi"), "1");

        assertThat(
                        checkBudget(
                                resources,
                                Map.of(CoreOptions.FLINK_JVM_OPTIONS, "-XX:MaxRAMPercentage=10")))
                .hasValueSatisfying(
                        message ->
                                assertThat(message)
                                        .contains("the smallest heap (64Mi)")
                                        .doesNotContain("heap size in the JVM options"));
    }

    @Test
    void testMemoryBudgetUsesHeapSizeInJvmArgs() {
        // No heap is derived for a percentage, so the JVM uses the size in JVM_ARGS
        var resources = withCpu(requestAndLimit("512Mi"), "1");
        var conf = new Configuration();
        conf.set(CoreOptions.FLINK_JVM_OPTIONS, "-XX:MaxRAMPercentage=50");
        conf.setString(JVM_ARGS_KEY, "-Xmx2g");
        MiniClusterResourceConfigUtils.applyResourceConfig(conf, resources, null);

        assertThat(MiniClusterResourceConfigUtils.checkMemoryBudget(conf, resources, null))
                .hasValueSatisfying(message -> assertThat(message).contains("the heap (2048Mi)"));
    }

    @Test
    void testMemoryBudgetIgnoresHeapSizeInJvmArgsOverriddenByDerivedHeap() {
        var resources = withCpu(requestAndLimit("512Mi"), "1");
        var conf = new Configuration();
        conf.setString(JVM_ARGS_KEY, "-Xmx2g");
        MiniClusterResourceConfigUtils.applyResourceConfig(conf, resources, null);

        assertThat(MiniClusterResourceConfigUtils.checkMemoryBudget(conf, resources, null))
                .isEmpty();
    }

    @Test
    void testMemoryBudgetUsesInitialHeapSizeInPodTemplateJvmArgs() {
        // The JVM raises the maximum heap to the initial size
        var resources = withCpu(requestAndLimit("512Mi"), "1");
        var podTemplate =
                podTemplateWithEnv(Constants.MAIN_CONTAINER_NAME, JVM_ARGS_ENV, "-Xms512m");
        var conf = new Configuration();
        MiniClusterResourceConfigUtils.applyResourceConfig(conf, resources, podTemplate);

        assertThat(MiniClusterResourceConfigUtils.checkMemoryBudget(conf, resources, podTemplate))
                .hasValueSatisfying(message -> assertThat(message).contains("the heap (512Mi)"));
    }

    @ParameterizedTest
    @CsvSource({"-Xmx32m -Xms256m, 256", "-Xms32m -Xmx256m, 256", "-XX:MinHeapSize=128m, 128"})
    void testMemoryBudgetRaisesHeapToMinimumSize(String jvmOptions, int heapMebiBytes) {
        var resources = withCpu(requestAndLimit("64Mi"), "1");

        assertThat(checkBudget(resources, Map.of(CoreOptions.FLINK_JVM_OPTIONS, jvmOptions)))
                .hasValueSatisfying(
                        message ->
                                assertThat(message).contains("the heap (" + heapMebiBytes + "Mi)"));
    }

    @Test
    void testMemoryBudgetUsesSmallestHeapForSmallMinimumSize() {
        var resources = withCpu(requestAndLimit("64Mi"), "1");

        assertThat(
                        checkBudget(
                                resources,
                                Map.of(CoreOptions.FLINK_JVM_OPTIONS, "-XX:MinHeapSize=16m")))
                .hasValueSatisfying(
                        message -> assertThat(message).contains("the smallest heap (64Mi)"));
    }

    private static Stream<Arguments> userHeapSizesInOrder() {
        // The JVM uses the last heap size, in the order Flink passes the JVM options
        return Stream.of(
                Arguments.of(
                        Map.of(
                                CoreOptions.FLINK_JVM_OPTIONS, "-Xmx1g",
                                CoreOptions.FLINK_JM_JVM_OPTIONS, "-Xmx32m"),
                        true),
                Arguments.of(
                        Map.of(
                                CoreOptions.FLINK_JVM_OPTIONS, "-Xmx32m",
                                CoreOptions.FLINK_JM_JVM_OPTIONS, "-Xmx1g"),
                        false),
                Arguments.of(
                        Map.of(
                                CoreOptions.FLINK_DEFAULT_JVM_OPTIONS, "-Xmx1g",
                                CoreOptions.FLINK_DEFAULT_JM_JVM_OPTIONS, "-XX:MaxHeapSize=32M"),
                        true),
                Arguments.of(Map.of(CoreOptions.FLINK_JVM_OPTIONS, "-Xmx32m -Xmx1G"), false),
                Arguments.of(
                        Map.of(CoreOptions.FLINK_JVM_OPTIONS, "-Xmx1g -XX:MaxHeapSize=33554432"),
                        true));
    }

    @ParameterizedTest
    @MethodSource("userHeapSizesInOrder")
    void testMemoryBudgetUsesLastUserHeapSize(
            Map<ConfigOption<String>, String> jvmOptions, boolean fits) {
        var resources = withCpu(requestAndLimit("352Mi"), "1");

        assertThat(checkBudget(resources, jvmOptions).isEmpty()).isEqualTo(fits);
    }

    @Test
    void testMemoryBudgetWithoutCpuIsForOneProcessor() {
        assertThat(checkBudget(requestAndLimit("320Mi")))
                .isPresent()
                .isEqualTo(checkBudget(withCpu(requestAndLimit("320Mi"), "1")));
        assertThat(checkBudget(requestAndLimit("384Mi"))).isEmpty();
    }

    @Test
    void testMemoryBudgetUsesUserProcessorCount() {
        // Fits for one processor, but not for the threads of eight
        var resources = requestAndLimit("384Mi");

        assertThat(
                        checkBudget(
                                resources,
                                Map.of(CoreOptions.FLINK_JVM_OPTIONS, PROCESSOR_COUNT_FLAG + 8)))
                .isPresent()
                .isEqualTo(checkBudget(withCpu(resources, "8")));
    }

    @Test
    void testMemoryBudgetUsesUserManagedMemory() {
        var resources = withCpu(requestAndLimit("320Mi"), "1");
        var conf = new Configuration();
        conf.set(TaskManagerOptions.MANAGED_MEMORY_SIZE, MemorySize.ZERO);
        MiniClusterResourceConfigUtils.applyResourceConfig(conf, resources, null);

        assertThat(MiniClusterResourceConfigUtils.checkMemoryBudget(conf, resources, null))
                .isEmpty();
    }

    @Test
    void testMemoryBudgetUsesRequest() {
        var resources =
                new ResourceRequirementsBuilder()
                        .addToRequests("memory", new Quantity("256Mi"))
                        .addToLimits("memory", new Quantity("1Gi"))
                        .addToRequests("cpu", new Quantity("1"))
                        .build();

        // Not OOM-killed below the limit, but more likely to be evicted above the request
        assertThat(checkBudget(resources))
                .hasValueSatisfying(
                        message ->
                                assertThat(message)
                                        .startsWith(
                                                "The FlinkMiniCluster may need more memory than it"
                                                        + " requests, and could be evicted or"
                                                        + " OOM-killed.")
                                        .contains("Its memory of 256Mi"));
    }

    @Test
    void testMemoryBudgetWithoutLimit() {
        // The heap is sized from the request without a limit, so the budget applies the same way
        var fits =
                new ResourceRequirementsBuilder()
                        .addToRequests("memory", new Quantity("512Mi"))
                        .addToRequests("cpu", new Quantity("1"))
                        .build();
        var doesNotFit =
                new ResourceRequirementsBuilder()
                        .addToRequests("memory", new Quantity("320Mi"))
                        .addToRequests("cpu", new Quantity("1"))
                        .build();

        assertThat(checkBudget(fits)).isEmpty();
        assertThat(checkBudget(doesNotFit)).isPresent();
    }

    @Test
    void testNoMemoryBudgetWithoutMemory() {
        assertThat(checkBudget(cpu(new Quantity("1"), null))).isEmpty();
        assertThat(checkBudget(null)).isEmpty();
    }

    private static Optional<String> checkBudget(@Nullable ResourceRequirements resources) {
        return checkBudget(resources, Map.of());
    }

    private static Optional<String> checkBudget(
            @Nullable ResourceRequirements resources,
            Map<ConfigOption<String>, String> jvmOptions) {
        var conf = new Configuration();
        jvmOptions.forEach(conf::set);
        MiniClusterResourceConfigUtils.applyResourceConfig(conf, resources, null);
        return MiniClusterResourceConfigUtils.checkMemoryBudget(conf, resources, null);
    }

    private static Configuration apply(ResourceRequirements resources) {
        var conf = new Configuration();
        MiniClusterResourceConfigUtils.applyResourceConfig(conf, resources, null);
        return conf;
    }

    private static Map<String, String> oneProcessorConfig() {
        return Map.of(
                CoreOptions.FLINK_JVM_OPTIONS.key(),
                PROCESSOR_COUNT_FLAG + DEFAULT_PROCESSOR_COUNT,
                MALLOC_CONF_KEY,
                "narenas:1,tcache:false");
    }

    private static Stream<ConfigOption<String>> jvmOptions() {
        return Stream.of(
                CoreOptions.FLINK_DEFAULT_JVM_OPTIONS,
                CoreOptions.FLINK_JVM_OPTIONS,
                CoreOptions.FLINK_DEFAULT_JM_JVM_OPTIONS,
                CoreOptions.FLINK_JM_JVM_OPTIONS);
    }

    private static PodTemplateSpec podTemplateWithEnv(String containerName) {
        return podTemplateWithEnv(containerName, MALLOC_CONF_ENV, "narenas:2");
    }

    private static PodTemplateSpec podTemplateWithEnv(
            String containerName, String name, String value) {
        return new PodTemplateSpecBuilder()
                .withNewSpec()
                .addNewContainer()
                .withName(containerName)
                .addNewEnv()
                .withName(name)
                .withValue(value)
                .endEnv()
                .endContainer()
                .endSpec()
                .build();
    }

    private static PodTemplateSpec podTemplateWithResources(
            String containerName, ResourceRequirements resources) {
        return new PodTemplateSpecBuilder()
                .withNewSpec()
                .addNewContainer()
                .withName(containerName)
                .withResources(resources)
                .endContainer()
                .endSpec()
                .build();
    }

    private static ResourceRequirements cpu(@Nullable Quantity request, @Nullable Quantity limit) {
        var builder = new ResourceRequirementsBuilder();
        if (request != null) {
            builder.addToRequests("cpu", request);
        }
        if (limit != null) {
            builder.addToLimits("cpu", limit);
        }
        return builder.build();
    }

    private static ResourceRequirements requestAndLimit(String memory) {
        return new ResourceRequirementsBuilder()
                .addToRequests("memory", new Quantity(memory))
                .addToLimits("memory", new Quantity(memory))
                .build();
    }

    private static ResourceRequirements withCpu(ResourceRequirements resources, String cpu) {
        return new ResourceRequirementsBuilder(resources)
                .addToRequests("cpu", new Quantity(cpu))
                .build();
    }

    private static long heapBytes(Configuration conf) {
        var jvmOptions = conf.get(CoreOptions.FLINK_JVM_OPTIONS);
        assertThat(jvmOptions).contains(HEAP_SIZE_FLAG);
        var heapSize =
                jvmOptions.substring(
                        jvmOptions.lastIndexOf(HEAP_SIZE_FLAG) + HEAP_SIZE_FLAG.length());
        return MemorySize.parse(heapSize.split(" ")[0]).getBytes();
    }

    private static long directMemoryBytes(Configuration conf) {
        var jvmOptions = conf.get(CoreOptions.FLINK_JVM_OPTIONS);
        assertThat(jvmOptions).containsOnlyOnce(DIRECT_MEMORY_SIZE_FLAG);
        var directMemorySize =
                jvmOptions.substring(
                        jvmOptions.indexOf(DIRECT_MEMORY_SIZE_FLAG)
                                + DIRECT_MEMORY_SIZE_FLAG.length());
        return Long.parseLong(directMemorySize.split(" ")[0]);
    }

    /** The given size less the most the heap can lose from rounding down to whole mebibytes. */
    private static long withRounding(MemorySize size) {
        return size.getBytes() - MemorySize.ofMebiBytes(1).getBytes();
    }

    private static long maxHeapBytes(MemorySize memory) {
        return (long) (memory.getBytes() * MAX_HEAP_FRACTION);
    }

    private static MemorySize clamp(
            MemorySize memory, double fraction, MemorySize min, MemorySize max) {
        var bytes = (long) (memory.getBytes() * fraction);
        return new MemorySize(Math.max(min.getBytes(), Math.min(max.getBytes(), bytes)));
    }
}
