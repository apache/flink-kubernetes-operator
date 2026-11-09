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

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.CoreOptions;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.configuration.ResourceManagerOptions;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.kubernetes.operator.api.CrdConstants;
import org.apache.flink.kubernetes.operator.utils.ResourceConfigUtils;
import org.apache.flink.kubernetes.utils.Constants;

import io.fabric8.kubernetes.api.model.Container;
import io.fabric8.kubernetes.api.model.EnvVar;
import io.fabric8.kubernetes.api.model.PodTemplateSpec;
import io.fabric8.kubernetes.api.model.Quantity;
import io.fabric8.kubernetes.api.model.ResourceRequirements;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.math.RoundingMode;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.regex.Pattern;
import java.util.stream.Stream;

/**
 * Derives the CPU and memory configuration of a {@code FlinkMiniCluster} from its {@code
 * spec.resources}, preferring requests and falling back to limits.
 *
 * <p>Many of the thread pools in a MiniCluster are sized from the number of processors the JVM
 * sees: the RPC and REST server pools, as well as the JVM's own garbage collector and compiler
 * threads. Without a CPU limit, the JVM sees every processor of the node, so the number of threads
 * would depend on the node the pod is scheduled to rather than on the pod. So {@code
 * -XX:ActiveProcessorCount} is set from the CPU of the container, without adding a CPU limit and
 * the throttling that comes with it. That is the CPU in {@code spec.resources}, or in the main
 * container of the pod template when {@code spec.resources} has none, as the container keeps it.
 * Without a CPU, it is derived for 1 CPU, the default CPU of a {@code FlinkDeployment}'s
 * JobManager. A processor count the user gives the JVM is kept, and the native memory the heap
 * leaves room for is sized for it.
 *
 * <p>The jemalloc arenas that the Flink image's entrypoint sets up depend on the node in the same
 * way: 1 for 1 CPU, otherwise 4 per CPU of the CPU limit, or of the node without one. Each arena
 * keeps memory it has freed for a while. So {@code MALLOC_CONF} is set from the same CPU, with the
 * entrypoint's own formula. It also turns off jemalloc's per-thread caches, which hold freed memory
 * for each thread. On arm64, where the image's jemalloc uses 64KB pages, they held ~90MB in a small
 * container, more than the rest of the budget allows for. Without them a small container needs
 * ~20MB less on x86 too, and low-throughput jobs ran no slower.
 *
 * <p>A MiniCluster runs the JobManager and TaskManager in one JVM, so Flink sizes it with its local
 * execution memory model rather than the process memory model a TaskManager uses. That model
 * ignores {@code taskmanager.memory.process.size} and most of the other memory options, so nothing
 * budgets the memory outside the heap against the container limit. Of the options it does honour,
 * network memory ({@code taskmanager.memory.network.min} and {@code .max}, 64m when unset) is
 * allocated at startup, and managed memory ({@code taskmanager.memory.managed.size}, 128m when
 * unset) when it is first used, e.g. by RocksDB. No maximum heap size is set, so the JVM sizes the
 * heap from the container memory limit, or from the memory of the node when there isn't one.
 *
 * <p>So this derives managed and network memory as fractions of the container memory, clamped
 * between a floor and Flink's own defaults, and a maximum heap size ({@code -Xmx}) that leaves room
 * for them, for the other native memory of the JVM, and for short-lived spikes such as JIT
 * compilation. The heap doesn't go below a floor, even when the container is too small for the rest
 * of the budget, as a smaller heap isn't useful, unless the floor would take more than the largest
 * fraction of the container the heap is given. Like the process size of a {@code FlinkDeployment},
 * the heap comes from the memory request when there is one, so it is set with or without a memory
 * limit. The JVM would also limit direct memory, which network memory is, to the heap size, so the
 * direct memory limit ({@code -XX:MaxDirectMemorySize}) is set as Flink sets it for a TaskManager.
 *
 * <p>Memory in {@code spec.resources} is read as it is for a {@code FlinkDeployment}, with {@link
 * ResourceConfigUtils#parseMemoryQuantity}, so e.g. {@code 512m} is 512 mebibytes.
 *
 * <p>Values already in the configuration, including the operator's defaults, are left as they are,
 * as are a {@code MALLOC_CONF}, and a processor count or direct memory limit in {@code JVM_ARGS} on
 * the main container of the pod template. A heap size in {@code JVM_ARGS} is not kept: Flink's
 * scripts pass {@code JVM_ARGS} to the JVM before the JVM options, so the derived heap overrides
 * it, as the heap Flink derives for a {@code FlinkDeployment} does. A minimum or initial heap size
 * there is kept, and leaves the heap to the user, as the JVM won't start with a derived maximum
 * below it. The one exception is the deprecated {@code env.java.opts} when it is set without {@code
 * env.java.opts.all}: its value is moved to {@code env.java.opts.all} when a JVM option is added,
 * as a Flink 2 config file can't have both. When the user sets both, the config file is rejected,
 * as it is for a {@code FlinkDeployment}. A {@code MALLOC_CONF} set through {@code envFrom} can't
 * be seen, so the derived one replaces it, as an {@code env} entry takes precedence over {@code
 * envFrom}. Nor can a {@code JVM_ARGS} set that way, so the derived processor count overrides one
 * in it.
 *
 * <p>The constants favour small containers running small, low-throughput jobs, which is what a
 * MiniCluster is for. They come from measuring simple stateless and RocksDB jobs on x86 and arm64,
 * so jobs that load more classes or keep more state may need larger containers, or overrides in the
 * configuration.
 */
public final class MiniClusterResourceConfigUtils {

    private static final Logger LOG = LoggerFactory.getLogger(MiniClusterResourceConfigUtils.class);

    /** Flink's own local execution defaults, so a MiniCluster never gets more than these. */
    @VisibleForTesting static final MemorySize MAX_MANAGED_MEMORY = MemorySize.parse("128m");

    @VisibleForTesting static final MemorySize MAX_NETWORK_MEMORY = MemorySize.parse("64m");

    /** Floors for small containers, a quarter and a half of Flink's own defaults. */
    @VisibleForTesting static final MemorySize MIN_MANAGED_MEMORY = MemorySize.parse("32m");

    @VisibleForTesting static final MemorySize MIN_NETWORK_MEMORY = MemorySize.parse("32m");

    /**
     * Fractions of the container memory for managed and network memory, before they are clamped.
     * The network fraction is Flink's own {@code taskmanager.memory.network.fraction}. The managed
     * fraction is small because RocksDB grows to fill managed memory over time, while jobs without
     * RocksDB or batch operators don't use it at all, so in a small container the memory is better
     * spent on the heap.
     */
    @VisibleForTesting static final double MANAGED_MEMORY_FRACTION = 0.15;

    @VisibleForTesting static final double NETWORK_MEMORY_FRACTION = 0.1;

    /**
     * Native memory outside the heap, other than managed and network memory, reserved before the
     * heap is sized: metaspace, class space, compiled code, symbols, the garbage collector, thread
     * stacks and memory held by the allocator. Measured at about 160MB for simple jobs with one
     * processor, growing by about 4MB for each extra processor, as the JVM and Flink start more
     * threads.
     */
    @VisibleForTesting static final MemorySize NATIVE_MEMORY_RESERVE = MemorySize.parse("160m");

    @VisibleForTesting
    static final MemorySize NATIVE_MEMORY_PER_EXTRA_PROCESSOR = MemorySize.parse("4m");

    /**
     * Memory left unallocated for short-lived spikes above the steady state, such as JIT
     * compilation, which were measured at 40-50MB whatever the container size.
     */
    @VisibleForTesting static final MemorySize HEADROOM = MemorySize.parse("64m");

    /**
     * The smallest heap derived, even when the container is too small for it, as a smaller heap
     * isn't useful to a job. In a container so small that this is more than {@link
     * #MAX_HEAP_FRACTION} of it, the heap is capped at that fraction instead.
     */
    @VisibleForTesting static final MemorySize MIN_HEAP = MemorySize.parse("64m");

    /**
     * The smallest memory a MiniCluster is accepted with. Less than the smallest heap leaves no
     * room for the JVM at all, so it is more likely a mistake in the units, e.g. {@code 512k} or
     * {@code 512} for {@code 512Mi}.
     */
    public static final MemorySize MIN_MEMORY = MIN_HEAP;

    /**
     * The largest fraction of the memory derived for the heap, so large containers keep room. It
     * applies after {@link #MIN_HEAP}, so the heap never takes the whole of a tiny container.
     */
    @VisibleForTesting static final double MAX_HEAP_FRACTION = 0.75;

    /**
     * The processor count derived without a CPU in the resources, as Flink defaults a {@code
     * FlinkDeployment}'s JobManager to 1 CPU. Otherwise the JVM, Flink's thread pools and the
     * entrypoint's jemalloc arenas would be sized from every processor of the node.
     */
    @VisibleForTesting static final int DEFAULT_PROCESSOR_COUNT = 1;

    /** JVM options the MiniCluster is started with, as read by {@code bin/minicluster.sh}. */
    private static final List<ConfigOption<String>> JVM_OPTIONS =
            List.of(
                    CoreOptions.FLINK_DEFAULT_JVM_OPTIONS,
                    CoreOptions.FLINK_JVM_OPTIONS,
                    CoreOptions.FLINK_DEFAULT_JM_JVM_OPTIONS,
                    CoreOptions.FLINK_JM_JVM_OPTIONS);

    /** JVM flag that sets the number of processors the JVM uses to size its thread pools. */
    @VisibleForTesting static final String PROCESSOR_COUNT_FLAG = "-XX:ActiveProcessorCount=";

    /** JVM flag that sets the maximum heap size. */
    @VisibleForTesting static final String HEAP_SIZE_FLAG = "-Xmx";

    /** JVM flag that sets the most direct memory the JVM allocates, in bytes. */
    @VisibleForTesting static final String DIRECT_MEMORY_SIZE_FLAG = "-XX:MaxDirectMemorySize=";

    /** Environment variable that configures jemalloc, which the Flink image's entrypoint reads. */
    @VisibleForTesting static final String MALLOC_CONF_ENV = "MALLOC_CONF";

    /** Configuration key that sets {@link #MALLOC_CONF_ENV} on the MiniCluster container. */
    @VisibleForTesting
    static final String MALLOC_CONF_KEY =
            ResourceManagerOptions.CONTAINERIZED_MASTER_ENV_PREFIX + MALLOC_CONF_ENV;

    /**
     * Environment variable with JVM options, which Flink's scripts pass to the JVM before the JVM
     * options in the configuration.
     */
    @VisibleForTesting static final String JVM_ARGS_ENV = "JVM_ARGS";

    /** Configuration key that sets {@link #JVM_ARGS_ENV} on the MiniCluster container. */
    @VisibleForTesting
    static final String JVM_ARGS_KEY =
            ResourceManagerOptions.CONTAINERIZED_MASTER_ENV_PREFIX + JVM_ARGS_ENV;

    /** JVM flag that sets the maximum heap size, like {@link #HEAP_SIZE_FLAG}. */
    private static final String MAX_HEAP_SIZE_FLAG = "-XX:MaxHeapSize=";

    /**
     * JVM flags that set the initial and minimum heap sizes, which the maximum must not be below.
     */
    private static final String INITIAL_HEAP_SIZE_FLAG = "-Xms";

    private static final String XX_INITIAL_HEAP_SIZE_FLAG = "-XX:InitialHeapSize=";

    private static final String MIN_HEAP_SIZE_FLAG = "-XX:MinHeapSize=";

    /**
     * JVM flags that size the heap, directly or from the memory. If the user has set any of these,
     * the heap is theirs: a derived maximum below a minimum or initial size they set would stop the
     * JVM starting. Flags that size only the young generation are left out, as the JVM fits them to
     * the heap.
     */
    private static final Pattern HEAP_SIZE_FLAGS =
            Pattern.compile(
                    "-Xm[sx].*"
                            + "|-XX:(Initial|Min|Max)HeapSize=.*"
                            + "|-XX:(Initial|Min|Max)RAM(Percentage|Fraction)=.*"
                            + "|-XX:MaxRAM=.*");

    /**
     * JVM flags that set a minimum or initial heap size. The JVM won't start with a maximum below
     * these, so one in {@code JVM_ARGS} leaves the heap to the user too, even though the derived
     * maximum would override a heap size there.
     */
    private static final Pattern MIN_HEAP_SIZE_FLAGS =
            Pattern.compile("-Xms.*|-XX:(Initial|Min)HeapSize=.*");

    private MiniClusterResourceConfigUtils() {}

    /**
     * Sets the processor count, jemalloc settings, managed memory, network memory, direct memory
     * limit and heap size of a MiniCluster from its resource requirements, preferring the requests
     * and falling back to the limits. Values the user has already set, in the configuration or (for
     * environment variables) in the pod template, are left as they are.
     *
     * @param conf configuration to set the derived values in
     * @param resources resource requirements of the MiniCluster container
     * @param podTemplate pod template of the MiniCluster
     */
    static void applyResourceConfig(
            Configuration conf,
            @Nullable ResourceRequirements resources,
            @Nullable PodTemplateSpec podTemplate) {
        final int processorCount = getProcessorCount(resources, podTemplate);
        applyProcessorCount(conf, podTemplate, processorCount);
        // The arenas follow the CPU, as the entrypoint's would, not the JVM's processor count
        applyMallocConf(conf, podTemplate, processorCount);
        applyMemoryConfig(
                conf, resources, podTemplate, getJvmProcessorCount(conf, resources, podTemplate));
    }

    /**
     * Checks whether the memory in a MiniCluster's resource requirements fits its memory budget:
     * the managed and network memory in the configuration, the other native memory of the JVM,
     * headroom for spikes, and the heap. The heap is the maximum heap size the JVM is given, in
     * {@code JVM_ARGS} or the JVM options, raised to a larger minimum or initial size. When the
     * heap is sized some other way, e.g. as a percentage, it is the larger of a minimum or initial
     * size and the smallest useful heap, as the JVM's own rules make that hard to predict. A
     * container that doesn't fit isn't rejected, as small jobs can run in less, but it may be
     * evicted or OOM-killed, so the user should be told. The memory is the request when there is
     * one, as everything is derived from it: a container that uses more than its request is among
     * the first evicted when the node runs short of memory, even if it stays below its limit.
     *
     * @param conf configuration of the MiniCluster, after {@link #applyResourceConfig}
     * @param resources resource requirements of the MiniCluster container
     * @param podTemplate pod template of the MiniCluster
     * @return a message for the user if the memory doesn't fit the budget, otherwise empty
     */
    public static Optional<String> checkMemoryBudget(
            Configuration conf,
            @Nullable ResourceRequirements resources,
            @Nullable PodTemplateSpec podTemplate) {
        final var memory = getMemoryBytes(resources);
        if (memory.isEmpty()) {
            return Optional.empty();
        }
        // Sizes that can't be parsed are budgeted as derived, as applyResourceConfig sized the heap
        final var managedMemory =
                getMemorySize(conf, TaskManagerOptions.MANAGED_MEMORY_SIZE)
                        .orElseGet(() -> getDerivedManagedMemory(memory.get()));
        final var networkMemory =
                getNetworkMemory(conf).orElseGet(() -> getDerivedNetworkMemory(memory.get()));
        final var nativeMemoryReserve =
                getNativeMemoryReserve(getJvmProcessorCount(conf, resources, podTemplate));
        final var heapSize = getHeapSize(conf, podTemplate);
        final var heap = heapSize.orElse(MIN_HEAP);
        final var budget =
                managedMemory.add(networkMemory).add(nativeMemoryReserve).add(HEADROOM).add(heap);
        if (budget.getBytes() <= memory.get()) {
            return Optional.empty();
        }
        // A derived heap above the smallest heap always fits, so only the user's can be reduced
        final var reduceHeap = heap.compareTo(MIN_HEAP) > 0;
        return Optional.of(
                String.format(
                        "The FlinkMiniCluster may need more memory than it requests, and could be "
                                + "evicted or OOM-killed. "
                                + "Its memory of %s is less than the %s needed for managed memory "
                                + "(%s), network memory (%s), other JVM memory (%s), headroom for "
                                + "spikes (%s) and %s (%s). Increase the memory in "
                                + "spec.resources, or reduce %s (which jobs without RocksDB or "
                                + "batch operators don't use)%s in spec.flinkConfiguration.",
                        // Rounded down, as the budget is rounded up, so they never look the same
                        new MemorySize(memory.get()).getMebiBytes() + "Mi",
                        toQuantity(budget),
                        toQuantity(managedMemory),
                        toQuantity(networkMemory),
                        toQuantity(nativeMemoryReserve),
                        toQuantity(HEADROOM),
                        heapSize.isPresent() ? "the heap" : "the smallest heap",
                        toQuantity(heap),
                        TaskManagerOptions.MANAGED_MEMORY_SIZE.key(),
                        reduceHeap ? " or the heap size in the JVM options" : ""));
    }

    /**
     * The maximum heap size the JVM is given, in {@code JVM_ARGS} or the JVM options. Flink passes
     * {@code JVM_ARGS} first and then the JVM options in the order of {@link #JVM_OPTIONS}, and the
     * JVM uses the last of each flag. It raises the maximum to a larger minimum or initial size, so
     * that is the heap when there is no maximum, as long as it is larger than the smallest heap.
     */
    private static Optional<MemorySize> getHeapSize(
            Configuration conf, @Nullable PodTemplateSpec podTemplate) {
        MemorySize maxHeapSize = null;
        MemorySize initialHeapSize = null;
        MemorySize minHeapSize = null;
        for (var flag :
                (Iterable<String>)
                        Stream.concat(getJvmArgs(conf, podTemplate), getJvmOptions(conf))
                                ::iterator) {
            if (flag.startsWith(HEAP_SIZE_FLAG)) {
                maxHeapSize = parseHeapSize(flag, HEAP_SIZE_FLAG, maxHeapSize);
            } else if (flag.startsWith(MAX_HEAP_SIZE_FLAG)) {
                maxHeapSize = parseHeapSize(flag, MAX_HEAP_SIZE_FLAG, maxHeapSize);
            } else if (flag.startsWith(INITIAL_HEAP_SIZE_FLAG)) {
                // Sets both the initial and the minimum size
                initialHeapSize = parseHeapSize(flag, INITIAL_HEAP_SIZE_FLAG, initialHeapSize);
                minHeapSize = parseHeapSize(flag, INITIAL_HEAP_SIZE_FLAG, minHeapSize);
            } else if (flag.startsWith(XX_INITIAL_HEAP_SIZE_FLAG)) {
                initialHeapSize = parseHeapSize(flag, XX_INITIAL_HEAP_SIZE_FLAG, initialHeapSize);
            } else if (flag.startsWith(MIN_HEAP_SIZE_FLAG)) {
                minHeapSize = parseHeapSize(flag, MIN_HEAP_SIZE_FLAG, minHeapSize);
            }
        }
        final var floor =
                Stream.of(initialHeapSize, minHeapSize)
                        .filter(Objects::nonNull)
                        .max(MemorySize::compareTo);
        if (maxHeapSize == null) {
            return floor.filter(size -> size.compareTo(MIN_HEAP) > 0);
        }
        final var max = maxHeapSize;
        return Optional.of(floor.filter(size -> size.compareTo(max) > 0).orElse(max));
    }

    /** The size a heap flag sets, or the previous size if it can't be parsed. */
    @Nullable
    private static MemorySize parseHeapSize(
            String flag, String prefix, @Nullable MemorySize previous) {
        try {
            // The JVM's units (k, m, g and t, in either case, or bytes) parse the same way
            return MemorySize.parse(flag.substring(prefix.length()));
        } catch (IllegalArgumentException e) {
            // The JVM won't start with it, which the user will see more directly
            LOG.debug("Could not parse heap size {}", flag, e);
            return previous;
        }
    }

    /** Formats a size as a Kubernetes quantity in mebibytes, rounded up. */
    private static String toQuantity(MemorySize size) {
        return toMebiBytesRoundedUp(size) + "Mi";
    }

    private static long toMebiBytesRoundedUp(MemorySize size) {
        final long mebiByte = MemorySize.ofMebiBytes(1).getBytes();
        return (size.getBytes() + mebiByte - 1) / mebiByte;
    }

    private static int getProcessorCount(
            @Nullable ResourceRequirements resources, @Nullable PodTemplateSpec podTemplate) {
        final var cpu = getCpu(resources, podTemplate);
        if (cpu == null) {
            return DEFAULT_PROCESSOR_COUNT;
        }
        try {
            // Rounded up, as the JVM and the Flink image's entrypoint do for a CPU limit
            return Math.max(
                    1, cpu.getNumericalAmount().setScale(0, RoundingMode.CEILING).intValueExact());
        } catch (IllegalArgumentException | ArithmeticException e) {
            LOG.warn(
                    "Could not parse CPU {} to derive the MiniCluster processor count from, using {}",
                    cpu,
                    DEFAULT_PROCESSOR_COUNT,
                    e);
            return DEFAULT_PROCESSOR_COUNT;
        }
    }

    /**
     * The CPU of the container: from the resources if they name a CPU, as they then replace the pod
     * template's, or otherwise from the main container of the pod template.
     */
    @Nullable
    private static Quantity getCpu(
            @Nullable ResourceRequirements resources, @Nullable PodTemplateSpec podTemplate) {
        final var cpu = ResourceConfigUtils.getRequestOrLimit(resources, CrdConstants.CPU);
        if (cpu != null) {
            return cpu;
        }
        return getMainContainer(podTemplate)
                .map(Container::getResources)
                .map(r -> ResourceConfigUtils.getRequestOrLimit(r, CrdConstants.CPU))
                .orElse(null);
    }

    private static void applyProcessorCount(
            Configuration conf, @Nullable PodTemplateSpec podTemplate, int processors) {
        if (getProcessorCountFlag(conf, podTemplate).isEmpty()) {
            appendJvmOption(conf, PROCESSOR_COUNT_FLAG + processors);
        }
    }

    /**
     * The number of processors the JVM sizes its threads for: the processor count it is given, or
     * otherwise the CPU in the resources, including when the JVM is told to detect the count.
     */
    private static int getJvmProcessorCount(
            Configuration conf,
            @Nullable ResourceRequirements resources,
            @Nullable PodTemplateSpec podTemplate) {
        final var flag = getProcessorCountFlag(conf, podTemplate);
        if (flag.isPresent()) {
            try {
                final int processors = Integer.parseInt(flag.get());
                if (processors >= 1) {
                    return processors;
                }
            } catch (NumberFormatException e) {
                // The JVM won't start with it, which the user will see more directly
                LOG.debug("Could not parse processor count {}", flag.get(), e);
            }
        }
        return getProcessorCount(resources, podTemplate);
    }

    /**
     * The value of the last processor count flag the JVM is given. Flink's scripts pass {@code
     * JVM_ARGS} to the JVM before the JVM options, and the JVM uses the last one.
     */
    private static Optional<String> getProcessorCountFlag(
            Configuration conf, @Nullable PodTemplateSpec podTemplate) {
        return Stream.concat(getJvmArgs(conf, podTemplate), getJvmOptions(conf))
                .filter(flag -> flag.startsWith(PROCESSOR_COUNT_FLAG))
                .reduce((first, second) -> second)
                .map(flag -> flag.substring(PROCESSOR_COUNT_FLAG.length()));
    }

    /**
     * The flags in {@code JVM_ARGS}, from the configuration, or from the main container of the pod
     * template, which an environment variable in the configuration overrides.
     */
    private static Stream<String> getJvmArgs(
            Configuration conf, @Nullable PodTemplateSpec podTemplate) {
        return Optional.ofNullable(conf.getString(JVM_ARGS_KEY, null))
                .or(() -> getMainContainerEnv(podTemplate, JVM_ARGS_ENV).map(EnvVar::getValue))
                .stream()
                .flatMap(MiniClusterResourceConfigUtils::splitFlags);
    }

    private static void applyMallocConf(
            Configuration conf, @Nullable PodTemplateSpec podTemplate, int processors) {
        // An environment variable in the configuration would override one in the pod template, so
        // neither is touched if the user has set either
        if (conf.containsKey(MALLOC_CONF_KEY)
                || getMainContainerEnv(podTemplate, MALLOC_CONF_ENV).isPresent()) {
            return;
        }
        // The arenas the entrypoint would choose for a CPU limit of this many CPUs
        conf.setString(
                MALLOC_CONF_KEY,
                "narenas:" + (processors == 1 ? 1 : 4 * processors) + ",tcache:false");
    }

    /** The last environment variable with a name on the main container of the pod template. */
    private static Optional<EnvVar> getMainContainerEnv(
            @Nullable PodTemplateSpec podTemplate, String name) {
        return getMainContainer(podTemplate).map(Container::getEnv).stream()
                .flatMap(List::stream)
                .filter(env -> name.equals(env.getName()))
                .reduce((first, second) -> second);
    }

    /**
     * The main container of the pod template. When there is more than one, the pod factory keeps
     * only the last, as Flink does for a {@code FlinkDeployment}.
     */
    private static Optional<Container> getMainContainer(@Nullable PodTemplateSpec podTemplate) {
        if (podTemplate == null
                || podTemplate.getSpec() == null
                || podTemplate.getSpec().getContainers() == null) {
            return Optional.empty();
        }
        return podTemplate.getSpec().getContainers().stream()
                .filter(Objects::nonNull)
                .filter(c -> Constants.MAIN_CONTAINER_NAME.equals(c.getName()))
                .reduce((first, second) -> second);
    }

    private static void applyMemoryConfig(
            Configuration conf,
            @Nullable ResourceRequirements resources,
            @Nullable PodTemplateSpec podTemplate,
            int processorCount) {
        final var memory = getMemoryBytes(resources);
        if (memory.isEmpty()) {
            return;
        }
        final long memoryBytes = memory.get();

        final var derivedManagedMemory = getDerivedManagedMemory(memoryBytes);
        if (!conf.contains(TaskManagerOptions.MANAGED_MEMORY_SIZE)) {
            conf.set(TaskManagerOptions.MANAGED_MEMORY_SIZE, derivedManagedMemory);
        }
        final var managedMemory =
                getMemorySize(conf, TaskManagerOptions.MANAGED_MEMORY_SIZE)
                        .orElse(derivedManagedMemory);

        final var derivedNetworkMemory = getDerivedNetworkMemory(memoryBytes);
        if (!conf.contains(TaskManagerOptions.NETWORK_MEMORY_MIN)
                && !conf.contains(TaskManagerOptions.NETWORK_MEMORY_MAX)) {
            conf.set(TaskManagerOptions.NETWORK_MEMORY_MIN, derivedNetworkMemory);
            conf.set(TaskManagerOptions.NETWORK_MEMORY_MAX, derivedNetworkMemory);
        }
        final var networkMemory = getNetworkMemory(conf).orElse(derivedNetworkMemory);

        applyHeapConfig(
                conf,
                podTemplate,
                memoryBytes,
                managedMemory,
                networkMemory,
                getNativeMemoryReserve(processorCount));
        applyDirectMemoryConfig(conf, podTemplate, networkMemory);
    }

    /**
     * Sets the JVM's direct memory limit as Flink does for a TaskManager: framework off-heap, task
     * off-heap and network memory. Without one, the JVM limits direct memory to the heap size, and
     * network memory is direct memory allocated at startup, so network memory larger than a small
     * derived heap would stop the MiniCluster starting. Local execution ignores the framework
     * off-heap size, so it is always Flink's default, but the user can raise the limit with the
     * task off-heap size, as for a {@code FlinkDeployment}.
     */
    private static void applyDirectMemoryConfig(
            Configuration conf, @Nullable PodTemplateSpec podTemplate, MemorySize networkMemory) {
        // The JVM uses the last limit it is given, and Flink's scripts pass JVM_ARGS first
        if (Stream.concat(getJvmArgs(conf, podTemplate), getJvmOptions(conf))
                .anyMatch(flag -> flag.startsWith(DIRECT_MEMORY_SIZE_FLAG))) {
            return;
        }
        final var directMemory =
                TaskManagerOptions.FRAMEWORK_OFF_HEAP_MEMORY
                        .defaultValue()
                        .add(
                                getMemorySize(conf, TaskManagerOptions.TASK_OFF_HEAP_MEMORY)
                                        .orElse(MemorySize.ZERO))
                        .add(networkMemory);
        appendJvmOption(conf, DIRECT_MEMORY_SIZE_FLAG + directMemory.getBytes());
    }

    private static MemorySize getDerivedManagedMemory(long memoryBytes) {
        return clamp(
                (long) (memoryBytes * MANAGED_MEMORY_FRACTION),
                MIN_MANAGED_MEMORY,
                MAX_MANAGED_MEMORY);
    }

    private static MemorySize getDerivedNetworkMemory(long memoryBytes) {
        return clamp(
                (long) (memoryBytes * NETWORK_MEMORY_FRACTION),
                MIN_NETWORK_MEMORY,
                MAX_NETWORK_MEMORY);
    }

    private static MemorySize getNativeMemoryReserve(int processorCount) {
        return NATIVE_MEMORY_RESERVE.add(
                NATIVE_MEMORY_PER_EXTRA_PROCESSOR.multiply(processorCount - 1));
    }

    private static void applyHeapConfig(
            Configuration conf,
            @Nullable PodTemplateSpec podTemplate,
            long memoryBytes,
            MemorySize managedMemory,
            MemorySize networkMemory,
            MemorySize nativeMemoryReserve) {
        if (getJvmOptions(conf).anyMatch(HEAP_SIZE_FLAGS.asMatchPredicate())
                || getJvmArgs(conf, podTemplate).anyMatch(MIN_HEAP_SIZE_FLAGS.asMatchPredicate())) {
            return;
        }
        long heapBytes =
                memoryBytes
                        - managedMemory.getBytes()
                        - networkMemory.getBytes()
                        - nativeMemoryReserve.getBytes()
                        - HEADROOM.getBytes();
        if (heapBytes < MIN_HEAP.getBytes()) {
            LOG.debug(
                    "Container memory {} is too small for the derived memory budget, using the minimum heap {}",
                    new MemorySize(memoryBytes),
                    MIN_HEAP);
            heapBytes = MIN_HEAP.getBytes();
        }
        heapBytes = Math.min(heapBytes, (long) (memoryBytes * MAX_HEAP_FRACTION));

        // A size rather than -XX:MaxRAMPercentage, as the JVM ignores the percentage when it gives
        // less than about 128MiB, and uses half the limit or about 128MiB instead. Rounded down to
        // whole mebibytes, so the heap never takes more than the budget leaves for it, and
        // checkMemoryBudget can use it as it is.
        appendJvmOption(conf, HEAP_SIZE_FLAG + new MemorySize(heapBytes).getMebiBytes() + "m");
    }

    private static void appendJvmOption(Configuration conf, String flag) {
        // When only the deprecated env.java.opts key is set, this reads and removes it, as a Flink
        // 2 config file can't have both it and env.java.opts.all. When both are set, only
        // env.java.opts.all is read and removed, so the config file is rejected, as it is for a
        // FlinkDeployment.
        final var jvmOptions = conf.get(CoreOptions.FLINK_JVM_OPTIONS);
        conf.removeConfig(CoreOptions.FLINK_JVM_OPTIONS);
        conf.set(
                CoreOptions.FLINK_JVM_OPTIONS,
                jvmOptions == null || jvmOptions.isBlank() ? flag : jvmOptions + " " + flag);
    }

    /**
     * The JVM options, one flag at a time, in the order Flink passes them to the JVM. Flags are
     * matched as the JVM matches them, case-sensitively: Flink starts the JVM with {@code
     * -XX:+IgnoreUnrecognizedVMOptions}, so an {@code -XX} flag in the wrong case has no effect.
     */
    private static Stream<String> getJvmOptions(Configuration conf) {
        return JVM_OPTIONS.stream()
                .map(conf::get)
                .filter(Objects::nonNull)
                .flatMap(MiniClusterResourceConfigUtils::splitFlags);
    }

    private static Stream<String> splitFlags(String jvmOptions) {
        return Arrays.stream(jvmOptions.trim().split("\\s+"));
    }

    /**
     * The network memory the user set for a MiniCluster. Local execution uses a single network
     * memory size: when only one of min and max is set, it is used for both.
     */
    private static Optional<MemorySize> getNetworkMemory(Configuration conf) {
        return getMemorySize(conf, TaskManagerOptions.NETWORK_MEMORY_MAX)
                .or(() -> getMemorySize(conf, TaskManagerOptions.NETWORK_MEMORY_MIN));
    }

    /**
     * The memory size the user set for an option, or empty if it isn't set or can't be parsed. A
     * value that can't be parsed is left in the configuration, as for a {@code FlinkDeployment}, so
     * the MiniCluster fails to start with Flink's error naming it.
     */
    private static Optional<MemorySize> getMemorySize(
            Configuration conf, ConfigOption<MemorySize> option) {
        try {
            return conf.getOptional(option);
        } catch (IllegalArgumentException e) {
            LOG.debug("Could not parse {} to derive the MiniCluster memory from", option.key(), e);
            return Optional.empty();
        }
    }

    /** The memory everything is derived from: the request, or the limit without one. */
    private static Optional<Long> getMemoryBytes(@Nullable ResourceRequirements resources) {
        final var quantity = ResourceConfigUtils.getRequestOrLimit(resources, CrdConstants.MEMORY);
        if (quantity == null) {
            return Optional.empty();
        }
        try {
            return Optional.of(ResourceConfigUtils.parseMemoryQuantity(quantity).getBytes());
        } catch (IllegalArgumentException e) {
            LOG.warn(
                    "Could not parse memory {} to derive the MiniCluster memory from", quantity, e);
            return Optional.empty();
        }
    }

    private static MemorySize clamp(long bytes, MemorySize min, MemorySize max) {
        if (bytes < min.getBytes()) {
            return min;
        }
        if (bytes > max.getBytes()) {
            return max;
        }
        return new MemorySize(bytes);
    }
}
