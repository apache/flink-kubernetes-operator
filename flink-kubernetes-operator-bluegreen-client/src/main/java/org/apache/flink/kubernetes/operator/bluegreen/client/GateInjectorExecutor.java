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

package org.apache.flink.kubernetes.operator.bluegreen.client;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.dag.Pipeline;
import org.apache.flink.api.java.typeutils.GenericTypeInfo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.core.execution.PipelineExecutor;
import org.apache.flink.streaming.api.graph.StreamEdge;
import org.apache.flink.streaming.api.graph.StreamGraph;
import org.apache.flink.streaming.api.graph.StreamNode;
import org.apache.flink.streaming.api.operators.ProcessOperator;
import org.apache.flink.streaming.api.operators.SimpleOperatorFactory;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.util.InstantiationUtil;

import lombok.AllArgsConstructor;
import org.slf4j.Logger;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

/**
 * {@link PipelineExecutor} decorator that transparently injects a BlueGreen gate operator into the
 * {@link StreamGraph} before delegating to the wrapped executor.
 *
 * <p>Can be used programmatically (wrap your executor) or automatically via the {@code
 * flink-kubernetes-operator-bluegreen-agent}, which intercepts {@code
 * StreamExecutionEnvironment.execute(StreamGraph)} and calls {@link #injectGates} without any user
 * code changes.
 */
@AllArgsConstructor
public class GateInjectorExecutor implements PipelineExecutor {

    private final PipelineExecutor delegate;
    private final Configuration config;
    private final Logger logger;

    @Override
    public CompletableFuture<JobClient> execute(
            Pipeline pipeline, Configuration config, ClassLoader classLoader) throws Exception {

        if (pipeline instanceof StreamGraph) {
            injectGateOperators((StreamGraph) pipeline, config, classLoader);
        }
        return delegate.execute(pipeline, config, classLoader);
    }

    /**
     * Entry point for Option A (Application mode): call this from the user entry-point after
     * building the StreamGraph and before env.execute(graph).
     *
     * <pre>{@code
     * StreamGraph graph = env.getStreamGraph("My Job");
     * GateInjectorExecutor.injectGates(graph, flinkConfig);
     * env.execute(graph);
     * }</pre>
     */
    public static void injectGates(StreamGraph graph, Configuration config) {
        injectGateOperators(graph, config, Thread.currentThread().getContextClassLoader());
    }

    private static void injectGateOperators(
            StreamGraph graph, Configuration config, ClassLoader cl) {
        // These keys are injected by the BlueGreen controller when it creates the deployment.
        // On the initial deployment (no transition in progress) or in non-BlueGreen clusters
        // they will be absent — skip injection rather than crash.
        String activeDeploymentType = config.getString("bluegreen.active-deployment-type", null);
        String configMapName = config.getString("bluegreen.configmap.name", null);
        if (activeDeploymentType == null || configMapName == null) {
            System.err.println(
                    "[BlueGreen] bluegreen.active-deployment-type or bluegreen.configmap.name"
                            + " not found in config — skipping gate injection");
            return;
        }

        GateInjectionPosition position =
                GateInjectionPosition.valueOf(
                        config.getString(
                                "bluegreen.gate.injection.position",
                                GateInjectionPosition.BEFORE_SINK.name()));

        switch (position) {
            case AFTER_SOURCE:
                {
                    List<StreamNode> sources =
                            graph.getStreamNodes().stream()
                                    .filter(n -> n.getInEdges().isEmpty())
                                    .collect(Collectors.toList());

                    // Only single-source DAGs are supported. Multiple sources imply independent
                    // event-time domains; a single ConfigMap watermark W derived from one source
                    // is meaningless for another — no safe multi-source topology exists.
                    // TODO: consider supporting fan-in (N sources, 1 sink) with per-source gate
                    //       coordination once the watermark aggregation design is finalised.
                    if (sources.size() != 1) {
                        throw new IllegalStateException(
                                "bluegreen.gate.injection.position=AFTER_SOURCE requires exactly 1 source, "
                                        + "found "
                                        + sources.size()
                                        + ": "
                                        + sources.stream()
                                                .map(StreamNode::getOperatorName)
                                                .collect(Collectors.joining(", ")));
                    }

                    StreamNode source = sources.get(0);
                    List.copyOf(source.getOutEdges())
                            .forEach(
                                    edge -> {
                                        StreamNode downstream =
                                                graph.getStreamNode(edge.getTargetId());
                                        injectGate(
                                                graph,
                                                config,
                                                cl,
                                                edge,
                                                source,
                                                downstream,
                                                "BlueGreen-Gate[" + source.getOperatorName() + "]");
                                    });
                    break;
                }
            case BEFORE_SINK:
                {
                    List<StreamNode> sinks =
                            graph.getStreamNodes().stream()
                                    .filter(n -> n.getOutEdges().isEmpty())
                                    .collect(Collectors.toList());

                    // Only single-sink DAGs are supported. Fan-out (1 source → N sinks) is safe in
                    // principle — all branches share the same event-time domain and W is consistent
                    // — but distinguishing it from independent source-per-sink chains requires a
                    // full reachability traversal. For now we keep the invariant simple: exactly 1
                    // sink. For fan-out DAGs, prefer AFTER_SOURCE instead, which naturally places a
                    // single gate before all branches.
                    // TODO: consider adding reachability-based fan-out detection to lift this
                    // restriction.
                    if (sinks.size() != 1) {
                        throw new IllegalStateException(
                                "bluegreen.gate.injection.position=BEFORE_SINK requires exactly 1 sink, "
                                        + "found "
                                        + sinks.size()
                                        + ": "
                                        + sinks.stream()
                                                .map(StreamNode::getOperatorName)
                                                .collect(Collectors.joining(", "))
                                        + ". For fan-out DAGs use bluegreen.gate.injection.position=AFTER_SOURCE instead.");
                    }

                    StreamNode sink = sinks.get(0);
                    List.copyOf(sink.getInEdges())
                            .forEach(
                                    edge -> {
                                        StreamNode upstream =
                                                graph.getStreamNode(edge.getSourceId());
                                        injectGate(
                                                graph,
                                                config,
                                                cl,
                                                edge,
                                                upstream,
                                                sink,
                                                "BlueGreen-Gate[" + sink.getOperatorName() + "]");
                                    });
                    break;
                }
        }
    }

    private static void injectGate(
            StreamGraph graph,
            Configuration config,
            ClassLoader cl,
            StreamEdge edge,
            StreamNode upstream,
            StreamNode downstream,
            String gateName) {

        int gateId =
                graph.getStreamNodes().stream().mapToInt(StreamNode::getId).max().getAsInt() + 1;

        // TypeInformation recovery is the known friction point (see design notes).
        // Gate is a passthrough: we use GenericTypeInfo as a placeholder for addOperator(),
        // then immediately override with the upstream serializer directly.
        TypeInformation<Object> typeInfo = new GenericTypeInfo<>(Object.class);

        WatermarkGateProcessFunction<Object> gateFunction = buildGateFunction(config, upstream, cl);
        ProcessOperator<Object, Object> gateOperator = new ProcessOperator<>(gateFunction);

        graph.addOperator(
                gateId,
                downstream.getSlotSharingGroup(),
                null,
                SimpleOperatorFactory.of(gateOperator),
                typeInfo,
                typeInfo,
                gateName);

        // Override with the correct serializer from upstream
        graph.getStreamNode(gateId).setSerializersIn(upstream.getTypeSerializerOut());
        graph.getStreamNode(gateId).setSerializerOut(upstream.getTypeSerializerOut());

        // Gate always matches the parallelism of the operator immediately downstream of it.
        graph.setParallelism(gateId, downstream.getParallelism());
        // StreamGraph.setMaxParallelism(int,int) only applies values > 0.
        // ExecutionConfig.getMaxParallelism() returns the job-level setting
        // (set via env.setMaxParallelism()), or -1 when unset. Flink's
        // scheduler normalises -1 → 128 internally; we do the same here
        // so the gate node always gets a valid positive value.
        int configuredMaxPar = graph.getExecutionConfig().getMaxParallelism();
        graph.setMaxParallelism(gateId, configuredMaxPar > 0 ? configuredMaxPar : 128);

        // Rewire: upstream ──✕──> downstream  →  upstream ──> gate ──> downstream
        downstream.getInEdges().remove(edge);
        upstream.getOutEdges().remove(edge);
        graph.addEdge(upstream.getId(), gateId, 0);
        graph.addEdge(gateId, downstream.getId(), 0);
    }

    private static WatermarkGateProcessFunction<Object> buildGateFunction(
            Configuration config, StreamNode upstream, ClassLoader cl) {

        GateStrategy strategy =
                GateStrategy.valueOf(
                        config.getString("bluegreen.gate.strategy", GateStrategy.WATERMARK.name()));

        switch (strategy) {
            case WATERMARK:
                {
                    Map<String, String> flinkConfigMap = new HashMap<>();
                    flinkConfigMap.put(
                            "bluegreen.active-deployment-type",
                            config.getString("bluegreen.active-deployment-type", null));
                    flinkConfigMap.put(
                            "kubernetes.namespace", config.getString("kubernetes.namespace", null));
                    flinkConfigMap.put(
                            "bluegreen.configmap.name",
                            config.getString("bluegreen.configmap.name", null));
                    WatermarkExtractor<Object> extractor =
                            buildWatermarkExtractor(config, upstream, cl);
                    return WatermarkGateProcessFunction.create(flinkConfigMap, extractor);
                }
            default:
                throw new IllegalStateException("Unsupported gate strategy: " + strategy);
        }
    }

    private static WatermarkExtractor<Object> buildWatermarkExtractor(
            Configuration config, StreamNode upstream, ClassLoader cl) {

        boolean isSqlJob = upstream.getTypeSerializerOut() instanceof RowDataSerializer;

        if (isSqlJob) {
            int fieldIdx = config.getInteger("bluegreen.gate.watermark.field-index", -1);
            // fieldIdx is a captured primitive — lambda is serializable via WatermarkExtractor
            return (WatermarkExtractor<Object>)
                    record ->
                            (fieldIdx < 0 || ((RowData) record).isNullAt(fieldIdx))
                                    ? Long.MIN_VALUE
                                    : ((RowData) record).getLong(fieldIdx);
        }

        String extractorClass = config.getString("bluegreen.gate.watermark.extractor-class", null);
        if (extractorClass != null) {
            try {
                Object instance =
                        Class.forName(extractorClass, true, cl)
                                .getDeclaredConstructor()
                                .newInstance();
                // Full serialization dry-run — surfaces non-serializable fields anywhere
                // in the object graph before JobGraph submission, not at TaskManager distribution
                InstantiationUtil.serializeObject(instance);
                return (WatermarkExtractor<Object>) instance;
            } catch (IOException e) {
                throw new IllegalArgumentException(
                        extractorClass + " is not fully serializable: " + e.getMessage(), e);
            } catch (ReflectiveOperationException e) {
                throw new IllegalArgumentException(
                        "Could not instantiate extractor class: " + extractorClass, e);
            }
        }

        // No extractor configured — fall back to processing-time gating
        return (WatermarkExtractor<Object>) record -> Long.MIN_VALUE;
    }
}
