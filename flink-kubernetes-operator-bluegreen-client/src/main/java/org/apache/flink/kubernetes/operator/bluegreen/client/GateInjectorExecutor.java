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

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.VoidSerializer;
import org.apache.flink.api.dag.Pipeline;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.api.java.typeutils.GenericTypeInfo;
import org.apache.flink.api.java.typeutils.runtime.kryo.KryoSerializer;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.core.execution.PipelineExecutor;
import org.apache.flink.streaming.api.graph.StreamEdge;
import org.apache.flink.streaming.api.graph.StreamGraph;
import org.apache.flink.streaming.api.graph.StreamNode;
import org.apache.flink.streaming.api.graph.StreamingJobGraphGenerator;
import org.apache.flink.streaming.api.operators.ChainingStrategy;
import org.apache.flink.streaming.api.operators.ProcessOperator;
import org.apache.flink.streaming.api.operators.SimpleOperatorFactory;
import org.apache.flink.streaming.runtime.operators.sink.SinkWriterOperatorFactory;
import org.apache.flink.streaming.runtime.partitioner.KeyGroupStreamPartitioner;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.AbstractRowDataSerializer;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.utils.LogicalTypeChecks;
import org.apache.flink.util.InstantiationUtil;

import lombok.AllArgsConstructor;
import org.slf4j.Logger;

import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import java.util.stream.Stream;

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

    private static final String BEFORE_UID = "bluegreen.gate.injection.before-uid";
    private static final String EXTRACTOR_CLASS = "bluegreen.gate.watermark.extractor-class";
    private static final String FIELD_INDEX = "bluegreen.gate.watermark.field-index";
    private static final String FIELD_PATH = "bluegreen.gate.watermark.field-path";

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
                    // —
                    // but distinguishing it from independent source-per-sink chains requires a full
                    // reachability traversal. For now we keep the invariant simple: exactly 1 sink.
                    // For fan-out DAGs, prefer AFTER_SOURCE instead, which naturally places a
                    // single
                    // gate before all branches.
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

                    StreamNode target = beforeSinkTarget(graph, config, sinks.get(0));
                    List.copyOf(target.getInEdges())
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
                                                target,
                                                "BlueGreen-Gate[" + target.getOperatorName() + "]");
                                    });
                    break;
                }
        }
    }

    /**
     * The operator the gate is placed in front of for BEFORE_SINK: the operator whose uid is set in
     * bluegreen.gate.injection.before-uid, else the writer of a Sink V2, else the sink itself. A
     * Sink V2 with a committer ends in its committer, whose input is committables, not records.
     */
    @VisibleForTesting
    static StreamNode beforeSinkTarget(StreamGraph graph, Configuration config, StreamNode sink) {
        String uid = config.getString(BEFORE_UID, null);
        StreamNode target;
        if (uid != null) {
            List<StreamNode> matches =
                    graph.getStreamNodes().stream()
                            .filter(n -> uid.equals(n.getTransformationUID()))
                            .collect(Collectors.toList());
            if (matches.size() != 1) {
                throw new IllegalStateException(
                        BEFORE_UID
                                + "="
                                + uid
                                + " matches "
                                + matches.size()
                                + " operators, not 1.");
            }
            target = matches.get(0);
        } else {
            target = sinkWriterOf(graph, sink);
        }

        for (StreamEdge in : target.getInEdges()) {
            if (graph.getStreamNode(in.getSourceId()).getTypeSerializerOut()
                    instanceof VoidSerializer) {
                throw new IllegalStateException(
                        "bluegreen.gate.injection.position=BEFORE_SINK would place the gate in front"
                                + " of "
                                + target.getOperatorName()
                                + ", which receives no records. Set "
                                + BEFORE_UID
                                + " to the uid of the operator that writes your records, e.g."
                                + " <uidPrefix>-writer for Iceberg's FlinkSink.");
            }
        }
        return target;
    }

    /** Walks up from a sink to its Sink V2 writer, or returns the sink if it has none. */
    private static StreamNode sinkWriterOf(StreamGraph graph, StreamNode sink) {
        StreamNode node = sink;
        while (!(node.getOperatorFactory() instanceof SinkWriterOperatorFactory)) {
            if (node.getInEdges().size() != 1) {
                return sink;
            }
            node = graph.getStreamNode(node.getInEdges().get(0).getSourceId());
        }
        return node;
    }

    private static void injectGate(
            StreamGraph graph,
            Configuration config,
            ClassLoader cl,
            StreamEdge edge,
            StreamNode upstream,
            StreamNode downstream,
            String gateName) {

        // Flink's own id allocator. max(node id) + 1 can be the id of a virtual partition or
        // side-output node, which getStreamNodes() does not list; addEdge would then reroute the
        // gate's output edge to that virtual node's upstream, leaving the gate without an output.
        int gateId = Transformation.getNewNodeId();

        // TypeInformation recovery is the known friction point (see design notes).
        // Gate is a passthrough: we use GenericTypeInfo as a placeholder for addOperator(),
        // then immediately override with the upstream serializer directly.
        TypeInformation<Object> typeInfo = new GenericTypeInfo<>(Object.class);

        WatermarkGateProcessFunction<Object> gateFunction = buildGateFunction(config, upstream, cl);
        SimpleOperatorFactory<Object> gateFactory =
                SimpleOperatorFactory.of(new ProcessOperator<>(gateFunction));
        // Set explicitly rather than inherited: Flink 1.x takes it from ProcessOperator, Flink 2.x
        // from the operator factory's default.
        gateFactory.setChainingStrategy(ChainingStrategy.ALWAYS);

        graph.addOperator(
                gateId,
                downstream.getSlotSharingGroup(),
                null,
                gateFactory,
                typeInfo,
                typeInfo,
                gateName);
        StreamNode gate = graph.getStreamNode(gateId);

        // Override with the correct serializer from upstream
        gate.setSerializersIn(upstream.getTypeSerializerOut());
        gate.setSerializerOut(upstream.getTypeSerializerOut());

        // The gate joins the downstream operator's chain, so it copies what Flink compares when
        // chaining (parallelism, max parallelism, slot sharing group) and the co-location group.
        // The max parallelism is copied as is, including unset (-1): when the gate heads the
        // chain it sets the vertex's max parallelism, which must stay what the downstream
        // operator would have had, e.g. for its key groups.
        graph.setParallelism(gateId, downstream.getParallelism());
        graph.setMaxParallelism(gateId, maxParallelismOf(downstream));
        gate.setCoLocationGroup(downstream.getCoLocationGroup());

        // Rewire: upstream ──edge──> downstream  →  upstream ──edge'──> gate ──forward──>
        // downstream
        // edge' keeps the original partitioner, side-output tag and exchange mode, so the gate
        // neither adds nor drops a shuffle (e.g. a keyBy). The forward edge keeps the downstream
        // input position, and becomes a key partitioner when an unchained gate feeds a keyed
        // operator.
        downstream.getInEdges().remove(edge);
        upstream.getOutEdges().remove(edge);
        StreamEdge gateInput =
                new StreamEdge(
                        upstream,
                        gate,
                        0,
                        edge.getBufferTimeout(),
                        edge.getPartitioner(),
                        edge.getOutputTag(),
                        edge.getExchangeMode(),
                        0,
                        edge.getIntermediateDatasetIdToProduce());
        upstream.addOutEdge(gateInput);
        gate.addInEdge(gateInput);
        graph.addEdge(gateId, downstream.getId(), edge.getTypeNumber());
        keepKeyGroupsIfNotChainable(graph, gate, downstream, edge);

        warnIfNotChainable(graph, gate, gateName);
    }

    /**
     * Replaces the gate's forward output edge with a copy of the original key partitioner when the
     * gate cannot chain into a keyed downstream operator. A forward edge only preserves key groups
     * while both vertices keep the same parallelism; once one is rescaled on its own (e.g. by the
     * autoscaler) records would reach subtasks that do not own their key groups. At equal
     * parallelism the key partitioner sends every record to the same subtask index as forward does,
     * so the cost is unchanged.
     */
    private static void keepKeyGroupsIfNotChainable(
            StreamGraph graph, StreamNode gate, StreamNode downstream, StreamEdge original) {
        StreamEdge forward = gate.getOutEdges().get(0);
        if (!(original.getPartitioner() instanceof KeyGroupStreamPartitioner)
                || StreamingJobGraphGenerator.isChainable(forward, graph)) {
            return;
        }
        gate.getOutEdges().remove(forward);
        downstream.getInEdges().remove(forward);
        StreamEdge keyed =
                new StreamEdge(
                        gate,
                        downstream,
                        forward.getTypeNumber(),
                        forward.getBufferTimeout(),
                        original.getPartitioner().copy(),
                        forward.getOutputTag(),
                        forward.getExchangeMode(),
                        0,
                        forward.getIntermediateDatasetIdToProduce());
        gate.addOutEdge(keyed);
        downstream.addInEdge(keyed);
    }

    /**
     * Reads a node's max parallelism: the operator's own setting, else the job-wide one, else unset
     * (-1). {@link StreamNode#getMaxParallelism()} is public in Flink 2.x but package-private in
     * Flink 1.x, hence the reflective call, which works on both.
     */
    @VisibleForTesting
    static int maxParallelismOf(StreamNode node) {
        try {
            Method getter = StreamNode.class.getDeclaredMethod("getMaxParallelism");
            getter.setAccessible(true);
            return (int) getter.invoke(node);
        } catch (ReflectiveOperationException e) {
            throw new IllegalStateException(
                    "Could not read the max parallelism of " + node.getOperatorName(), e);
        }
    }

    /**
     * Warns when the gate can chain with neither neighbor, e.g. because chaining is disabled or the
     * downstream operator has several inputs. A standalone gate vertex can be rescaled on its own.
     */
    private static void warnIfNotChainable(StreamGraph graph, StreamNode gate, String gateName) {
        if (!StreamingJobGraphGenerator.isChainable(gate.getInEdges().get(0), graph)
                && !StreamingJobGraphGenerator.isChainable(gate.getOutEdges().get(0), graph)) {
            System.err.println(
                    "[BlueGreen] "
                            + gateName
                            + " cannot be chained to its upstream or downstream operator and will"
                            + " run as a separate vertex, which can be rescaled on its own (e.g. by"
                            + " the autoscaler). Consider excluding it via"
                            + " job.autoscaler.vertex.exclude.ids.");
        }
    }

    private static WatermarkGateProcessFunction<Object> buildGateFunction(
            Configuration config, StreamNode upstream, ClassLoader cl) {

        String strategyName = config.getString("bluegreen.gate.strategy", null);
        if (strategyName == null) {
            throw new IllegalStateException(
                    "bluegreen.gate.strategy is not set. The operator sets it for ADVANCED"
                            + " Blue/Green deployments; supported values: WATERMARK.");
        }
        GateStrategy strategy = GateStrategy.valueOf(strategyName);

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

    /**
     * Builds the one extraction strategy the job configured. The record type at the gate is only
     * used to check that this strategy can read it: nothing is chosen, defaulted or replaced.
     */
    @VisibleForTesting
    static WatermarkExtractor<Object> buildWatermarkExtractor(
            Configuration config, StreamNode upstream, ClassLoader cl) {

        List<String> configured =
                Stream.of(EXTRACTOR_CLASS, FIELD_INDEX, FIELD_PATH)
                        .filter(config::containsKey)
                        .collect(Collectors.toList());
        if (configured.size() != 1) {
            throw new IllegalStateException(
                    "The WATERMARK gate needs exactly one of "
                            + EXTRACTOR_CLASS
                            + ", "
                            + FIELD_INDEX
                            + " or "
                            + FIELD_PATH
                            + ", found: "
                            + (configured.isEmpty() ? "none" : String.join(", ", configured))
                            + ".");
        }

        Optional<Boolean> rowData = isRowData(upstream.getTypeSerializerOut());
        switch (configured.get(0)) {
            case FIELD_INDEX:
                return fieldIndexExtractor(config, upstream.getTypeSerializerOut(), rowData);
            case FIELD_PATH:
                if (rowData.orElse(false)) {
                    throw new IllegalStateException(
                            FIELD_PATH
                                    + " resolves fields by name, but the records at the gate are"
                                    + " Flink RowData, which carries values by position only. Use "
                                    + FIELD_INDEX
                                    + " or "
                                    + EXTRACTOR_CLASS
                                    + ".");
                }
                return new FieldPathWatermarkExtractor(config.getString(FIELD_PATH, null));
            default:
                return instantiateExtractor(config.getString(EXTRACTOR_CLASS, null), cl);
        }
    }

    /**
     * Reads the column at field-index with the accessor its type needs. The column types come from
     * the records' RowDataSerializer, so the index and the type are checked here, before the job
     * runs, rather than on the first record.
     */
    private static WatermarkExtractor<Object> fieldIndexExtractor(
            Configuration config, TypeSerializer<?> serializer, Optional<Boolean> rowData) {
        if (!rowData.orElse(true)) {
            throw new IllegalStateException(
                    FIELD_INDEX
                            + " reads a column of Flink's RowData, the record type of SQL / Table"
                            + " API jobs, but the records at the gate are "
                            + serializer.getClass().getSimpleName()
                            + "-serialized objects. Use "
                            + FIELD_PATH
                            + " or "
                            + EXTRACTOR_CLASS
                            + ".");
        }
        // ConfigOption-based read: the string-key Configuration.getInteger(String, int) is gone in
        // Flink 2.x, get(ConfigOption) works in both majors.
        int fieldIdx = config.get(ConfigOptions.key(FIELD_INDEX).intType().noDefaultValue());
        LogicalType[] columns = columnTypesOf(serializer);
        if (fieldIdx < 0 || fieldIdx >= columns.length) {
            throw new IllegalArgumentException(
                    FIELD_INDEX
                            + "="
                            + fieldIdx
                            + " is outside the "
                            + columns.length
                            + " columns of the records at the gate: "
                            + Arrays.toString(columns)
                            + ".");
        }
        LogicalType column = columns[fieldIdx];
        // fieldIdx and precision are captured primitives — lambdas are serializable via
        // WatermarkExtractor. A null column reads as Long.MIN_VALUE, like a null field-path.
        switch (column.getTypeRoot()) {
            case BIGINT:
                return record ->
                        ((RowData) record).isNullAt(fieldIdx)
                                ? Long.MIN_VALUE
                                : ((RowData) record).getLong(fieldIdx);
            case TIMESTAMP_WITHOUT_TIME_ZONE:
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                // getMillisecond() is what Flink SQL derives a rowtime watermark from, for both
                // types, so the column and the watermark share one time domain.
                int precision = LogicalTypeChecks.getPrecision(column);
                return record ->
                        ((RowData) record).isNullAt(fieldIdx)
                                ? Long.MIN_VALUE
                                : ((RowData) record)
                                        .getTimestamp(fieldIdx, precision)
                                        .getMillisecond();
            default:
                throw new IllegalArgumentException(
                        FIELD_INDEX
                                + "="
                                + fieldIdx
                                + " points at column type "
                                + column.asSummaryString()
                                + ". It must be BIGINT (epoch milliseconds), TIMESTAMP(p) or"
                                + " TIMESTAMP_LTZ(p).");
        }
    }

    /** The column types of SQL / Table API RowData, kept privately by RowDataSerializer. */
    private static LogicalType[] columnTypesOf(TypeSerializer<?> serializer) {
        if (!(serializer instanceof RowDataSerializer)) {
            throw new IllegalStateException(
                    FIELD_INDEX
                            + " needs the column types of the records at the gate, but they are "
                            + serializer.getClass().getSimpleName()
                            + "-serialized, which carries none. Give the stream a RowData type"
                            + " with its columns (InternalTypeInfo), or use "
                            + EXTRACTOR_CLASS
                            + ".");
        }
        // No getter in 1.20 (2.x has one on the serializer snapshot); the field is in both.
        try {
            Field types = RowDataSerializer.class.getDeclaredField("types");
            types.setAccessible(true);
            return (LogicalType[]) types.get(serializer);
        } catch (ReflectiveOperationException | RuntimeException e) {
            throw new IllegalStateException(
                    "Could not read the column types of the records at the gate from"
                            + " RowDataSerializer. Use "
                            + EXTRACTOR_CLASS
                            + ".",
                    e);
        }
    }

    /**
     * Whether the records at the gate are RowData: from a SQL / Table API job, or RowData a
     * DataStream job left to Kryo. Empty when the serializer does not tell.
     */
    private static Optional<Boolean> isRowData(TypeSerializer<?> serializer) {
        if (serializer instanceof AbstractRowDataSerializer) {
            return Optional.of(true);
        }
        if (serializer instanceof KryoSerializer) {
            // KryoSerializer has no getter for the class it serializes, in 1.20 or 2.x.
            try {
                Field type = KryoSerializer.class.getDeclaredField("type");
                type.setAccessible(true);
                return Optional.of(RowData.class.isAssignableFrom((Class<?>) type.get(serializer)));
            } catch (ReflectiveOperationException | RuntimeException e) {
                return Optional.empty();
            }
        }
        return Optional.of(false);
    }

    @SuppressWarnings("unchecked")
    private static WatermarkExtractor<Object> instantiateExtractor(
            String extractorClass, ClassLoader cl) {
        try {
            Object instance =
                    Class.forName(extractorClass, true, cl).getDeclaredConstructor().newInstance();
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
}
