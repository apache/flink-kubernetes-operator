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

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SupportsCommitter;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.streaming.api.connector.sink2.SupportsPreCommitTopology;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;
import org.apache.flink.streaming.api.functions.sink.v2.DiscardingSink;
import org.apache.flink.streaming.api.graph.StreamEdge;
import org.apache.flink.streaming.api.graph.StreamGraph;
import org.apache.flink.streaming.api.graph.StreamNode;
import org.apache.flink.streaming.api.graph.StreamingJobGraphGenerator;
import org.apache.flink.streaming.api.operators.ChainingStrategy;
import org.apache.flink.streaming.runtime.operators.sink.SinkWriterOperatorFactory;
import org.apache.flink.streaming.runtime.partitioner.ForwardPartitioner;
import org.apache.flink.streaming.runtime.partitioner.KeyGroupStreamPartitioner;
import org.apache.flink.streaming.runtime.partitioner.RebalancePartitioner;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LocalZonedTimestampType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.TimestampType;

import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.io.Serializable;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Tests how {@link GateInjectorExecutor} wires the gate into the job graph. */
public class GateInjectorExecutorTest {

    private static final String EXTRACTOR_CLASS = "bluegreen.gate.watermark.extractor-class";
    private static final String FIELD_INDEX = "bluegreen.gate.watermark.field-index";
    private static final String FIELD_PATH = "bluegreen.gate.watermark.field-path";
    private static final String GATE_NAME_PREFIX = "BlueGreen-Gate[";

    @Test
    public void gateChainsIntoForwardPipeline() {
        StreamGraph graph = injected(forwardPipeline(), GateInjectionPosition.BEFORE_SINK);

        StreamNode gate = gateNode(graph);
        assertEquals(ChainingStrategy.ALWAYS, gate.getOperatorFactory().getChainingStrategy());
        // Unset max parallelism is copied as is, not replaced by a default
        StreamNode sink = graph.getStreamNode(gate.getOutEdges().get(0).getTargetId());
        assertEquals(
                GateInjectorExecutor.maxParallelismOf(sink),
                GateInjectorExecutor.maxParallelismOf(gate));

        assertEquals(vertexCount(forwardPipeline()), vertexCount(graph));
    }

    @Test
    public void gateKeepsKeyByAndHeadsTheKeyedChain() {
        StreamGraph graph = injected(keyedPipeline(), GateInjectionPosition.AFTER_SOURCE);

        StreamNode gate = gateNode(graph);
        StreamEdge gateInput = gate.getInEdges().get(0);
        StreamEdge gateOutput = gate.getOutEdges().get(0);
        // The keyBy moves in front of the gate, and the gate forwards into the keyed operator
        assertInstanceOf(KeyGroupStreamPartitioner.class, gateInput.getPartitioner());
        assertInstanceOf(ForwardPartitioner.class, gateOutput.getPartitioner());
        assertEquals(4, gate.getParallelism());
        assertEquals(64, GateInjectorExecutor.maxParallelismOf(gate));

        assertEquals(vertexCount(keyedPipeline()), vertexCount(graph));
        // The gate heads the keyed operator's vertex, which keeps the keyed operator's key groups
        JobVertex gateVertex = gateVertex(graph);
        assertTrue(gateVertex.getName().contains("Filter"));
        assertEquals(64, gateVertex.getMaxParallelism());
    }

    @Test
    public void gateKeepsKeyGroupsWhenItCannotChainIntoTheKeyedOperator() {
        StreamGraph original = keyedPipeline(SingleOutputStreamOperator::disableChaining);
        StreamGraph graph =
                injected(
                        keyedPipeline(SingleOutputStreamOperator::disableChaining),
                        GateInjectionPosition.AFTER_SOURCE);

        StreamNode gate = gateNode(graph);
        StreamEdge gateInput = gate.getInEdges().get(0);
        StreamEdge gateOutput = gate.getOutEdges().get(0);
        // A forward edge into the unchained keyed operator would break its key groups on rescale
        assertInstanceOf(KeyGroupStreamPartitioner.class, gateInput.getPartitioner());
        assertInstanceOf(KeyGroupStreamPartitioner.class, gateOutput.getPartitioner());
        StreamNode keyed = graph.getStreamNode(gateOutput.getTargetId());
        assertEquals(List.of(gateOutput), keyed.getInEdges());

        // The gate runs as its own vertex, and the job graph still builds
        assertEquals(vertexCount(original) + 1, vertexCount(graph));
    }

    @Test
    public void gateGoesInFrontOfTheWriterOfACommittingSink() {
        StreamGraph graph = injected(committingSinkPipeline(), GateInjectionPosition.BEFORE_SINK);

        StreamNode gate = gateNode(graph);
        // The gate has its own output edge (its id is not a virtual node's) into the writer
        assertEquals(1, gate.getOutEdges().size());
        StreamNode writer = graph.getStreamNode(gate.getOutEdges().get(0).getTargetId());
        assertInstanceOf(SinkWriterOperatorFactory.class, writer.getOperatorFactory());
        assertEquals(List.of(gate.getOutEdges().get(0)), writer.getInEdges());
        // The committer still receives the writer's committables directly
        StreamNode committer = terminalNode(graph);
        assertEquals(writer.getId(), committer.getInEdges().get(0).getSourceId());
    }

    @Test
    public void gateGoesInFrontOfTheWriterOfASinkWithAPreCommitStep() {
        // The shape of Iceberg 1.9's IcebergSink, which SQL INSERT INTO uses by default
        StreamGraph graph = injected(aggregatingSinkPipeline(), GateInjectionPosition.BEFORE_SINK);

        StreamNode gate = gateNode(graph);
        StreamNode writer = graph.getStreamNode(gate.getOutEdges().get(0).getTargetId());
        assertInstanceOf(SinkWriterOperatorFactory.class, writer.getOperatorFactory());
        // writer -> pre-commit aggregator -> committer, so the gate was found two steps up
        StreamNode committer = terminalNode(graph);
        StreamNode aggregator = graph.getStreamNode(committer.getInEdges().get(0).getSourceId());
        assertTrue(aggregator.getOperatorName().endsWith("pre-commit aggregator"));
        assertEquals(writer.getId(), aggregator.getInEdges().get(0).getSourceId());
    }

    @Test
    public void gateGoesInFrontOfTheOperatorWithTheConfiguredUid() {
        StreamGraph graph = icebergLikePipeline();
        GateInjectorExecutor.injectGates(
                graph,
                gateConfig(
                        GateInjectionPosition.BEFORE_SINK,
                        Map.of("bluegreen.gate.injection.before-uid", "table-writer")));

        StreamNode gate = gateNode(graph);
        StreamNode target = graph.getStreamNode(gate.getOutEdges().get(0).getTargetId());
        assertEquals("table-writer", target.getTransformationUID());
    }

    @Test
    public void failsWhenTheSinkReceivesNoRecords() {
        IllegalStateException e =
                assertThrows(
                        IllegalStateException.class,
                        () ->
                                GateInjectorExecutor.injectGates(
                                        icebergLikePipeline(),
                                        gateConfig(GateInjectionPosition.BEFORE_SINK)));
        assertTrue(e.getMessage().contains("bluegreen.gate.injection.before-uid"));
    }

    @Test
    public void failsWhenTheConfiguredUidMatchesNoOperator() {
        assertThrows(
                IllegalStateException.class,
                () ->
                        GateInjectorExecutor.injectGates(
                                icebergLikePipeline(),
                                gateConfig(
                                        GateInjectionPosition.BEFORE_SINK,
                                        Map.of(
                                                "bluegreen.gate.injection.before-uid",
                                                "no-such-uid"))));
    }

    @Test
    public void failsUnlessExactlyOneExtractorIsConfigured() {
        IllegalStateException none =
                assertThrows(
                        IllegalStateException.class,
                        () ->
                                GateInjectorExecutor.injectGates(
                                        forwardPipeline(),
                                        extractedBy(GateInjectionPosition.BEFORE_SINK, Map.of())));
        assertTrue(none.getMessage().contains("found: none"), none.getMessage());

        IllegalStateException two =
                assertThrows(
                        IllegalStateException.class,
                        () ->
                                GateInjectorExecutor.injectGates(
                                        forwardPipeline(),
                                        gateConfig(
                                                GateInjectionPosition.BEFORE_SINK,
                                                Map.of(EXTRACTOR_CLASS, "com.example.Extractor"))));
        assertTrue(
                two.getMessage().contains("found: " + EXTRACTOR_CLASS + ", " + FIELD_PATH),
                two.getMessage());
    }

    @Test
    public void fieldIndexReadsRowData() {
        StreamGraph graph = rowDataPipeline(InternalTypeInfo.ofFields(new BigIntType()));

        GateInjectorExecutor.injectGates(
                graph, extractedBy(GateInjectionPosition.BEFORE_SINK, Map.of(FIELD_INDEX, "0")));

        assertTrue(gateNode(graph).getOperatorName().startsWith(GATE_NAME_PREFIX));
    }

    @Test
    public void fieldIndexReadsEachTimestampColumnType() {
        LogicalType[] columns = {
            new BigIntType(), new TimestampType(3), new LocalZonedTimestampType(6)
        };
        StreamGraph graph = rowDataPipeline(InternalTypeInfo.ofFields(columns));
        GenericRowData row =
                GenericRowData.of(
                        1_000L,
                        TimestampData.fromEpochMillis(2_000L),
                        TimestampData.fromEpochMillis(3_000L, 456_000));
        // The same row as SQL operators pass it on, with TIMESTAMP(6) outside the fixed-size part
        RowData binary = new RowDataSerializer(columns).toBinaryRow(row).copy();

        for (int column = 0; column < columns.length; column++) {
            WatermarkExtractor<Object> extractor = fieldIndexExtractor(graph, column);
            long expected = (column + 1) * 1_000L;
            assertEquals(expected, extractor.apply(row), columns[column].asSummaryString());
            assertEquals(expected, extractor.apply(binary), columns[column].asSummaryString());
        }
        assertEquals(
                Long.MIN_VALUE,
                fieldIndexExtractor(graph, 1).apply(GenericRowData.of(1_000L, null, null)));
    }

    @Test
    public void failsWhenFieldIndexCannotReadTheColumn() {
        StreamGraph graph =
                rowDataPipeline(InternalTypeInfo.ofFields(new BigIntType(), new IntType()));
        IllegalArgumentException notATime =
                assertThrows(IllegalArgumentException.class, () -> fieldIndexExtractor(graph, 1));
        assertTrue(notATime.getMessage().contains("column type INT"), notATime.getMessage());

        IllegalArgumentException outside =
                assertThrows(IllegalArgumentException.class, () -> fieldIndexExtractor(graph, 2));
        assertTrue(outside.getMessage().contains("outside the 2 columns"), outside.getMessage());

        // RowData a DataStream job left to Kryo carries no column types
        IllegalStateException untyped =
                assertThrows(
                        IllegalStateException.class,
                        () ->
                                fieldIndexExtractor(
                                        rowDataPipeline(TypeInformation.of(RowData.class)), 0));
        assertTrue(untyped.getMessage().contains("needs the column types"), untyped.getMessage());
    }

    @Test
    public void failsWhenFieldIndexMeetsRecordsThatAreNotRowData() {
        IllegalStateException e =
                assertThrows(
                        IllegalStateException.class,
                        () ->
                                GateInjectorExecutor.injectGates(
                                        forwardPipeline(),
                                        extractedBy(
                                                GateInjectionPosition.BEFORE_SINK,
                                                Map.of(FIELD_INDEX, "0"))));
        assertTrue(e.getMessage().startsWith(FIELD_INDEX), e.getMessage());
    }

    @Test
    public void failsWhenFieldPathMeetsRowData() {
        // SQL / Table API RowData, and RowData a DataStream job left to Kryo
        for (TypeInformation<RowData> rowType :
                List.of(
                        InternalTypeInfo.ofFields(new BigIntType()),
                        TypeInformation.of(RowData.class))) {
            IllegalStateException e =
                    assertThrows(
                            IllegalStateException.class,
                            () ->
                                    GateInjectorExecutor.injectGates(
                                            rowDataPipeline(rowType),
                                            gateConfig(GateInjectionPosition.BEFORE_SINK)));
            assertTrue(e.getMessage().startsWith(FIELD_PATH), e.getMessage());
        }
    }

    @Test
    public void gateKeepsRebalanceAndChainsIntoSink() {
        StreamGraph graph = injected(rebalancePipeline(), GateInjectionPosition.BEFORE_SINK);

        StreamNode gate = gateNode(graph);
        assertInstanceOf(RebalancePartitioner.class, gate.getInEdges().get(0).getPartitioner());
        assertEquals(3, gate.getParallelism());

        assertEquals(vertexCount(rebalancePipeline()), vertexCount(graph));
        assertTrue(gateVertex(graph).getName().contains("Sink"));
    }

    @Test
    public void gatesKeepTheInputPositionsOfATwoInputOperator() {
        StreamGraph graph = injected(twoInputPipeline(), GateInjectionPosition.AFTER_SOURCE);

        List<Integer> inputPositions =
                graph.getStreamNodes().stream()
                        .filter(node -> node.getOperatorName().startsWith(GATE_NAME_PREFIX))
                        .map(gate -> gate.getOutEdges().get(0).getTypeNumber())
                        .sorted()
                        .collect(Collectors.toList());
        assertEquals(List.of(1, 2), inputPositions);

        assertEquals(vertexCount(twoInputPipeline()), vertexCount(graph));
    }

    @Test
    public void warnsWhenGateCannotBeChained() {
        StreamGraph graph = forwardPipeline(StreamExecutionEnvironment::disableOperatorChaining);

        PrintStream originalErr = System.err;
        ByteArrayOutputStream captured = new ByteArrayOutputStream();
        System.setErr(new PrintStream(captured, true, StandardCharsets.UTF_8));
        try {
            GateInjectorExecutor.injectGates(graph, gateConfig(GateInjectionPosition.BEFORE_SINK));
        } finally {
            System.setErr(originalErr);
        }

        assertTrue(captured.toString(StandardCharsets.UTF_8).contains("cannot be chained"));
    }

    @Test
    public void doesNotWarnWhenGateIsChained() {
        StreamGraph graph = forwardPipeline();

        PrintStream originalErr = System.err;
        ByteArrayOutputStream captured = new ByteArrayOutputStream();
        System.setErr(new PrintStream(captured, true, StandardCharsets.UTF_8));
        try {
            GateInjectorExecutor.injectGates(graph, gateConfig(GateInjectionPosition.BEFORE_SINK));
        } finally {
            System.setErr(originalErr);
        }

        assertFalse(captured.toString(StandardCharsets.UTF_8).contains("cannot be chained"));
    }

    // ==================== Pipelines ====================

    /** source -> filter -> sink, all at parallelism 2 and connected by forward edges. */
    private static StreamGraph forwardPipeline() {
        return forwardPipeline(env -> {});
    }

    private static StreamGraph forwardPipeline(Consumer<StreamExecutionEnvironment> envSetup) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);
        envSetup.accept(env);
        env.fromSequence(0, 10).filter(v -> true).sinkTo(new DiscardingSink<>());
        return env.getStreamGraph();
    }

    private static StreamGraph rowDataPipeline(TypeInformation<RowData> rowType) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.fromSequence(0, 10)
                .map(v -> (RowData) GenericRowData.of(v))
                .returns(rowType)
                .sinkTo(new DiscardingSink<>());
        return env.getStreamGraph();
    }

    /** source (2) -keyBy-> keyed filter (4, max parallelism 64) -> sink (4). */
    private static StreamGraph keyedPipeline() {
        return keyedPipeline(keyedOperator -> {});
    }

    private static StreamGraph keyedPipeline(
            Consumer<SingleOutputStreamOperator<Long>> keyedOperatorSetup) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);
        SingleOutputStreamOperator<Long> keyedOperator =
                env.fromSequence(0, 10)
                        .keyBy(v -> v % 4, Types.LONG)
                        .filter(v -> true)
                        .setParallelism(4)
                        .setMaxParallelism(64);
        keyedOperatorSetup.accept(keyedOperator);
        keyedOperator.sinkTo(new DiscardingSink<>()).setParallelism(4);
        return env.getStreamGraph();
    }

    /** source -> filter at parallelism 2, rebalanced into a sink at parallelism 3. */
    private static StreamGraph rebalancePipeline() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);
        env.fromSequence(0, 10).filter(v -> true).sinkTo(new DiscardingSink<>()).setParallelism(3);
        return env.getStreamGraph();
    }

    /** source -> map -> a Sink V2 with a committer (Writer -> Committer). */
    private static StreamGraph committingSinkPipeline() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.fromSequence(0, 10).map(v -> v).sinkTo(committingSink());
        return env.getStreamGraph();
    }

    private static StreamGraph aggregatingSinkPipeline() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.fromSequence(0, 10).map(v -> v).sinkTo(aggregatingSink());
        return env.getStreamGraph();
    }

    /**
     * The shape of Iceberg's FlinkSink: a writer and a committer as regular operators, ending in a
     * sink that receives no records (Void).
     */
    private static StreamGraph icebergLikePipeline() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.fromSequence(0, 10)
                .map(v -> v)
                .uid("table-writer")
                .map(v -> (Void) null)
                .returns(Types.VOID)
                .uid("table-committer")
                .sinkTo(new DiscardingSink<>());
        return env.getStreamGraph();
    }

    /**
     * A Sink V2 with a committer. Built as a proxy because the abstract createWriter method differs
     * between Flink 1.x (InitContext) and 2.x (WriterInitContext); only the committable serializer
     * is needed to build the stream graph.
     */
    @SuppressWarnings("unchecked")
    private static Sink<Long> committingSink() {
        return (Sink<Long>)
                Proxy.newProxyInstance(
                        GateInjectorExecutorTest.class.getClassLoader(),
                        new Class<?>[] {Sink.class, SupportsCommitter.class},
                        new CommittingSinkHandler());
    }

    private static class CommittingSinkHandler implements InvocationHandler, Serializable {
        @Override
        public Object invoke(Object proxy, Method method, Object[] args) {
            switch (method.getName()) {
                case "getCommittableSerializer":
                    return new StringSerializer();
                case "hashCode":
                    return System.identityHashCode(proxy);
                case "equals":
                    return proxy == args[0];
                case "toString":
                    return "CommittingSink";
                default:
                    throw new UnsupportedOperationException(method.getName());
            }
        }
    }

    /** A committing sink that, like Iceberg's IcebergSink, aggregates in one task before commit. */
    @SuppressWarnings("unchecked")
    private static Sink<Long> aggregatingSink() {
        return (Sink<Long>)
                Proxy.newProxyInstance(
                        GateInjectorExecutorTest.class.getClassLoader(),
                        new Class<?>[] {
                            Sink.class, SupportsCommitter.class, SupportsPreCommitTopology.class
                        },
                        new AggregatingSinkHandler());
    }

    private static class AggregatingSinkHandler extends CommittingSinkHandler {
        @Override
        @SuppressWarnings("unchecked")
        public Object invoke(Object proxy, Method method, Object[] args) {
            switch (method.getName()) {
                case "getWriteResultSerializer":
                    return new StringSerializer();
                case "addPreCommitTopology":
                    DataStream<Object> writeResults = (DataStream<Object>) args[0];
                    return writeResults
                            .global()
                            .map(
                                    (MapFunction<Object, Object>) result -> result,
                                    writeResults.getType())
                            .name("pre-commit aggregator")
                            .setParallelism(1)
                            .global();
                default:
                    return super.invoke(proxy, method, args);
            }
        }
    }

    private static class StringSerializer implements SimpleVersionedSerializer<String> {
        @Override
        public int getVersion() {
            return 1;
        }

        @Override
        public byte[] serialize(String committable) {
            return committable.getBytes(StandardCharsets.UTF_8);
        }

        @Override
        public String deserialize(int version, byte[] serialized) {
            return new String(serialized, StandardCharsets.UTF_8);
        }
    }

    /** One source feeding both inputs of a two-input operator. */
    private static StreamGraph twoInputPipeline() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);
        DataStream<Long> source = env.fromSequence(0, 10);
        source.connect(source)
                .map(
                        new CoMapFunction<Long, Long, Long>() {
                            @Override
                            public Long map1(Long value) {
                                return value;
                            }

                            @Override
                            public Long map2(Long value) {
                                return value;
                            }
                        })
                .sinkTo(new DiscardingSink<>());
        return env.getStreamGraph();
    }

    // ==================== Helpers ====================

    private static Configuration gateConfig(GateInjectionPosition position) {
        return gateConfig(position, Map.of());
    }

    private static Configuration gateConfig(
            GateInjectionPosition position, Map<String, String> extra) {
        Map<String, String> config =
                new HashMap<>(
                        Map.of(
                                "bluegreen.active-deployment-type",
                                "BLUE",
                                "bluegreen.configmap.name",
                                "test-configmap",
                                "kubernetes.namespace",
                                "default",
                                "bluegreen.gate.strategy",
                                "WATERMARK",
                                FIELD_PATH,
                                "timestamp",
                                "bluegreen.gate.injection.position",
                                position.name()));
        config.putAll(extra);
        return Configuration.fromMap(config);
    }

    /** The extractor the gate builds for field-index, for the records leaving the Map. */
    private static WatermarkExtractor<Object> fieldIndexExtractor(StreamGraph graph, int column) {
        StreamNode map =
                graph.getStreamNodes().stream()
                        .filter(node -> node.getOperatorName().equals("Map"))
                        .findFirst()
                        .orElseThrow();
        return GateInjectorExecutor.buildWatermarkExtractor(
                extractedBy(
                        GateInjectionPosition.BEFORE_SINK,
                        Map.of(FIELD_INDEX, String.valueOf(column))),
                map,
                GateInjectorExecutorTest.class.getClassLoader());
    }

    /** A gate config with the given extraction keys in place of the default field-path. */
    private static Configuration extractedBy(
            GateInjectionPosition position, Map<String, String> extractors) {
        Configuration config = gateConfig(position);
        config.removeConfig(ConfigOptions.key(FIELD_PATH).stringType().noDefaultValue());
        extractors.forEach(
                (key, value) ->
                        config.set(ConfigOptions.key(key).stringType().noDefaultValue(), value));
        return config;
    }

    private static StreamGraph injected(StreamGraph graph, GateInjectionPosition position) {
        GateInjectorExecutor.injectGates(graph, gateConfig(position));
        return graph;
    }

    private static StreamNode gateNode(StreamGraph graph) {
        return graph.getStreamNodes().stream()
                .filter(node -> node.getOperatorName().startsWith(GATE_NAME_PREFIX))
                .findFirst()
                .orElseThrow();
    }

    private static StreamNode terminalNode(StreamGraph graph) {
        return graph.getStreamNodes().stream()
                .filter(node -> node.getOutEdges().isEmpty())
                .findFirst()
                .orElseThrow();
    }

    private static int vertexCount(StreamGraph graph) {
        return StreamingJobGraphGenerator.createJobGraph(graph).getNumberOfVertices();
    }

    private static JobVertex gateVertex(StreamGraph graph) {
        JobGraph jobGraph = StreamingJobGraphGenerator.createJobGraph(graph);
        return StreamSupport.stream(jobGraph.getVertices().spliterator(), false)
                .filter(vertex -> vertex.getName().contains(GATE_NAME_PREFIX))
                .findFirst()
                .orElseThrow();
    }
}
