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

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobVertex;
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
import org.apache.flink.streaming.runtime.partitioner.ForwardPartitioner;
import org.apache.flink.streaming.runtime.partitioner.KeyGroupStreamPartitioner;
import org.apache.flink.streaming.runtime.partitioner.RebalancePartitioner;

import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Tests how {@link GateInjectorExecutor} wires the gate into the job graph. */
public class GateInjectorExecutorTest {

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
        return Configuration.fromMap(
                Map.of(
                        "bluegreen.active-deployment-type", "BLUE",
                        "bluegreen.configmap.name", "test-configmap",
                        "kubernetes.namespace", "default",
                        "bluegreen.gate.watermark.field-path", "timestamp",
                        "bluegreen.gate.injection.position", position.name()));
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
