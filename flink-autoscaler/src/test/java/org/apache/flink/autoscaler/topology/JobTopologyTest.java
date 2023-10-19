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

package org.apache.flink.autoscaler.topology;

import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobgraph.jsonplan.JsonPlanGenerator;
import org.apache.flink.runtime.scheduler.SchedulerBase;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;

import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.apache.flink.autoscaler.topology.ShipStrategy.FORWARD;
import static org.apache.flink.autoscaler.topology.ShipStrategy.HASH;
import static org.apache.flink.autoscaler.topology.ShipStrategy.REBALANCE;
import static org.apache.flink.autoscaler.topology.ShipStrategy.RESCALE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Test for JobTopology parsing logic. */
public class JobTopologyTest {

    @Test
    public void testTopologyFromJson() throws JsonProcessingException {
        var env = StreamExecutionEnvironment.getExecutionEnvironment();
        var s1 = env.fromElements(1).name("s1");
        var s2 = env.fromElements(1).name("s2");

        s1.union(s2)
                .keyBy(v -> v)
                .map(i -> i)
                .name("map1")
                .setParallelism(2)
                .print()
                .disableChaining()
                .name("sink1")
                .setParallelism(2);

        var s3 = env.fromElements(1).name("s3");
        var map2 = s3.rescale().map(i -> i).name("map2").setParallelism(4).rebalance();

        map2.print().name("sink2").setParallelism(5);
        map2.print().name("sink3").setParallelism(6);

        var jobGraph = env.getStreamGraph().getJobGraph();
        var jsonPlan = JsonPlanGenerator.generatePlan(jobGraph);

        var vertices = new HashMap<String, JobVertexID>();
        var maxParallelism = new HashMap<JobVertexID, Integer>();
        for (JobVertex vertex : jobGraph.getVertices()) {
            vertices.put(vertex.getName(), vertex.getID());
            maxParallelism.put(
                    vertex.getID(),
                    vertex.getMaxParallelism() != -1
                            ? vertex.getMaxParallelism()
                            : SchedulerBase.getDefaultMaxParallelism(vertex));
        }

        JobTopology jobTopology =
                JobTopology.fromJsonPlan(
                        jsonPlan, Map.of(), maxParallelism, Map.of(), Collections.emptySet());

        assertTrue(jobTopology.get(vertices.get("Sink: sink1")).getOutputs().isEmpty());
        assertTrue(jobTopology.get(vertices.get("Sink: sink2")).getOutputs().isEmpty());
        assertTrue(jobTopology.get(vertices.get("Sink: sink3")).getOutputs().isEmpty());

        assertEquals(
                Map.of(vertices.get("map1"), HASH),
                jobTopology.get(vertices.get("Source: s1")).getOutputs());
        assertEquals(
                Map.of(vertices.get("map1"), HASH),
                jobTopology.get(vertices.get("Source: s2")).getOutputs());
        assertEquals(
                Map.of(vertices.get("Sink: sink1"), FORWARD),
                jobTopology.get(vertices.get("map1")).getOutputs());

        assertEquals(
                Map.of(vertices.get("map2"), RESCALE),
                jobTopology.get(vertices.get("Source: s3")).getOutputs());

        assertEquals(
                Map.of(
                        vertices.get("Sink: sink2"),
                        REBALANCE,
                        vertices.get("Sink: sink3"),
                        REBALANCE),
                jobTopology.get(vertices.get("map2")).getOutputs());

        assertEquals(2, jobTopology.get(vertices.get("map1")).getParallelism());
        assertEquals(4, jobTopology.get(vertices.get("map2")).getParallelism());
        jobTopology.getVertexInfos().forEach((v, p) -> assertEquals(128, p.getMaxParallelism()));
    }
}
