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

import com.fasterxml.jackson.core.JsonProcessingException;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Set;

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
                .shuffle()
                .map(i -> i)
                .name("map1")
                .setParallelism(2)
                .shuffle()
                .print()
                .name("sink1")
                .setParallelism(3);

        var s3 = env.fromElements(1).name("s3");
        var map2 = s3.shuffle().map(i -> i).name("map2").setParallelism(4).shuffle();

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
                JobTopology.fromJsonPlan(jsonPlan, maxParallelism, Collections.emptySet());

        assertTrue(jobTopology.getOutputs().get(vertices.get("Sink: sink1")).isEmpty());
        assertTrue(jobTopology.getOutputs().get(vertices.get("Sink: sink2")).isEmpty());
        assertTrue(jobTopology.getOutputs().get(vertices.get("Sink: sink3")).isEmpty());

        assertEquals(
                Set.of(vertices.get("map1")),
                jobTopology.getOutputs().get(vertices.get("Source: s1")));
        assertEquals(
                Set.of(vertices.get("map1")),
                jobTopology.getOutputs().get(vertices.get("Source: s2")));
        assertEquals(
                Set.of(vertices.get("map2")),
                jobTopology.getOutputs().get(vertices.get("Source: s3")));

        assertEquals(
                Set.of(vertices.get("Sink: sink2"), vertices.get("Sink: sink3")),
                jobTopology.getOutputs().get(vertices.get("map2")));

        assertEquals(2, jobTopology.getParallelisms().get(vertices.get("map1")));
        assertEquals(4, jobTopology.getParallelisms().get(vertices.get("map2")));
        jobTopology.getMaxParallelisms().forEach((v, p) -> assertEquals(128, p));
    }
}
