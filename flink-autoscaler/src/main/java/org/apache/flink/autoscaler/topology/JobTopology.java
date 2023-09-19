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

import org.apache.flink.runtime.jobgraph.JobVertexID;

import org.apache.flink.shaded.guava30.com.google.common.collect.ImmutableMap;
import org.apache.flink.shaded.guava30.com.google.common.collect.ImmutableSet;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/** Structure representing information about the jobgraph that is relevant for scaling. */
@ToString
@EqualsAndHashCode
public class JobTopology {

    private static final ObjectMapper objectMapper = new ObjectMapper();

    @Getter private final ImmutableMap<JobVertexID, Set<JobVertexID>> inputs;
    @Getter private final ImmutableMap<JobVertexID, Set<JobVertexID>> outputs;
    @Getter private final ImmutableMap<JobVertexID, Integer> parallelisms;
    private final ImmutableMap<JobVertexID, Integer> originalMaxParallelism;
    @Getter private final Map<JobVertexID, Integer> maxParallelisms;
    @Getter private final Set<JobVertexID> finishedVertices;

    @Getter private final List<JobVertexID> verticesInTopologicalOrder;

    public JobTopology(VertexInfo... vertexInfo) {
        this(Set.of(vertexInfo));
    }

    public JobTopology(Set<VertexInfo> vertexInfo) {

        Map<JobVertexID, Set<JobVertexID>> vertexOutputs = new HashMap<>();
        Map<JobVertexID, Set<JobVertexID>> vertexInputs = new HashMap<>();
        Map<JobVertexID, Integer> vertexParallelism = new HashMap<>();
        maxParallelisms = new HashMap<>();

        var finishedVertices = ImmutableSet.<JobVertexID>builder();
        vertexInfo.forEach(
                info -> {
                    var vertexId = info.getId();
                    vertexParallelism.put(vertexId, info.getParallelism());
                    maxParallelisms.put(vertexId, info.getMaxParallelism());

                    vertexInputs.put(vertexId, info.getInputs());
                    vertexOutputs.computeIfAbsent(vertexId, id -> new HashSet<>());
                    info.getInputs()
                            .forEach(
                                    inputId ->
                                            vertexOutputs
                                                    .computeIfAbsent(inputId, id -> new HashSet<>())
                                                    .add(vertexId));
                    if (info.isFinished()) {
                        finishedVertices.add(vertexId);
                    }
                });

        var outputBuilder = ImmutableMap.<JobVertexID, Set<JobVertexID>>builder();
        vertexOutputs.forEach((id, l) -> outputBuilder.put(id, ImmutableSet.copyOf(l)));
        outputs = outputBuilder.build();

        var inputBuilder = ImmutableMap.<JobVertexID, Set<JobVertexID>>builder();
        vertexInputs.forEach((id, l) -> inputBuilder.put(id, ImmutableSet.copyOf(l)));
        this.inputs = inputBuilder.build();

        this.parallelisms = ImmutableMap.copyOf(vertexParallelism);
        this.originalMaxParallelism = ImmutableMap.copyOf(maxParallelisms);
        this.finishedVertices = finishedVertices.build();

        this.verticesInTopologicalOrder = returnVerticesInTopologicalOrder();
    }

    public boolean isSource(JobVertexID jobVertexID) {
        return getInputs().get(jobVertexID).isEmpty();
    }

    public void updateMaxParallelism(JobVertexID vertexID, int maxParallelism) {
        maxParallelisms.put(
                vertexID, Math.min(originalMaxParallelism.get(vertexID), maxParallelism));
    }

    private List<JobVertexID> returnVerticesInTopologicalOrder() {
        List<JobVertexID> sorted = new ArrayList<>(inputs.size());

        Map<JobVertexID, List<JobVertexID>> remainingInputs = new HashMap<>(inputs.size());
        inputs.forEach((v, l) -> remainingInputs.put(v, new ArrayList<>(l)));

        while (!remainingInputs.isEmpty()) {
            List<JobVertexID> verticesWithZeroIndegree = new ArrayList<>();
            remainingInputs.forEach(
                    (v, inputs) -> {
                        if (inputs.isEmpty()) {
                            verticesWithZeroIndegree.add(v);
                        }
                    });

            verticesWithZeroIndegree.forEach(
                    v -> {
                        remainingInputs.remove(v);
                        outputs.get(v).forEach(o -> remainingInputs.get(o).remove(v));
                    });

            sorted.addAll(verticesWithZeroIndegree);
        }
        return sorted;
    }

    public static JobTopology fromJsonPlan(
            String jsonPlan, Map<JobVertexID, Integer> maxParallelismMap, Set<JobVertexID> finished)
            throws JsonProcessingException {
        ObjectNode plan = objectMapper.readValue(jsonPlan, ObjectNode.class);
        ArrayNode nodes = (ArrayNode) plan.get("nodes");

        var vertexInfo = new HashSet<VertexInfo>();

        for (JsonNode node : nodes) {
            var vertexId = JobVertexID.fromHexString(node.get("id").asText());
            var inputList = new HashSet<JobVertexID>();
            vertexInfo.add(
                    new VertexInfo(
                            vertexId,
                            inputList,
                            node.get("parallelism").asInt(),
                            maxParallelismMap.get(vertexId),
                            finished.contains(vertexId)));
            if (node.has("inputs")) {
                for (JsonNode input : node.get("inputs")) {
                    inputList.add(JobVertexID.fromHexString(input.get("id").asText()));
                }
            }
        }

        return new JobTopology(vertexInfo);
    }
}
