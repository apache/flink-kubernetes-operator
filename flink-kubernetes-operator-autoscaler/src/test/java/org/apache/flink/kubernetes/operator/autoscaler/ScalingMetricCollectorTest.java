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

package org.apache.flink.kubernetes.operator.autoscaler;

import org.apache.flink.api.common.JobID;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.kubernetes.operator.TestingClusterClient;
import org.apache.flink.runtime.rest.messages.job.JobDetailsInfo;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import org.junit.jupiter.api.Test;

import java.util.concurrent.CompletableFuture;

/** Tests for {@link ScalingMetricCollector}. */
public class ScalingMetricCollectorTest {

    @Test
    public void testJobTopologyParsingFromJobDetails() throws Exception {
        String s =
                "{\"jid\":\"8b6cdb9a1db8876d3dd803d5e6108ae3\",\"name\":\"State machine job\",\"isStoppable\":false,\"state\":\"RUNNING\",\"start-time\":1686216314565,\"end-time\":-1,\"duration\":25867,\"maxParallelism\":-1,\"now\":1686216340432,\"timestamps\":{\"INITIALIZING\":1686216314565,\"RECONCILING\":0,\"CANCELLING\":0,\"FAILING\":0,\"CREATED\":1686216314697,\"SUSPENDED\":0,\"RUNNING\":1686216314900,\"FAILED\":0,\"CANCELED\":0,\"FINISHED\":0,\"RESTARTING\":0},\"vertices\":[{\"id\":\"bc764cd8ddf7a0cff126f51c16239658\",\"name\":\"Source: Custom Source\",\"maxParallelism\":128,\"parallelism\":2,\"status\":\"RUNNING\",\"start-time\":1686216324513,\"end-time\":-1,\"duration\":15919,\"tasks\":{\"DEPLOYING\":0,\"RUNNING\":2,\"FINISHED\":0,\"CANCELING\":0,\"SCHEDULED\":0,\"RECONCILING\":0,\"INITIALIZING\":0,\"CREATED\":0,\"FAILED\":0,\"CANCELED\":0},\"metrics\":{\"read-bytes\":0,\"read-bytes-complete\":true,\"write-bytes\":297883,\"write-bytes-complete\":true,\"read-records\":0,\"read-records-complete\":true,\"write-records\":22972,\"write-records-complete\":true,\"accumulated-backpressured-time\":0,\"accumulated-idle-time\":0,\"accumulated-busy-time\":\"NaN\"}},{\"id\":\"20ba6b65f97481d5570070de90e4e791\",\"name\":\"Flat Map -> Sink: Print to Std. Out\",\"maxParallelism\":128,\"parallelism\":2,\"status\":\"RUNNING\",\"start-time\":1686216324570,\"end-time\":-1,\"duration\":15862,\"tasks\":{\"DEPLOYING\":0,\"RUNNING\":2,\"FINISHED\":0,\"CANCELING\":0,\"SCHEDULED\":0,\"RECONCILING\":0,\"INITIALIZING\":0,\"CREATED\":0,\"FAILED\":0,\"CANCELED\":0},\"metrics\":{\"read-bytes\":321025,\"read-bytes-complete\":true,\"write-bytes\":0,\"write-bytes-complete\":true,\"read-records\":22957,\"read-records-complete\":true,\"write-records\":0,\"write-records-complete\":true,\"accumulated-backpressured-time\":0,\"accumulated-idle-time\":28998,\"accumulated-busy-time\":0.0}}],\"status-counts\":{\"DEPLOYING\":0,\"RUNNING\":2,\"FINISHED\":0,\"CANCELING\":0,\"SCHEDULED\":0,\"RECONCILING\":0,\"INITIALIZING\":0,\"CREATED\":0,\"FAILED\":0,\"CANCELED\":0},\"plan\":{\"jid\":\"8b6cdb9a1db8876d3dd803d5e6108ae3\",\"name\":\"State machine job\",\"type\":\"STREAMING\",\"nodes\":[{\"id\":\"20ba6b65f97481d5570070de90e4e791\",\"parallelism\":2,\"operator\":\"\",\"operator_strategy\":\"\",\"description\":\"Flat Map<br/>+- Sink: Print to Std. Out<br/>\",\"inputs\":[{\"num\":0,\"id\":\"bc764cd8ddf7a0cff126f51c16239658\",\"ship_strategy\":\"HASH\",\"exchange\":\"pipelined_bounded\"}],\"optimizer_properties\":{}},{\"id\":\"bc764cd8ddf7a0cff126f51c16239658\",\"parallelism\":2,\"operator\":\"\",\"operator_strategy\":\"\",\"description\":\"Source: Custom Source<br/>\",\"optimizer_properties\":{}}]}}";
        JobDetailsInfo jobDetailsInfo = new ObjectMapper().readValue(s, JobDetailsInfo.class);

        var metricsCollector = new RestApiMetricsCollector();
        var client = new TestingClusterClient<>(new Configuration(), "test");
        client.setRequestProcessor((h, p, b) -> CompletableFuture.completedFuture(jobDetailsInfo));
        metricsCollector.queryJobTopology(client, new JobID());
    }
}
