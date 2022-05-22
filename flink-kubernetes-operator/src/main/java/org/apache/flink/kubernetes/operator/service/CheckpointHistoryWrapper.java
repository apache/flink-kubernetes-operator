/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.kubernetes.operator.service;

import org.apache.flink.runtime.rest.messages.ResponseBody;
import org.apache.flink.runtime.rest.messages.checkpoints.CheckpointingStatistics;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;

import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;
import java.util.Optional;

import static org.apache.flink.runtime.rest.messages.checkpoints.CheckpointingStatistics.FIELD_NAME_HISTORY;
import static org.apache.flink.runtime.rest.messages.checkpoints.CheckpointingStatistics.FIELD_NAME_LATEST_CHECKPOINTS;
import static org.apache.flink.runtime.rest.messages.checkpoints.CheckpointingStatistics.RestoredCheckpointStatistics.FIELD_NAME_EXTERNAL_PATH;

/** Custom Response for handling checkpoint history in a multi-version compatible way. */
@JsonIgnoreProperties(ignoreUnknown = true)
@Data
@NoArgsConstructor
public class CheckpointHistoryWrapper implements ResponseBody {

    @JsonProperty(FIELD_NAME_HISTORY)
    private List<ObjectNode> history;

    @JsonProperty(FIELD_NAME_LATEST_CHECKPOINTS)
    private ObjectNode latestCheckpoints;

    public Optional<String> getRestorePath() {
        return Optional.ofNullable(latestCheckpoints)
                .map(
                        latest ->
                                latest.get(
                                        CheckpointingStatistics.LatestCheckpoints
                                                .FIELD_NAME_RESTORED))
                .map(restored -> restored.get(FIELD_NAME_EXTERNAL_PATH))
                .map(JsonNode::asText);
    }
}
