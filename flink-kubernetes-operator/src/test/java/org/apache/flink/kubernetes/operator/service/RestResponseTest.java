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

import org.apache.flink.runtime.rest.util.RestMapperUtils;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import org.junit.jupiter.api.Test;

import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Tests for custom rest response classes. */
public class RestResponseTest {

    private ObjectMapper objectMapper = RestMapperUtils.getStrictObjectMapper();

    @Test
    public void testGetLastSavepointRestCompatibility() {
        String flink14Response =
                "{\"counts\":{\"restored\":0,\"total\":2,\"in_progress\":0,\"completed\":2,\"failed\":0},\"summary\":{\"state_size\":{\"min\":8646,\"max\":25626,\"avg\":17136},\"end_to_end_duration\":{\"min\":95,\"max\":420,\"avg\":257},\"alignment_buffered\":{\"min\":0,\"max\":0,\"avg\":0},\"processed_data\":{\"min\":0,\"max\":70,\"avg\":35},\"persisted_data\":{\"min\":0,\"max\":0,\"avg\":0}},\"latest\":{\"completed\":{\"@class\":\"completed\",\"id\":1,\"status\":\"COMPLETED\",\"is_savepoint\":false,\"trigger_timestamp\":1652347653972,\"latest_ack_timestamp\":1652347654392,\"state_size\":8646,\"end_to_end_duration\":420,\"alignment_buffered\":0,\"processed_data\":0,\"persisted_data\":0,\"num_subtasks\":4,\"num_acknowledged_subtasks\":4,\"checkpoint_type\":\"CHECKPOINT\",\"tasks\":{},\"external_path\":\"file:/opt/flink/volume/flink-cp/9f096f515d5d66dbda0d854b5d5a7af2/chk-1\",\"discarded\":true},\"savepoint\":{\"@class\":\"completed\",\"id\":2,\"status\":\"COMPLETED\",\"is_savepoint\":true,\"trigger_timestamp\":1652347655184,\"latest_ack_timestamp\":1652347655279,\"state_size\":25626,\"end_to_end_duration\":95,\"alignment_buffered\":0,\"processed_data\":70,\"persisted_data\":0,\"num_subtasks\":4,\"num_acknowledged_subtasks\":4,\"checkpoint_type\":\"SYNC_SAVEPOINT\",\"tasks\":{},\"external_path\":\"file:/opt/flink/volume/flink-sp/savepoint-9f096f-cebc9a861a41\",\"discarded\":false},\"failed\":null,\"restored\":null},\"history\":[{\"@class\":\"completed\",\"id\":2,\"status\":\"COMPLETED\",\"is_savepoint\":true,\"trigger_timestamp\":1652347655184,\"latest_ack_timestamp\":1652347655279,\"state_size\":25626,\"end_to_end_duration\":95,\"alignment_buffered\":0,\"processed_data\":70,\"persisted_data\":0,\"num_subtasks\":4,\"num_acknowledged_subtasks\":4,\"checkpoint_type\":\"SYNC_SAVEPOINT\",\"tasks\":{},\"external_path\":\"file:/opt/flink/volume/flink-sp/savepoint-9f096f-cebc9a861a41\",\"discarded\":false},{\"@class\":\"completed\",\"id\":1,\"status\":\"COMPLETED\",\"is_savepoint\":false,\"trigger_timestamp\":1652347653972,\"latest_ack_timestamp\":1652347654392,\"state_size\":8646,\"end_to_end_duration\":420,\"alignment_buffered\":0,\"processed_data\":0,\"persisted_data\":0,\"num_subtasks\":4,\"num_acknowledged_subtasks\":4,\"checkpoint_type\":\"CHECKPOINT\",\"tasks\":{},\"external_path\":\"file:/opt/flink/volume/flink-cp/9f096f515d5d66dbda0d854b5d5a7af2/chk-1\",\"discarded\":true}]}";
        String flink15Response =
                "{\"counts\":{\"restored\":0,\"total\":12,\"in_progress\":0,\"completed\":3,\"failed\":9},\"summary\":{\"checkpointed_size\":{\"min\":4308,\"max\":16053,\"avg\":11856,\"p50\":15207,\"p90\":16053,\"p95\":16053,\"p99\":16053,\"p999\":16053},\"state_size\":{\"min\":4308,\"max\":16053,\"avg\":11856,\"p50\":15207,\"p90\":16053,\"p95\":16053,\"p99\":16053,\"p999\":16053},\"end_to_end_duration\":{\"min\":31,\"max\":117,\"avg\":61,\"p50\":36,\"p90\":117,\"p95\":117,\"p99\":117,\"p999\":117},\"alignment_buffered\":{\"min\":0,\"max\":0,\"avg\":0,\"p50\":0,\"p90\":0,\"p95\":0,\"p99\":0,\"p999\":0},\"processed_data\":{\"min\":0,\"max\":1232,\"avg\":448,\"p50\":112,\"p90\":1232,\"p95\":1232,\"p99\":1232,\"p999\":1232},\"persisted_data\":{\"min\":0,\"max\":0,\"avg\":0,\"p50\":0,\"p90\":0,\"p95\":0,\"p99\":0,\"p999\":0}},\"latest\":{\"completed\":{\"className\":\"completed\",\"id\":3,\"status\":\"COMPLETED\",\"is_savepoint\":false,\"trigger_timestamp\":1652348962111,\"latest_ack_timestamp\":1652348962142,\"checkpointed_size\":15207,\"state_size\":15207,\"end_to_end_duration\":31,\"alignment_buffered\":0,\"processed_data\":0,\"persisted_data\":0,\"num_subtasks\":4,\"num_acknowledged_subtasks\":4,\"checkpoint_type\":\"CHECKPOINT\",\"tasks\":{},\"external_path\":\"file:/flink-data/checkpoints/00000000000000000000000000000000/chk-3\",\"discarded\":true},\"savepoint\":null,\"failed\":null,\"restored\":null},\"history\":[{\"className\":\"completed\",\"id\":3,\"status\":\"COMPLETED\",\"is_savepoint\":false,\"trigger_timestamp\":1652348962111,\"latest_ack_timestamp\":1652348962142,\"checkpointed_size\":15207,\"state_size\":15207,\"end_to_end_duration\":31,\"alignment_buffered\":0,\"processed_data\":0,\"persisted_data\":0,\"num_subtasks\":4,\"num_acknowledged_subtasks\":4,\"checkpoint_type\":\"CHECKPOINT\",\"tasks\":{},\"external_path\":\"file:/flink-data/checkpoints/00000000000000000000000000000000/chk-3\",\"discarded\":true},{\"className\":\"completed\",\"id\":2,\"status\":\"COMPLETED\",\"is_savepoint\":false,\"trigger_timestamp\":1652348960109,\"latest_ack_timestamp\":1652348960145,\"checkpointed_size\":16053,\"state_size\":16053,\"end_to_end_duration\":36,\"alignment_buffered\":0,\"processed_data\":1232,\"persisted_data\":0,\"num_subtasks\":4,\"num_acknowledged_subtasks\":4,\"checkpoint_type\":\"CHECKPOINT\",\"tasks\":{},\"external_path\":\"file:/flink-data/checkpoints/00000000000000000000000000000000/chk-2\",\"discarded\":true},{\"className\":\"completed\",\"id\":1,\"status\":\"COMPLETED\",\"is_savepoint\":false,\"trigger_timestamp\":1652348958109,\"latest_ack_timestamp\":1652348958226,\"checkpointed_size\":4308,\"state_size\":4308,\"end_to_end_duration\":117,\"alignment_buffered\":0,\"processed_data\":112,\"persisted_data\":0,\"num_subtasks\":4,\"num_acknowledged_subtasks\":4,\"checkpoint_type\":\"CHECKPOINT\",\"tasks\":{},\"external_path\":\"file:/flink-data/checkpoints/00000000000000000000000000000000/chk-1\",\"discarded\":true}]}";

        assertDoesNotThrow(
                () -> objectMapper.readValue(flink14Response, CheckpointHistoryWrapper.class));
        assertDoesNotThrow(
                () -> objectMapper.readValue(flink15Response, CheckpointHistoryWrapper.class));
    }

    @Test
    public void testGetInProgressCheckpointsFromResponseWithoutHistoryDetails()
            throws JsonProcessingException {
        String response =
                "{\"counts\":{\"restored\":0,\"total\":2,\"in_progress\":0,\"completed\":2,\"failed\":0}}";
        var checkpointHistoryWrapper =
                objectMapper.readValue(response, CheckpointHistoryWrapper.class);
        Optional<CheckpointHistoryWrapper.PendingCheckpointInfo> optionalPendingCheckpointInfo =
                assertDoesNotThrow(checkpointHistoryWrapper::getInProgressCheckpoint);
        assertTrue(optionalPendingCheckpointInfo.isEmpty());
    }

    @Test
    public void testGetInProgressCheckpointsWithoutHistory() {
        CheckpointHistoryWrapper checkpointHistoryWrapper = new CheckpointHistoryWrapper();
        Optional<CheckpointHistoryWrapper.PendingCheckpointInfo> optionalPendingCheckpointInfo =
                assertDoesNotThrow(checkpointHistoryWrapper::getInProgressCheckpoint);
        assertTrue(optionalPendingCheckpointInfo.isEmpty());
    }

    @Test
    public void testClusterInfoRestCompatibility() {
        String flink13Response =
                "{\"refresh-interval\":3000,\"timezone-name\":\"Coordinated Universal Time\",\"timezone-offset\":0,\"flink-version\":\"1.13.6\",\"flink-revision\":\"b2ca390 @ 2022-02-03T14:54:22+01:00\",\"features\":{\"web-submit\":false}}";
        String flink14Response =
                "{\"refresh-interval\":3000,\"timezone-name\":\"Coordinated Universal Time\",\"timezone-offset\":0,\"flink-version\":\"1.14.4\",\"flink-revision\":\"895c609 @ 2022-02-25T11:57:14+01:00\",\"features\":{\"web-submit\":false,\"web-cancel\":false}}";

        assertDoesNotThrow(
                () -> objectMapper.readValue(flink13Response, CustomDashboardConfiguration.class));
        assertDoesNotThrow(
                () -> objectMapper.readValue(flink14Response, CustomDashboardConfiguration.class));
    }
}
