/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

CREATE TABLE t_flink_autoscaler_state_store
(
    id            BIGSERIAL     NOT NULL,
    update_time   TIMESTAMP     NOT NULL,
    job_key       TEXT          NOT NULL,
    state_type    TEXT          NOT NULL,
    state_value   TEXT          NOT NULL,
    PRIMARY KEY (id),
    UNIQUE (job_key, state_type)
);

CREATE TABLE t_flink_autoscaler_event_handler
(
    id BIGINT NOT NULL PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
    create_time     TIMESTAMP       NOT NULL,
    update_time     TIMESTAMP       NOT NULL,
    job_key         VARCHAR(191)    NOT NULL,
    reason          VARCHAR(500)    NOT NULL,
    event_type      VARCHAR(100)    NOT NULL,
    message         TEXT            NOT NULL,
    event_count     INTEGER         NOT NULL,
    event_key       VARCHAR(100)    NOT NULL
);

CREATE INDEX job_key_reason_event_key_idx ON t_flink_autoscaler_event_handler (job_key, reason, event_key);
CREATE INDEX job_key_reason_create_time_idx ON t_flink_autoscaler_event_handler (job_key, reason, create_time);
