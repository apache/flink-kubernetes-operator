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
    id            BIGINT       NOT NULL GENERATED ALWAYS AS IDENTITY (START WITH 1, INCREMENT BY 1),
    update_time   TIMESTAMP    NOT NULL,
    job_key       VARCHAR(191) NOT NULL,
    state_type    VARCHAR(100) NOT NULL,
    state_value   CLOB NOT NULL,
    PRIMARY KEY (id)
);

CREATE UNIQUE INDEX un_job_state_type_inx ON t_flink_autoscaler_state_store (job_key, state_type);

CREATE TABLE t_flink_autoscaler_event_handler
(
    id BIGINT NOT NULL GENERATED ALWAYS AS IDENTITY (START WITH 1, INCREMENT BY 1),
    create_time TIMESTAMP NOT NULL,
    update_time TIMESTAMP NOT NULL,
    job_key VARCHAR(191) NOT NULL,
    reason VARCHAR(500) NOT NULL,
    event_type VARCHAR(100) NOT NULL,
    message CLOB NOT NULL,
    event_count INTEGER NOT NULL,
    event_key VARCHAR(100) NOT NULL,
    PRIMARY KEY (id)
);

CREATE INDEX job_key_reason_event_key_idx ON t_flink_autoscaler_event_handler (job_key, reason, event_key);
CREATE INDEX job_key_reason_create_time_idx ON t_flink_autoscaler_event_handler (job_key, reason, create_time);
