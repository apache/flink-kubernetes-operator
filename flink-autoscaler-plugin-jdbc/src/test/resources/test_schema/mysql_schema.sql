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

create table `t_flink_autoscaler_state_store`
(
    `id`            bigint       not null auto_increment,
    `update_time`   datetime     not null comment 'The update time',
    `job_key`       varchar(191) not null comment 'The job key',
    `state_type`    varchar(100) not null comment 'The state type',
    `state_value`   longtext     not null comment 'The real state',
    primary key (`id`) using btree,
    unique key `un_job_state_type_inx` (`job_key`,`state_type`) using btree
) engine=innodb default charset=utf8mb4 collate=utf8mb4_general_ci;

create table `t_flink_autoscaler_event_handler`
(
    `id`            bigint       not null auto_increment,
    `create_time`   datetime     not null comment 'The create time',
    `update_time`   datetime     not null comment 'The update time',
    `job_key`       varchar(191) not null comment 'The job key',
    `reason`        varchar(191) not null comment 'The event reason, such as: ScalingReport, IneffectiveScaling and AutoscalerError, etc.',
    `event_type`    varchar(100) not null comment 'The event type, such as: Normal, Warning.',
    `message`       longtext     not null comment 'The event message.',
    `event_count`   int          not null comment 'The count of current event.',
    `event_key`     varchar(100) not null comment 'The event key is used for event deduplication.',
    primary key (`id`) using btree,
    INDEX `job_key_reason_event_key_idx` (`job_key`, `reason`, `event_key`),
    INDEX `job_key_reason_create_time_idx` (`job_key`, `reason`, `create_time`)
) engine=innodb default charset=utf8mb4 collate=utf8mb4_general_ci;
