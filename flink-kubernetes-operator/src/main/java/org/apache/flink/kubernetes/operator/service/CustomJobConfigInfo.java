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

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Map;

/** Custom Response for fetching job configuration in a multi-version compatible way. */
@JsonIgnoreProperties(ignoreUnknown = true)
@Data
@NoArgsConstructor
public class CustomJobConfigInfo implements ResponseBody {

    public static final String FIELD_NAME_EXECUTION_CONFIG = "execution-config";

    @JsonProperty(FIELD_NAME_EXECUTION_CONFIG)
    private ExecutionConfigInfo executionConfigInfo;

    /** Execution configuration fields consumed by the operator. */
    @JsonIgnoreProperties(ignoreUnknown = true)
    @Data
    @NoArgsConstructor
    public static class ExecutionConfigInfo {

        public static final String FIELD_NAME_PARALLELISM = "job-parallelism";
        public static final String FIELD_NAME_OBJECT_REUSE_MODE = "object-reuse-mode";
        public static final String FIELD_NAME_GLOBAL_JOB_PARAMETERS = "user-config";

        @JsonProperty(FIELD_NAME_PARALLELISM)
        private Integer parallelism;

        @JsonProperty(FIELD_NAME_OBJECT_REUSE_MODE)
        private Boolean objectReuse;

        @JsonProperty(FIELD_NAME_GLOBAL_JOB_PARAMETERS)
        private Map<String, String> globalJobParameters;
    }
}
