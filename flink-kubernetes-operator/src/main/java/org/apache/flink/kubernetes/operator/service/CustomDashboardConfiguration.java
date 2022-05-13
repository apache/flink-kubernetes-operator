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

/** Custom Response for handling dashboard configs. */
@JsonIgnoreProperties(ignoreUnknown = true)
@Data
@NoArgsConstructor
public class CustomDashboardConfiguration implements ResponseBody {

    public static final String FIELD_NAME_FLINK_VERSION = "flink-version";
    public static final String FIELD_NAME_FLINK_REVISION = "flink-revision";

    @JsonProperty(FIELD_NAME_FLINK_VERSION)
    private String flinkVersion;

    @JsonProperty(FIELD_NAME_FLINK_REVISION)
    private String flinkRevision;
}
