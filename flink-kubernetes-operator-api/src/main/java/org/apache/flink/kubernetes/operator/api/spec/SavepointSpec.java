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

package org.apache.flink.kubernetes.operator.api.spec;

import org.apache.flink.annotation.Experimental;
import org.apache.flink.kubernetes.operator.api.status.SavepointFormatType;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/** Spec for savepoint state snapshots. */
@Experimental
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
@JsonIgnoreProperties(ignoreUnknown = true)
public class SavepointSpec {
    /** Optional path for the savepoint. */
    private String path;

    /** Savepoint format to use. */
    private SavepointFormatType formatType = SavepointFormatType.CANONICAL;

    /** Dispose the savepoints upon CR deletion. */
    private Boolean disposeOnDelete = true;

    /**
     * Indicates that the savepoint already exists on the given path. The Operator will not trigger
     * any new savepoints, just update the status of the resource as a completed snapshot.
     */
    private Boolean alreadyExists = false;
}
