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

package org.apache.flink.kubernetes.operator.api.status;

import org.apache.flink.annotation.Experimental;
import org.apache.flink.kubernetes.operator.api.diff.Diffable;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import io.fabric8.crd.generator.annotation.PrinterColumn;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.experimental.SuperBuilder;

/** Last observed status of the Flink state snapshot. */
@Experimental
@Data
@AllArgsConstructor
@NoArgsConstructor
@EqualsAndHashCode
@ToString
@SuperBuilder
@JsonIgnoreProperties(ignoreUnknown = true)
public class FlinkStateSnapshotStatus implements Diffable<FlinkStateSnapshotStatus> {

    /** Current state of the snapshot. */
    @PrinterColumn(name = "Snapshot Status")
    private FlinkStateSnapshotState state = FlinkStateSnapshotState.TRIGGER_PENDING;

    /** Trigger ID of the snapshot. */
    private String triggerId;

    /** Trigger timestamp of a pending snapshot operation. */
    private String triggerTimestamp;

    /** Timestamp when the snapshot was last created/failed. */
    @PrinterColumn(name = "Result Timestamp")
    private String resultTimestamp;

    /** Final path of the snapshot. */
    @PrinterColumn(name = "Path")
    private String path;

    /** Optional error information about the FlinkStateSnapshot. */
    private String error;

    /** Number of failures, used for tracking max retries. */
    private int failures = 0;
}
