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

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.experimental.SuperBuilder;

/** Last observed status of the Flink Blue/Green deployment. */
@Experimental
@Data
@AllArgsConstructor
@NoArgsConstructor
@ToString(callSuper = true)
@SuperBuilder
@JsonIgnoreProperties(ignoreUnknown = true)
public class FlinkBlueGreenDeploymentStatus {

    private JobStatus jobStatus = new JobStatus();

    /** The state of the blue/green transition. */
    private FlinkBlueGreenDeploymentState blueGreenState;

    /** Last reconciled (serialized) deployment spec. */
    private String lastReconciledSpec;

    /** Timestamp of last reconciliation. */
    private String lastReconciledTimestamp;

    /** Computed from abortGracePeriodMs, timestamp after which the deployment should be aborted. */
    private String abortTimestamp;

    /** Timestamp when the deployment became READY/STABLE. Used to determine when to delete it. */
    private String deploymentReadyTimestamp;

    /** Information about the TaskManagers for the scale subresource. */
    private TaskManagerInfo taskManager;
}
