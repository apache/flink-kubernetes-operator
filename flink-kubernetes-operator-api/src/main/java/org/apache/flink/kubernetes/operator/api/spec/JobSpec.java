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
import org.apache.flink.kubernetes.operator.api.diff.DiffType;
import org.apache.flink.kubernetes.operator.api.diff.Diffable;
import org.apache.flink.kubernetes.operator.api.diff.SpecDiff;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;

/** Flink job spec. */
@Experimental
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
@EqualsAndHashCode
@JsonIgnoreProperties(ignoreUnknown = true)
public class JobSpec implements Diffable<JobSpec> {

    /**
     * Optional URI of the job jar within the Flink docker container. For example:
     * local:///opt/flink/examples/streaming/StateMachineExample.jar. If not specified the job jar
     * should be available in the system classpath.
     */
    private String jarURI;

    /** Parallelism of the Flink job. */
    @SpecDiff(value = DiffType.SCALE, mode = KubernetesDeploymentMode.STANDALONE)
    private int parallelism;

    /** Fully qualified main class name of the Flink job. */
    private String entryClass;

    /** Arguments for the Flink job main class. */
    private String[] args = new String[0];

    /** Desired state for the job. */
    private JobState state = JobState.RUNNING;

    /**
     * Nonce used to manually trigger savepoint for the running job. In order to trigger a
     * savepoint, change the number to a different non-null value.
     */
    @SpecDiff(DiffType.IGNORE)
    @Deprecated
    private Long savepointTriggerNonce;

    /**
     * Savepoint path used by the job the first time it is deployed or during savepoint
     * redeployments (triggered by changing the savepointRedeployNonce).
     */
    @SpecDiff(DiffType.IGNORE)
    private String initialSavepointPath;

    /**
     * Nonce used to manually trigger checkpoint for the running job. In order to trigger a
     * checkpoint, change the number to a different non-null value.
     */
    @SpecDiff(DiffType.IGNORE)
    @Deprecated
    private Long checkpointTriggerNonce;

    /** Upgrade mode of the Flink job. */
    @SpecDiff(DiffType.IGNORE)
    private UpgradeMode upgradeMode = UpgradeMode.STATELESS;

    /** Allow checkpoint state that cannot be mapped to any job vertex in tasks. */
    @SpecDiff(DiffType.IGNORE)
    private Boolean allowNonRestoredState;

    /**
     * Nonce used to trigger a full redeployment of the job from the savepoint path specified in
     * initialSavepointPath. In order to trigger redeployment, change the number to a different
     * non-null value. Rollback is not possible after redeployment.
     */
    @SpecDiff(value = DiffType.SAVEPOINT_REDEPLOY, onNullIgnore = true)
    private Long savepointRedeployNonce;
}
