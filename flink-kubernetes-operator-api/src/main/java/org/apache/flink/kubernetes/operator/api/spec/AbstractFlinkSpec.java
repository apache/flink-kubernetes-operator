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

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.SuperBuilder;

import java.util.Map;

/** The common spec. */
@Experimental
@Data
@AllArgsConstructor
@NoArgsConstructor
@SuperBuilder
public abstract class AbstractFlinkSpec implements Diffable<AbstractFlinkSpec> {

    /** Job specification for application deployments/session job. Null for session clusters. */
    private JobSpec job;

    /**
     * Nonce used to manually trigger restart for the cluster/session job. In order to trigger
     * restart, change the number to a different non-null value.
     */
    @SpecDiff(onNullIgnore = true)
    private Long restartNonce;

    /** Flink configuration overrides for the Flink deployment or Flink session job. */
    @SpecDiff.Config({
        @SpecDiff.Entry(prefix = "job.autoscaler", type = DiffType.IGNORE),
        @SpecDiff.Entry(prefix = "parallelism.default", type = DiffType.IGNORE),
        @SpecDiff.Entry(prefix = "kubernetes.operator", type = DiffType.IGNORE),
        @SpecDiff.Entry(
                prefix = "pipeline.jobvertex-parallelism-overrides",
                type = DiffType.SCALE,
                mode = KubernetesDeploymentMode.NATIVE)
    })
    private Map<String, String> flinkConfiguration;
}
