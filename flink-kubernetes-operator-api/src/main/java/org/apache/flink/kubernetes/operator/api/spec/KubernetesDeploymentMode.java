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
import org.apache.flink.kubernetes.operator.api.FlinkDeployment;

import com.fasterxml.jackson.annotation.JsonProperty;

/** Enum to control Flink deployment mode on Kubernetes. */
@Experimental
public enum KubernetesDeploymentMode {

    /**
     * Deploys Flink using Flinks native Kubernetes support. Only supported for newer versions of
     * Flink
     */
    @JsonProperty("native")
    NATIVE,

    /** Deploys Flink on-top of kubernetes in standalone mode. */
    @JsonProperty("standalone")
    STANDALONE;

    public static KubernetesDeploymentMode getDeploymentMode(FlinkDeployment flinkDeployment) {
        return getDeploymentMode(flinkDeployment.getSpec());
    }

    public static KubernetesDeploymentMode getDeploymentMode(FlinkDeploymentSpec spec) {
        return spec.getMode() == null ? KubernetesDeploymentMode.NATIVE : spec.getMode();
    }
}
