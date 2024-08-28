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
import org.apache.flink.kubernetes.operator.api.AbstractFlinkResource;
import org.apache.flink.kubernetes.operator.api.CrdConstants;
import org.apache.flink.kubernetes.operator.api.FlinkDeployment;
import org.apache.flink.kubernetes.operator.api.FlinkSessionJob;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/** Flink resource reference that can be a FlinkDeployment or FlinkSessionJob. */
@Experimental
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
@JsonIgnoreProperties(ignoreUnknown = true)
public class JobReference {

    /** Kind of the Flink resource, FlinkDeployment or FlinkSessionJob. */
    private JobKind kind;

    /** Name of the Flink resource. */
    private String name;

    public static JobReference fromFlinkResource(AbstractFlinkResource<?, ?> flinkResource) {
        var result = new JobReference();
        result.setName(flinkResource.getMetadata().getName());

        if (flinkResource instanceof FlinkDeployment) {
            result.setKind(JobKind.FLINK_DEPLOYMENT);
        } else if (flinkResource instanceof FlinkSessionJob) {
            result.setKind(JobKind.FLINK_SESSION_JOB);
        }

        return result;
    }

    public String toString() {
        String kindString = kind.name();
        if (kind == JobKind.FLINK_DEPLOYMENT) {
            kindString = CrdConstants.KIND_FLINK_DEPLOYMENT;
        } else if (kind == JobKind.FLINK_SESSION_JOB) {
            kindString = CrdConstants.KIND_SESSION_JOB;
        }
        return String.format("%s (%s)", name, kindString);
    }
}
