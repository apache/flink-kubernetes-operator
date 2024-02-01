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
import io.fabric8.crd.generator.annotation.SchemaFrom;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.PodTemplateSpec;
import io.fabric8.kubernetes.model.annotation.SpecReplicas;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/** TaskManager spec. */
@Experimental
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
@JsonIgnoreProperties(ignoreUnknown = true)
public class TaskManagerSpec implements Diffable<TaskManagerSpec> {
    /** Resource specification for the TaskManager pods. */
    private Resource resource;

    /** Number of TaskManager replicas. If defined, takes precedence over parallelism */
    @SpecDiff(value = DiffType.SCALE, mode = KubernetesDeploymentMode.STANDALONE)
    @SpecReplicas
    private Integer replicas;

    /** TaskManager pod template. It will be merged with FlinkDeploymentSpec.podTemplate. */
    @SchemaFrom(type = Pod.class)
    private PodTemplateSpec podTemplate;
}
