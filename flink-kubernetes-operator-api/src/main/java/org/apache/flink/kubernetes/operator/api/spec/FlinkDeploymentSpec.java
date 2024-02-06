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

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import io.fabric8.crd.generator.annotation.SchemaFrom;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.PodTemplateSpec;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.experimental.SuperBuilder;

import java.util.Map;

/** Spec that describes a Flink application or session cluster deployment. */
@Experimental
@Data
@NoArgsConstructor
@AllArgsConstructor
@EqualsAndHashCode(callSuper = true)
@ToString(callSuper = true)
@SuperBuilder
@JsonIgnoreProperties(ignoreUnknown = true)
public class FlinkDeploymentSpec extends AbstractFlinkSpec {
    /** Flink docker image used to start the Job and TaskManager pods. */
    private String image;

    /** Image pull policy of the Flink docker image. */
    private String imagePullPolicy;

    /** Kubernetes service used by the Flink deployment. */
    private String serviceAccount;

    /** Flink image version. */
    private FlinkVersion flinkVersion;

    /** Ingress specs. */
    private IngressSpec ingress;

    /**
     * Base pod template for job and task manager pods. Can be overridden by the jobManager and
     * taskManager pod templates.
     */
    @SchemaFrom(type = Pod.class)
    private PodTemplateSpec podTemplate;

    /** JobManager specs. */
    private JobManagerSpec jobManager;

    /** TaskManager specs. */
    private TaskManagerSpec taskManager;

    /**
     * Log configuration overrides for the Flink deployment. Format logConfigFileName ->
     * configContent.
     */
    private Map<String, String> logConfiguration;

    /** Deployment mode of the Flink cluster, native or standalone. */
    private KubernetesDeploymentMode mode;
}
