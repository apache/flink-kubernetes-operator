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

package org.apache.flink.kubernetes.operator.crd.spec;

import io.fabric8.kubernetes.api.model.Pod;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Map;

/** Spec that describes a Flink application deployment. */
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class FlinkDeploymentSpec {
    private String image;
    private String imagePullPolicy;
    private String serviceAccount;
    private String flinkVersion;
    private String ingressDomain;
    private Map<String, String> flinkConfiguration;
    private Pod podTemplate;
    private JobManagerSpec jobManager;
    private TaskManagerSpec taskManager;
    private JobSpec job;
    private Map<String, String> logging;
}
