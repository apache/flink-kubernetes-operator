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

package org.apache.flink.kubernetes.operator;

import org.apache.flink.kubernetes.operator.crd.FlinkDeployment;
import org.apache.flink.kubernetes.operator.crd.spec.KubernetesDeploymentMode;
import org.apache.flink.kubernetes.operator.service.FlinkService;
import org.apache.flink.kubernetes.operator.service.FlinkServiceFactory;

/** Flink service factory mock for tests. */
public class TestingFlinkServiceFactory extends FlinkServiceFactory {
    private final FlinkService flinkService;

    public TestingFlinkServiceFactory(FlinkService flinkService) {
        super(null, null);
        this.flinkService = flinkService;
    }

    public FlinkService getOrCreate(KubernetesDeploymentMode deploymentMode) {
        return flinkService;
    }

    public FlinkService getOrCreate(FlinkDeployment deployment) {
        return flinkService;
    }
}
