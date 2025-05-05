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

package org.apache.flink.kubernetes.operator.api.bluegreen;

import org.apache.flink.kubernetes.operator.api.FlinkDeployment;

/**
 * Enumeration of the two possible Flink Blue/Green deployment types. Only one of each type will be
 * present at all times for a particular job.
 */
public enum DeploymentType {
    /** Identifier for the first or "Blue" deployment type. */
    BLUE,

    /** Identifier for the second or "Green" deployment type. */
    GREEN;

    public static DeploymentType fromDeployment(FlinkDeployment flinkDeployment) {
        String typeAnnotation =
                flinkDeployment.getMetadata().getLabels().get(DeploymentType.class.getSimpleName());
        return DeploymentType.valueOf(typeAnnotation);
    }
}
