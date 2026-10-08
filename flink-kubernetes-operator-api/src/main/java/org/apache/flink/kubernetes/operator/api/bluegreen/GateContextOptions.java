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

import lombok.Getter;

/** Options values for the GateContext. */
public enum GateContextOptions {
    IS_FIRST_DEPLOYMENT("is-first-deployment"),
    ACTIVE_DEPLOYMENT_TYPE("active-deployment-type"),
    DEPLOYMENT_DELETION_DELAY("deployment-deletion-delay-ms"),
    TRANSITION_STAGE("stage");

    @Getter private final String label;

    private GateContextOptions(String label) {
        this.label = label;
    }
}
