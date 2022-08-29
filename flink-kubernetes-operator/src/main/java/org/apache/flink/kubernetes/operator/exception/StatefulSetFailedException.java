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

package org.apache.flink.kubernetes.operator.exception;

import io.fabric8.kubernetes.api.model.ContainerStateWaiting;
import io.fabric8.kubernetes.api.model.apps.StatefulSetCondition;

/** Exception to signal terminal statefulSet failure. */
public class StatefulSetFailedException extends FlinkDeploymentException {

    private static final long serialVersionUID = 4756119502434252719L;

    public StatefulSetFailedException(StatefulSetCondition deployCondition) {
        super(deployCondition.getMessage(), deployCondition.getReason());
    }

    public StatefulSetFailedException(ContainerStateWaiting stateWaiting) {
        super(stateWaiting.getMessage(), stateWaiting.getReason());
    }

    public StatefulSetFailedException(String message, String reason) {
        super(message, reason);
    }
}
