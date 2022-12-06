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
import io.fabric8.kubernetes.api.model.apps.DeploymentCondition;

/** Exception to signal terminal deployment failure. */
public class DeploymentFailedException extends RuntimeException {

    public static final String REASON_CRASH_LOOP_BACKOFF = "CrashLoopBackOff";
    public static final String REASON_IMAGE_PULL_BACKOFF = "ImagePullBackOff";
    public static final String REASON_ERR_IMAGE_PULL = "ErrImagePull";

    private static final long serialVersionUID = -1070179896083579221L;

    private final String reason;

    public DeploymentFailedException(DeploymentCondition deployCondition) {
        super(deployCondition.getMessage());
        this.reason = deployCondition.getReason();
    }

    public DeploymentFailedException(ContainerStateWaiting stateWaiting) {
        super(stateWaiting.getMessage());
        this.reason = stateWaiting.getReason();
    }

    public DeploymentFailedException(String message, String reason) {
        super(message);
        this.reason = reason;
    }

    public String getReason() {
        return reason;
    }
}
