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

import org.apache.flink.shaded.guava31.com.google.common.collect.ImmutableSet;

import io.fabric8.kubernetes.api.model.ContainerStatus;
import io.fabric8.kubernetes.api.model.apps.DeploymentCondition;

import java.util.Optional;
import java.util.Set;

/** Exception to signal terminal deployment failure. */
public class DeploymentFailedException extends RuntimeException {

    public static final Set<String> CONTAINER_ERROR_REASONS =
            ImmutableSet.of(
                    "CrashLoopBackOff",
                    "ImagePullBackOff",
                    "ErrImagePull",
                    "RunContainerError",
                    "CreateContainerConfigError",
                    "OOMKilled");

    private static final long serialVersionUID = -1070179896083579221L;

    private final String reason;

    public DeploymentFailedException(DeploymentCondition deployCondition) {
        super(deployCondition.getMessage());
        this.reason = deployCondition.getReason();
    }

    public DeploymentFailedException(String message, String reason) {
        super(message);
        this.reason = reason;
    }

    public String getReason() {
        return reason;
    }

    public static DeploymentFailedException forContainerStatus(ContainerStatus status) {
        var waiting = status.getState().getWaiting();
        var lastState = status.getLastState();
        String message = null;
        if ("CrashLoopBackOff".equals(waiting.getReason())
                && lastState != null
                && lastState.getTerminated() != null) {
            message =
                    Optional.ofNullable(lastState.getTerminated().getMessage())
                            .map(err -> "CrashLoop - " + err)
                            .orElse(null);
        }

        if (message == null) {
            message = waiting.getMessage();
        }
        return new DeploymentFailedException(
                String.format("[%s] %s", status.getName(), message), waiting.getReason());
    }
}
