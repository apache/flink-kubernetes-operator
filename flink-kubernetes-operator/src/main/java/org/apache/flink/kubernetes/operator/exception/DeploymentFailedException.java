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

import com.google.common.collect.ImmutableSet;
import io.fabric8.kubernetes.api.model.ContainerStateTerminated;
import io.fabric8.kubernetes.api.model.ContainerStatus;
import io.fabric8.kubernetes.api.model.apps.DeploymentCondition;

import java.util.Optional;
import java.util.Set;

/**
 * Signals that the JobManager deployment failed in a way that needs intervention.
 *
 * <p>Raised for a Deployment condition such as {@code FailedCreate}, and for a container that
 * either waits with one of the {@link #CONTAINER_ERROR_REASONS} or has a {@link
 * #failedTermination}.
 *
 * <p>Kubernetes records the reason of a kill ({@code OOMKilled}, {@code Error}) only in the
 * terminated state, never in the waiting one. The termination is therefore what tells a crash loop
 * apart from a slow start, in every phase of the loop.
 *
 * <p>A crash loop is described from its termination (reason, exit code, container message), so the
 * text stays the same across back-off steps and pod replacements and events are deduplicated. The
 * other waiting reasons keep the kubelet message.
 */
public class DeploymentFailedException extends RuntimeException {

    public static final String CRASH_LOOP_BACK_OFF = "CrashLoopBackOff";
    public static final Set<String> CONTAINER_ERROR_REASONS =
            ImmutableSet.of(
                    CRASH_LOOP_BACK_OFF,
                    "ImagePullBackOff",
                    "ErrImagePull",
                    "RunContainerError",
                    "CreateContainerConfigError");

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

    public static Optional<ContainerStateTerminated> failedTermination(ContainerStatus status) {
        var terminated = status.getState() != null ? status.getState().getTerminated() : null;
        if (terminated == null && status.getLastState() != null) {
            terminated = status.getLastState().getTerminated();
        }
        return Optional.ofNullable(terminated)
                .filter(t -> t.getExitCode() != null && t.getExitCode() != 0);
    }

    public static Optional<DeploymentFailedException> fromContainerStatus(ContainerStatus status) {
        var waiting = status.getState() != null ? status.getState().getWaiting() : null;
        var termination = failedTermination(status);
        if (termination.isPresent()
                && (waiting == null || CRASH_LOOP_BACK_OFF.equals(waiting.getReason()))) {
            var terminated = termination.get();
            var message =
                    String.format(
                            "%s, last termination %s (exit code %d)",
                            CRASH_LOOP_BACK_OFF,
                            terminated.getReason() != null
                                    ? terminated.getReason()
                                    : "with unknown reason",
                            terminated.getExitCode());
            if (terminated.getMessage() != null && !terminated.getMessage().isBlank()) {
                message += ": " + terminated.getMessage().trim();
            }
            return Optional.of(
                    new DeploymentFailedException(
                            String.format("[%s] %s", status.getName(), message),
                            CRASH_LOOP_BACK_OFF));
        }
        if (waiting != null && CONTAINER_ERROR_REASONS.contains(waiting.getReason())) {
            return Optional.of(
                    new DeploymentFailedException(
                            String.format("[%s] %s", status.getName(), waiting.getMessage()),
                            waiting.getReason()));
        }
        return Optional.empty();
    }
}
