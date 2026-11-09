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

import io.fabric8.kubernetes.api.model.ContainerState;
import io.fabric8.kubernetes.api.model.ContainerStateRunning;
import io.fabric8.kubernetes.api.model.ContainerStateTerminated;
import io.fabric8.kubernetes.api.model.ContainerStateWaiting;
import io.fabric8.kubernetes.api.model.ContainerStatus;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Tests for {@link DeploymentFailedException}. */
public class DeploymentFailedExceptionTest {

    @Test
    public void testErrorFromContainerStatus() {
        var containerStatus = new ContainerStatus();
        containerStatus.setName("c1");
        var state = new ContainerState();
        var waiting = new ContainerStateWaiting();
        waiting.setMessage("msg");
        waiting.setReason("r");
        state.setWaiting(waiting);

        containerStatus.setState(state);

        // A waiting reason outside the error set is not a failure
        assertTrue(DeploymentFailedException.fromContainerStatus(containerStatus).isEmpty());

        waiting.setReason("ErrImagePull");
        var ex = DeploymentFailedException.fromContainerStatus(containerStatus).orElseThrow();
        assertEquals("[c1] msg", ex.getMessage());
        assertEquals("ErrImagePull", ex.getReason());

        waiting.setReason("CrashLoopBackOff");
        waiting.setMessage("backing off");
        ex = DeploymentFailedException.fromContainerStatus(containerStatus).orElseThrow();
        assertEquals("[c1] backing off", ex.getMessage());
        assertEquals("CrashLoopBackOff", ex.getReason());

        // Last state set but not terminated
        var lastState = new ContainerState();
        containerStatus.setLastState(lastState);

        ex = DeploymentFailedException.fromContainerStatus(containerStatus).orElseThrow();
        assertEquals("[c1] backing off", ex.getMessage());
        assertEquals("CrashLoopBackOff", ex.getReason());

        // Terminated without an exit code: still the kubelet message
        var terminated = new ContainerStateTerminated();
        terminated.setMessage("crash");
        lastState.setTerminated(terminated);

        ex = DeploymentFailedException.fromContainerStatus(containerStatus).orElseThrow();
        assertEquals("[c1] backing off", ex.getMessage());
        assertEquals("CrashLoopBackOff", ex.getReason());

        // A failed termination describes the crash loop instead of the kubelet message
        terminated.setReason("OOMKilled");
        terminated.setExitCode(137);
        ex = DeploymentFailedException.fromContainerStatus(containerStatus).orElseThrow();
        assertEquals(
                "[c1] CrashLoopBackOff, last termination OOMKilled (exit code 137): crash",
                ex.getMessage());
        assertEquals("CrashLoopBackOff", ex.getReason());

        // The same text whatever the kubelet back-off timer says
        waiting.setMessage("back-off 2m40s restarting failed container=c1 pod=other");
        assertEquals(
                "[c1] CrashLoopBackOff, last termination OOMKilled (exit code 137): crash",
                DeploymentFailedException.fromContainerStatus(containerStatus)
                        .orElseThrow()
                        .getMessage());

        // And the same text while the container is running again between two kills
        var running = new ContainerState();
        running.setRunning(new ContainerStateRunning());
        containerStatus.setState(running);
        terminated.setMessage(null);
        ex = DeploymentFailedException.fromContainerStatus(containerStatus).orElseThrow();
        assertEquals(
                "[c1] CrashLoopBackOff, last termination OOMKilled (exit code 137)",
                ex.getMessage());
        assertEquals("CrashLoopBackOff", ex.getReason());

        // Other waiting reasons keep their own message even with a termination on record
        var pulling = new ContainerState();
        var pullWaiting = new ContainerStateWaiting();
        pullWaiting.setReason("ImagePullBackOff");
        pullWaiting.setMessage("pull failed");
        pulling.setWaiting(pullWaiting);
        containerStatus.setState(pulling);
        ex = DeploymentFailedException.fromContainerStatus(containerStatus).orElseThrow();
        assertEquals("[c1] pull failed", ex.getMessage());
        assertEquals("ImagePullBackOff", ex.getReason());
    }

    @Test
    public void testFailedTermination() {
        var containerStatus = new ContainerStatus();
        containerStatus.setName("c1");
        containerStatus.setState(new ContainerState());
        assertTrue(DeploymentFailedException.failedTermination(containerStatus).isEmpty());

        // Restarted after a clean exit: not a failure
        var lastState = new ContainerState();
        var completed = new ContainerStateTerminated();
        completed.setExitCode(0);
        lastState.setTerminated(completed);
        containerStatus.setLastState(lastState);
        assertTrue(DeploymentFailedException.failedTermination(containerStatus).isEmpty());

        // Restarted after a kill
        completed.setExitCode(137);
        completed.setReason("OOMKilled");
        assertEquals(
                "OOMKilled",
                DeploymentFailedException.failedTermination(containerStatus).get().getReason());

        // Terminated right now, before the restart
        var state = new ContainerState();
        var current = new ContainerStateTerminated();
        current.setExitCode(1);
        current.setReason("Error");
        state.setTerminated(current);
        containerStatus.setState(state);
        assertEquals(
                "Error",
                DeploymentFailedException.failedTermination(containerStatus).get().getReason());
    }
}
