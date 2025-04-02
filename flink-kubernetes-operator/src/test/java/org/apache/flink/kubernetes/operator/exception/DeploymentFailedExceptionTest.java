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
import io.fabric8.kubernetes.api.model.ContainerStateTerminated;
import io.fabric8.kubernetes.api.model.ContainerStateWaiting;
import io.fabric8.kubernetes.api.model.ContainerStatus;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

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

        var ex = DeploymentFailedException.forContainerStatus(containerStatus);
        assertEquals("[c1] msg", ex.getMessage());
        assertEquals("r", ex.getReason());

        waiting.setReason("CrashLoopBackOff");
        waiting.setMessage("backing off");
        ex = DeploymentFailedException.forContainerStatus(containerStatus);
        assertEquals("[c1] backing off", ex.getMessage());
        assertEquals("CrashLoopBackOff", ex.getReason());

        // Last state set but not terminated
        var lastState = new ContainerState();
        containerStatus.setLastState(lastState);

        ex = DeploymentFailedException.forContainerStatus(containerStatus);
        assertEquals("[c1] backing off", ex.getMessage());
        assertEquals("CrashLoopBackOff", ex.getReason());

        var terminated = new ContainerStateTerminated();
        terminated.setMessage("crash");
        lastState.setTerminated(terminated);

        ex = DeploymentFailedException.forContainerStatus(containerStatus);
        assertEquals("[c1] CrashLoop - crash", ex.getMessage());
        assertEquals("CrashLoopBackOff", ex.getReason());
    }
}
