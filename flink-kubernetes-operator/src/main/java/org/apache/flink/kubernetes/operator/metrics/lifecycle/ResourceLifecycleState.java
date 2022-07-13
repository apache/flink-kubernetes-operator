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

package org.apache.flink.kubernetes.operator.metrics.lifecycle;

import java.util.Collections;
import java.util.EnumSet;
import java.util.Set;

/** Enum encapsulating the lifecycle state of a Flink resource. */
public enum ResourceLifecycleState {
    CREATED(false),
    SUSPENDED(true),
    UPGRADING(false),
    DEPLOYED(false),
    STABLE(true),
    ROLLING_BACK(false),
    ROLLED_BACK(true),
    FAILED(true);

    private final boolean terminal;

    ResourceLifecycleState(boolean terminal) {
        this.terminal = terminal;
    }

    public Set<ResourceLifecycleState> getClearedStatesAfterTransition(
            ResourceLifecycleState transitionFrom) {
        if (this == transitionFrom) {
            return Collections.emptySet();
        }
        var states = EnumSet.allOf(ResourceLifecycleState.class);
        if (terminal) {
            states.remove(this);
            return states;
        }

        if (this == UPGRADING) {
            states.remove(UPGRADING);
            states.remove(transitionFrom);
            return states;
        }

        return Collections.emptySet();
    }
}
