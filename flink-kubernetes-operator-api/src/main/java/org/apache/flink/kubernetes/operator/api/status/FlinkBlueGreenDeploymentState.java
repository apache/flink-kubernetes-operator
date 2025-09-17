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

package org.apache.flink.kubernetes.operator.api.status;

/** Enumeration of the possible states of the blue/green transition. */
public enum FlinkBlueGreenDeploymentState {

    /**
     * We use this state while initializing for the first time, always with a "Blue" deployment
     * type.
     */
    INITIALIZING_BLUE,

    /** Identifies the system is running normally with a "Blue" deployment type. */
    ACTIVE_BLUE,

    /** Identifies the system is running normally with a "Green" deployment type. */
    ACTIVE_GREEN,

    /** Identifies the system is transitioning from "Green" to "Blue". */
    TRANSITIONING_TO_BLUE,

    /** Identifies the system is transitioning from "Blue" to "Green". */
    TRANSITIONING_TO_GREEN,

    /** Identifies the system is savepointing "Blue" before it transitions to "Green". */
    SAVEPOINTING_BLUE,

    /** Identifies the system is savepointing "Green" before it transitions to "Blue". */
    SAVEPOINTING_GREEN,
}
