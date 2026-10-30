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

/** Metrics the gates report and the operator reads during a transition. */
public final class GateMetrics {

    /**
     * Gauge each gate subtask reports: 1 once the standby has nothing left to emit for the
     * hand-over, 0 before. The operator tears the standby down only when every subtask reports 1.
     */
    public static final String HAND_OVER_DONE = "bluegreenGateHandOverDone";

    private GateMetrics() {}
}
