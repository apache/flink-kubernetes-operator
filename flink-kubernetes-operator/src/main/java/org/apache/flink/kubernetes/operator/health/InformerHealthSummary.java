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

package org.apache.flink.kubernetes.operator.health;

import io.javaoperatorsdk.operator.RuntimeInfo;
import io.javaoperatorsdk.operator.health.InformerHealthIndicator;
import io.javaoperatorsdk.operator.health.Status;
import lombok.Value;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/** Operator informer health summary. */
@Value
public class InformerHealthSummary {

    boolean anyHealthy;
    Set<InformerIdentifier> unhealthyInformers;

    public static InformerHealthSummary fromRuntimeInfo(RuntimeInfo runtimeInfo) {
        var newUnHealthy = new HashSet<InformerIdentifier>();
        boolean anyHealthy = false;

        for (var controllerEntry :
                runtimeInfo.unhealthyInformerWrappingEventSourceHealthIndicator().entrySet()) {
            for (var eventSourceEntry : controllerEntry.getValue().entrySet()) {
                Map<String, InformerHealthIndicator> informers =
                        eventSourceEntry.getValue().informerHealthIndicators();
                for (var informerEntry : informers.entrySet()) {
                    if (informerEntry.getValue().getStatus() == Status.HEALTHY) {
                        anyHealthy = true;
                    } else {
                        newUnHealthy.add(
                                new InformerIdentifier(
                                        controllerEntry.getKey(),
                                        eventSourceEntry.getKey(),
                                        informerEntry.getKey()));
                    }
                }
            }
        }

        return new InformerHealthSummary(anyHealthy || newUnHealthy.isEmpty(), newUnHealthy);
    }
}
