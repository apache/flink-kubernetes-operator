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

package org.apache.flink.kubernetes.operator.observer.deployment;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.kubernetes.operator.api.FlinkDeployment;
import org.apache.flink.kubernetes.operator.api.spec.KubernetesDeploymentMode;
import org.apache.flink.kubernetes.operator.config.Mode;
import org.apache.flink.kubernetes.operator.observer.Observer;
import org.apache.flink.kubernetes.operator.utils.EventRecorder;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/** The factory to create the observer based on the {@link FlinkDeployment} mode. */
public class FlinkDeploymentObserverFactory {

    private final EventRecorder eventRecorder;
    private final Map<Tuple2<Mode, KubernetesDeploymentMode>, Observer<FlinkDeployment>>
            observerMap;

    public FlinkDeploymentObserverFactory(EventRecorder eventRecorder) {
        this.eventRecorder = eventRecorder;
        this.observerMap = new ConcurrentHashMap<>();
    }

    public Observer<FlinkDeployment> getOrCreate(FlinkDeployment flinkApp) {
        return observerMap.computeIfAbsent(
                Tuple2.of(
                        Mode.getMode(flinkApp),
                        KubernetesDeploymentMode.getDeploymentMode(flinkApp)),
                modes -> {
                    switch (modes.f0) {
                        case SESSION:
                            return new SessionObserver(eventRecorder);
                        case APPLICATION:
                            return new ApplicationObserver(eventRecorder);
                        default:
                            throw new UnsupportedOperationException(
                                    String.format("Unsupported running mode: %s", modes.f0));
                    }
                });
    }
}
