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

import org.apache.flink.configuration.Configuration;
import org.apache.flink.kubernetes.operator.config.FlinkOperatorConfiguration;
import org.apache.flink.kubernetes.operator.config.Mode;
import org.apache.flink.kubernetes.operator.crd.FlinkDeployment;
import org.apache.flink.kubernetes.operator.observer.Observer;
import org.apache.flink.kubernetes.operator.service.FlinkService;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/** The factory to create the observer based ob the {@link FlinkDeployment} mode. */
public class ObserverFactory {

    private final FlinkService flinkService;
    private final FlinkOperatorConfiguration operatorConfig;
    private final Configuration flinkConfig;
    private final Map<Mode, Observer<FlinkDeployment>> observerMap;

    public ObserverFactory(
            FlinkService flinkService,
            FlinkOperatorConfiguration operatorConfiguration,
            Configuration flinkConfig) {
        this.flinkService = flinkService;
        this.operatorConfig = operatorConfiguration;
        this.flinkConfig = flinkConfig;
        this.observerMap = new ConcurrentHashMap<>();
    }

    public Observer<FlinkDeployment> getOrCreate(FlinkDeployment flinkApp) {
        return observerMap.computeIfAbsent(
                Mode.getMode(flinkApp),
                mode -> {
                    switch (mode) {
                        case SESSION:
                            return new SessionObserver(flinkService, operatorConfig, flinkConfig);
                        case APPLICATION:
                            return new ApplicationObserver(
                                    flinkService, operatorConfig, flinkConfig);
                        default:
                            throw new UnsupportedOperationException(
                                    String.format("Unsupported running mode: %s", mode));
                    }
                });
    }
}
