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

import org.apache.flink.kubernetes.operator.config.FlinkConfigManager;
import org.apache.flink.kubernetes.operator.config.KubernetesOperatorConfigOptions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Health probe service. */
public class OperatorHealthService {
    private static final Logger LOG = LoggerFactory.getLogger(OperatorHealthService.class);

    private final HealthProbe probe = HealthProbe.INSTANCE;
    private final FlinkConfigManager configManager;
    private HttpBootstrap httpBootstrap = null;

    public OperatorHealthService(FlinkConfigManager configManager) {
        this.configManager = configManager;
    }

    public void start() {
        // Start the service for the http endpoint
        try {
            httpBootstrap =
                    new HttpBootstrap(
                            probe,
                            configManager
                                    .getDefaultConfig()
                                    .get(
                                            KubernetesOperatorConfigOptions
                                                    .OPERATOR_HEALTH_PROBE_PORT));
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    public void stop() {
        httpBootstrap.stop();
    }

    public static OperatorHealthService fromConfig(FlinkConfigManager configManager) {
        var defaultConfig = configManager.getDefaultConfig();
        if (defaultConfig.getBoolean(
                KubernetesOperatorConfigOptions.OPERATOR_HEALTH_PROBE_ENABLED)) {
            return new OperatorHealthService(configManager);
        } else {
            LOG.info("Health probe disabled");
            return null;
        }
    }
}
