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
import lombok.Getter;
import lombok.Setter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

/** Flink operator health probe. */
public enum HealthProbe {
    INSTANCE;

    private static final Logger LOG = LoggerFactory.getLogger(HealthProbe.class);

    private final AtomicBoolean isHealthy = new AtomicBoolean(true);

    @Setter @Getter private RuntimeInfo runtimeInfo;

    private final List<CanaryResourceManager<?>> canaryResourceManagers = new ArrayList<>();

    public void registerCanaryResourceManager(CanaryResourceManager<?> canaryResourceManager) {
        canaryResourceManagers.add(canaryResourceManager);
    }

    public boolean isHealthy() {
        if (!isHealthy.get()) {
            return false;
        }

        if (runtimeInfo != null) {
            LOG.debug("Checking operator health");
            if (!runtimeInfo.allEventSourcesAreHealthy()) {
                LOG.error("Unhealthy event sources: {}", runtimeInfo.unhealthyEventSources());
                return false;
            }

            if (!runtimeInfo.isStarted()) {
                LOG.error("Operator not running");
                return false;
            }
        }

        for (CanaryResourceManager<?> canaryResourceManager : canaryResourceManagers) {
            if (!canaryResourceManager.allCanariesHealthy()) {
                LOG.error("Unhealthy canary resources");
                return false;
            }
        }
        return true;
    }
}
