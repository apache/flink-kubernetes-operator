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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Decides whether the operator is healthy. The operator is healthy while it is running, while every
 * canary resource is reconciled in time and while no informer has become unhealthy since the probe
 * was created. Informers that were already unhealthy at that moment are ignored, since the operator
 * has started without them on purpose, which is only possible with {@code
 * kubernetes.operator.startup.stop-on-informer-error} disabled.
 */
class OperatorHealthProbe {

    private static final Logger LOG = LoggerFactory.getLogger(OperatorHealthProbe.class);

    private final RuntimeInfo runtimeInfo;
    private final List<CanaryResourceManager<?>> canaryResourceManagers;

    private InformerHealthSummary previousInformerHealthSummary;

    OperatorHealthProbe(
            RuntimeInfo runtimeInfo, Collection<CanaryResourceManager<?>> canaryResourceManagers) {
        this.runtimeInfo = runtimeInfo;
        this.canaryResourceManagers = List.copyOf(canaryResourceManagers);
        this.previousInformerHealthSummary = summarizeInformers();
        LOG.info(
                "Initially unhealthy informers: {}",
                previousInformerHealthSummary.getUnhealthyInformers());
    }

    boolean isHealthy() {
        var healthSummary = summarizeInformers();
        if (!healthSummary.isAnyHealthy()) {
            LOG.error("All informers are unhealthy");
            return false;
        } else if (anyInformerBecameUnhealthy(healthSummary.getUnhealthyInformers())) {
            return false;
        } else {
            previousInformerHealthSummary = healthSummary;
        }

        if (!runtimeInfo.isStarted()) {
            LOG.error("Operator is not running");
            return false;
        }

        for (CanaryResourceManager<?> canaryResourceManager : canaryResourceManagers) {
            if (!canaryResourceManager.allCanariesHealthy()) {
                LOG.error("Unhealthy canary resources");
                return false;
            }
        }

        return true;
    }

    private boolean anyInformerBecameUnhealthy(Set<InformerIdentifier> unhealthyInformers) {
        boolean unhealthy = false;
        for (InformerIdentifier unhealthyInformer : unhealthyInformers) {
            if (!previousInformerHealthSummary
                    .getUnhealthyInformers()
                    .contains(unhealthyInformer)) {
                LOG.error("Informer became unhealthy: {}", unhealthyInformer);
                unhealthy = true;
            }
        }

        return unhealthy;
    }

    private InformerHealthSummary summarizeInformers() {
        var unhealthyInformers = new HashSet<InformerIdentifier>();
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
                        unhealthyInformers.add(
                                new InformerIdentifier(
                                        controllerEntry.getKey(),
                                        eventSourceEntry.getKey(),
                                        informerEntry.getKey()));
                    }
                }
            }
        }

        return new InformerHealthSummary(
                anyHealthy || unhealthyInformers.isEmpty(), unhealthyInformers);
    }

    @Value
    private static class InformerHealthSummary {
        boolean anyHealthy;
        Set<InformerIdentifier> unhealthyInformers;
    }

    @Value
    private static class InformerIdentifier {
        String controllerName;
        String eventSourceName;
        String informerName;
    }
}
