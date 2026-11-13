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

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.util.TimeUtils;

import io.javaoperatorsdk.operator.RegisteredController;
import io.javaoperatorsdk.operator.RuntimeInfo;
import io.javaoperatorsdk.operator.health.InformerHealthIndicator;
import io.javaoperatorsdk.operator.health.InformerWrappingEventSourceHealthIndicator;
import io.javaoperatorsdk.operator.health.Status;
import io.javaoperatorsdk.operator.processing.event.source.EventSource;
import lombok.Value;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.lang.reflect.Method;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.Collection;
import java.util.HashMap;
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
 *
 * <p>An informer whose resource version has not moved for longer than the configured timeout counts
 * as unhealthy too, because a working watch moves it with every event and with the bookmarks the
 * API server sends about once a minute.
 */
class OperatorHealthProbe {

    private static final Logger LOG = LoggerFactory.getLogger(OperatorHealthProbe.class);

    // TODO: Temporary until the getLastSyncResourceVersion method is exposed on the
    //  InformerHealthIndicator interface.
    private static final String INFORMER_WRAPPER =
            "io.javaoperatorsdk.operator.processing.event.source.informer.InformerWrapper";

    @Nullable
    private static final Method LAST_SYNC_RESOURCE_VERSION = lookupLastSyncResourceVersion();

    private final RuntimeInfo runtimeInfo;
    private final List<CanaryResourceManager<?>> canaryResourceManagers;
    private final Duration staleTimeout;
    private final Clock clock;

    private InformerHealthSummary previousInformerHealthSummary;
    private final Map<InformerIdentifier, LastResourceVersion> lastResourceVersions =
            new HashMap<>();
    private final Set<InformerIdentifier> staleInformers = new HashSet<>();

    OperatorHealthProbe(
            RuntimeInfo runtimeInfo,
            Collection<CanaryResourceManager<?>> canaryResourceManagers,
            Duration staleTimeout) {
        this(runtimeInfo, canaryResourceManagers, staleTimeout, Clock.systemUTC());
    }

    @VisibleForTesting
    OperatorHealthProbe(
            RuntimeInfo runtimeInfo,
            Collection<CanaryResourceManager<?>> canaryResourceManagers,
            Duration staleTimeout,
            Clock clock) {
        this.runtimeInfo = runtimeInfo;
        this.canaryResourceManagers = List.copyOf(canaryResourceManagers);
        this.staleTimeout = staleTimeout;
        this.clock = clock;
        initialHealthCheck();
    }

    // The first evaluation is remembered instead of answered: the informers that are unhealthy now
    // stay tolerated, and the timing of the others starts here
    private void initialHealthCheck() {
        previousInformerHealthSummary = summarizeInformers();
        LOG.info(
                "Initially unhealthy informers: {}",
                previousInformerHealthSummary.getUnhealthyInformers());
        if (staleCheckEnabled()) {
            anyInformerStale(previousInformerHealthSummary.getResourceVersions());
            LOG.info(
                    "Informers are reported stale after {} without a watch event or bookmark",
                    staleTimeout);
        } else {
            LOG.info("Informer staleness check disabled");
        }
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

        if (staleCheckEnabled() && anyInformerStale(healthSummary.getResourceVersions())) {
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

    private boolean staleCheckEnabled() {
        return staleTimeout.compareTo(Duration.ZERO) > 0;
    }

    private boolean anyInformerStale(Map<InformerIdentifier, String> resourceVersions) {
        lastResourceVersions.keySet().retainAll(resourceVersions.keySet());
        staleInformers.retainAll(resourceVersions.keySet());
        var now = clock.instant();
        resourceVersions.forEach(
                (informer, resourceVersion) -> {
                    var last = lastResourceVersions.get(informer);
                    if (last == null) {
                        lastResourceVersions.put(
                                informer, new LastResourceVersion(resourceVersion, now));
                        return;
                    }
                    if (!last.getResourceVersion().equals(resourceVersion)) {
                        lastResourceVersions.put(
                                informer, new LastResourceVersion(resourceVersion, now));
                        if (staleInformers.remove(informer)) {
                            LOG.info(
                                    "Informer {} receives watch events again after {}, resourceVersion {}",
                                    informer,
                                    formatAge(Duration.between(last.getSince(), now)),
                                    resourceVersion);
                        }
                        return;
                    }
                    var age = Duration.between(last.getSince(), now);
                    if (age.compareTo(staleTimeout) > 0 && staleInformers.add(informer)) {
                        LOG.error(
                                "Informer {} received no watch event or bookmark for {}, resourceVersion {} unchanged",
                                informer,
                                formatAge(age),
                                resourceVersion);
                    }
                });
        return !staleInformers.isEmpty();
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

    @SuppressWarnings({"rawtypes", "unchecked"})
    private InformerHealthSummary summarizeInformers() {
        var unhealthyInformers = new HashSet<InformerIdentifier>();
        var resourceVersions = new HashMap<InformerIdentifier, String>();
        boolean anyHealthy = false;

        for (RegisteredController<?> controller : runtimeInfo.getRegisteredControllers()) {
            var controllerName = controller.getConfiguration().getName();
            Map<String, InformerWrappingEventSourceHealthIndicator> eventSources =
                    controller.getControllerHealthInfo().informerEventSourceHealthIndicators();
            for (var eventSourceEntry : eventSources.entrySet()) {
                var eventSource = eventSourceEntry.getValue();
                var resourceType =
                        eventSource instanceof EventSource<?, ?> source
                                ? source.resourceType().getSimpleName()
                                : eventSourceEntry.getKey();
                // Only the informers of unhealthy event sources count for the health summary, the
                // same ones RuntimeInfo.unhealthyInformerWrappingEventSourceHealthIndicator()
                // returns.
                var countsForHealth = eventSource.getStatus() == Status.UNHEALTHY;
                Map<String, InformerHealthIndicator> informers =
                        eventSource.informerHealthIndicators();
                for (var informerEntry : informers.entrySet()) {
                    var informer =
                            new InformerIdentifier(
                                    controllerName,
                                    eventSourceEntry.getKey(),
                                    resourceType,
                                    informerEntry.getKey());
                    if (countsForHealth) {
                        if (informerEntry.getValue().getStatus() == Status.HEALTHY) {
                            anyHealthy = true;
                        } else {
                            unhealthyInformers.add(informer);
                        }
                    }
                    var resourceVersion = resourceVersion(informerEntry.getValue());
                    if (resourceVersion != null) {
                        resourceVersions.put(informer, resourceVersion);
                    }
                }
            }
        }

        return new InformerHealthSummary(
                anyHealthy || unhealthyInformers.isEmpty(), unhealthyInformers, resourceVersions);
    }

    // TODO: Read the version directly once JOSDK exposes it on InformerHealthIndicator. Today it
    //  lives on the package-private informer wrapper, which is the indicator object itself. Null
    //  while the informer has not listed yet or when the wrapper is not readable, in which case the
    //  informer is simply not tracked.
    @Nullable
    String resourceVersion(InformerHealthIndicator informer) {
        if (LAST_SYNC_RESOURCE_VERSION == null
                || !LAST_SYNC_RESOURCE_VERSION.getDeclaringClass().isInstance(informer)) {
            return null;
        }
        try {
            return (String) LAST_SYNC_RESOURCE_VERSION.invoke(informer);
        } catch (ReflectiveOperationException e) {
            LOG.debug("Could not read the resource version of {}", informer, e);
            return null;
        }
    }

    // TODO: Remove together with the reflective read above once JOSDK exposes the
    //  getLastSyncResourceVersion method on InformerHealthIndicator interface.
    @Nullable
    private static Method lookupLastSyncResourceVersion() {
        try {
            var method = Class.forName(INFORMER_WRAPPER).getMethod("getLastSyncResourceVersion");
            method.setAccessible(true);
            return method;
        } catch (ReflectiveOperationException | RuntimeException e) {
            LOG.warn(
                    "Cannot read informer resource versions from this JOSDK version, the informer staleness check is disabled",
                    e);
            return null;
        }
    }

    private static String formatAge(Duration age) {
        return TimeUtils.formatWithHighestUnit(Duration.ofSeconds(age.toSeconds()));
    }

    @Value
    private static class InformerHealthSummary {
        boolean anyHealthy;
        Set<InformerIdentifier> unhealthyInformers;
        Map<InformerIdentifier, String> resourceVersions;
    }

    @Value
    private static class InformerIdentifier {
        String controllerName;
        String eventSourceName;
        String resourceType;
        String namespace;

        @Override
        public String toString() {
            return controllerName + "/" + resourceType + "/" + namespace;
        }
    }

    @Value
    private static class LastResourceVersion {
        String resourceVersion;
        Instant since;
    }
}
