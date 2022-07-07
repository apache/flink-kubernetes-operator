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

package org.apache.flink.kubernetes.operator.metrics;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.kubernetes.operator.TestUtils;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.Metric;
import org.apache.flink.runtime.metrics.util.TestingMetricRegistry;

import io.javaoperatorsdk.operator.api.reconciler.RetryInfo;
import io.javaoperatorsdk.operator.processing.event.ResourceID;
import io.javaoperatorsdk.operator.processing.event.source.controller.ResourceAction;
import io.javaoperatorsdk.operator.processing.event.source.controller.ResourceEvent;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

/** {@link OperatorJosdkMetrics} tests. */
public class OperatorJosdkMetricsTest {

    private final ResourceID resourceId = new ResourceID("testns", "testname");
    private final String resourcePrefix =
            "testhost.k8soperator.flink-operator-test.testopname.resource.testname.testns.JOSDK.";
    private final String systemPrefix =
            "testhost.k8soperator.flink-operator-test.testopname.system.";

    private Map<String, Metric> metrics = new HashMap<>();
    private OperatorJosdkMetrics operatorMetrics;

    @BeforeEach
    public void setup() {
        TestingMetricRegistry registry =
                TestingMetricRegistry.builder()
                        .setDelimiter(".".charAt(0))
                        .setRegisterConsumer(
                                (metric, name, group) -> {
                                    metrics.put(group.getMetricIdentifier(name), metric);
                                })
                        .build();
        operatorMetrics =
                new OperatorJosdkMetrics(
                        TestUtils.createTestMetricGroup(registry, new Configuration()),
                        new Configuration());
    }

    @Test
    public void testMetrics() {
        operatorMetrics.failedReconciliation(resourceId, null);
        assertEquals(1, metrics.size());
        assertEquals(1, getCount("Reconciliation.failed"));
        operatorMetrics.failedReconciliation(resourceId, null);
        operatorMetrics.failedReconciliation(resourceId, null);
        assertEquals(1, metrics.size());
        assertEquals(3, getCount("Reconciliation.failed"));

        operatorMetrics.reconcileCustomResource(resourceId, null);
        assertEquals(2, metrics.size());
        assertEquals(1, getCount("Reconciliation"));

        operatorMetrics.reconcileCustomResource(
                resourceId,
                new RetryInfo() {
                    @Override
                    public int getAttemptCount() {
                        return 0;
                    }

                    @Override
                    public boolean isLastAttempt() {
                        return false;
                    }
                });
        assertEquals(3, metrics.size());
        assertEquals(2, getCount("Reconciliation"));
        assertEquals(1, getCount("Reconciliation.retries"));

        operatorMetrics.receivedEvent(new ResourceEvent(ResourceAction.ADDED, resourceId, null));
        assertEquals(5, metrics.size());
        assertEquals(1, getCount("Resource.Event"));
        assertEquals(1, getCount("Resource.Event.ADDED"));

        operatorMetrics.cleanupDoneFor(resourceId);
        assertEquals(6, metrics.size());
        assertEquals(1, getCount("Reconciliation.cleanup"));

        operatorMetrics.finishedReconciliation(resourceId);
        assertEquals(7, metrics.size());
        assertEquals(1, getCount("Reconciliation.finished"));

        operatorMetrics.monitorSizeOf(Map.of("a", "b", "c", "d"), "mymap");
        assertEquals(8, metrics.size());
        assertEquals(2, ((Gauge<Integer>) metrics.get(systemPrefix + "mymap.size")).getValue());
    }

    private long getCount(String name) {
        return ((Counter) metrics.get(resourcePrefix + name + ".Count")).getCount();
    }
}
