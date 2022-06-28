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

import org.apache.flink.kubernetes.operator.crd.FlinkDeployment;
import org.apache.flink.kubernetes.operator.exception.ReconciliationException;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.testutils.MetricListener;

import io.fabric8.kubernetes.api.model.ObjectMeta;
import io.javaoperatorsdk.operator.api.monitoring.Metrics;
import io.javaoperatorsdk.operator.processing.event.ResourceID;
import io.javaoperatorsdk.operator.processing.event.source.controller.ResourceAction;
import io.javaoperatorsdk.operator.processing.event.source.controller.ResourceEvent;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static org.apache.flink.kubernetes.operator.TestUtils.buildApplicationCluster;
import static org.apache.flink.kubernetes.operator.TestUtils.buildSessionCluster;
import static org.apache.flink.kubernetes.operator.metrics.FlinkOperatorMetrics.COUNT;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/** {@link FlinkOperatorMetrics} tests. */
public class FlinkOperatorMetricsTest {

    private MetricListener metricListener;
    private FlinkOperatorMetrics operatorMetrics;

    private static final String COUNTER = String.join(".", "counter", "events.received");
    private static final String EVENT =
            String.join(".", "event", ResourceEvent.class.getSimpleName());

    @BeforeEach
    public void setup() {
        metricListener = new MetricListener();
        operatorMetrics = new FlinkOperatorMetrics(metricListener.getMetricGroup());
    }

    @Test
    public void testTimeControllerExecution() throws Exception {
        String execution =
                String.join(".", "controller", "TestController", "execution", "reconcile");
        String executionMetric = String.join(".", execution, COUNT);
        String successMetric = String.join(".", execution, "success", COUNT);
        String successTypeMetric = String.join(".", execution, "success", "resource", COUNT);
        String failureMetric = String.join(".", execution, "failure", COUNT);
        String failureExceptionMetric =
                String.join(
                        ".",
                        execution,
                        "failure",
                        ReconciliationException.class.getSimpleName(),
                        COUNT);
        assertFalse(counter(executionMetric).isPresent());
        assertFalse(counter(successMetric).isPresent());
        assertFalse(counter(successTypeMetric).isPresent());
        assertFalse(counter(failureMetric).isPresent());
        assertFalse(counter(failureExceptionMetric).isPresent());
        FlinkDeployment flinkDeployment = buildApplicationCluster();
        Metrics.ControllerExecution<FlinkDeployment> successExecution =
                new Metrics.ControllerExecution<>() {

                    @Override
                    public String name() {
                        return "reconcile";
                    }

                    @Override
                    public String controllerName() {
                        return "TestController";
                    }

                    @Override
                    public String successTypeName(FlinkDeployment flinkDeployment) {
                        return "resource";
                    }

                    @Override
                    public FlinkDeployment execute() throws Exception {
                        return flinkDeployment;
                    }
                };
        operatorMetrics.timeControllerExecution(successExecution);
        assertEquals(1, counter(executionMetric).get().getCount());
        assertEquals(1, counter(successMetric).get().getCount());
        assertEquals(1, counter(successTypeMetric).get().getCount());
        operatorMetrics.timeControllerExecution(successExecution);
        assertEquals(2, counter(executionMetric).get().getCount());
        assertEquals(2, counter(successMetric).get().getCount());
        assertEquals(2, counter(successTypeMetric).get().getCount());
        try {
            operatorMetrics.timeControllerExecution(
                    new Metrics.ControllerExecution<FlinkDeployment>() {

                        @Override
                        public String name() {
                            return "reconcile";
                        }

                        @Override
                        public String controllerName() {
                            return "TestController";
                        }

                        @Override
                        public String successTypeName(FlinkDeployment flinkDeployment) {
                            return "resource";
                        }

                        @Override
                        public FlinkDeployment execute() throws Exception {
                            throw new ReconciliationException(new RuntimeException());
                        }
                    });
            fail("Failed to test timeControllerExecution with ReconciliationException.");
        } catch (Exception exception) {
            assertTrue(exception instanceof ReconciliationException);
            assertEquals(3, counter(executionMetric).get().getCount());
            assertEquals(1, counter(failureMetric).get().getCount());
            assertEquals(1, counter(failureExceptionMetric).get().getCount());
        }
    }

    @Test
    public void testReceivedEvent() {
        FlinkDeployment flinkDeployment = buildSessionCluster();
        String counterMetric = String.join(".", COUNTER, COUNT);
        String eventMetric = String.join(".", EVENT, COUNT);
        String eventCounterMetric = String.join(".", EVENT, counterMetric);
        assertFalse(counter(counterMetric).isPresent());
        assertFalse(counter(eventMetric).isPresent());
        assertFalse(counter(eventCounterMetric).isPresent());
        verifyCounter(flinkDeployment);
        assertEquals(1, counter(counterMetric).get().getCount());
        assertEquals(1, counter(eventMetric).get().getCount());
        assertEquals(1, counter(eventCounterMetric).get().getCount());
        flinkDeployment = buildApplicationCluster();
        ObjectMeta metaData = flinkDeployment.getMetadata();
        metaData.setName("test" + flinkDeployment.getMetadata().getName());
        metaData.setNamespace("test" + flinkDeployment.getMetadata().getNamespace());
        verifyCounter(flinkDeployment);
        assertEquals(2, counter(counterMetric).get().getCount());
        assertEquals(2, counter(eventMetric).get().getCount());
        assertEquals(2, counter(eventCounterMetric).get().getCount());
    }

    private void verifyCounter(FlinkDeployment flinkDeployment) {
        ResourceID resourceID = ResourceID.fromResource(flinkDeployment);
        String resource = String.join(".", "resource", resourceID.getName());
        String namespace = String.join(".", "namespace", resourceID.getNamespace().orElse(""));
        String scope =
                String.join(
                        ".",
                        "scope",
                        resourceID.getNamespace().isPresent() ? "namespace" : "cluster");
        String counterResourceMetric = String.join(".", COUNTER, resource, COUNT);
        String counterNamespaceMetric = String.join(".", COUNTER, resource, namespace, COUNT);
        String counterScopeMetric = String.join(".", COUNTER, resource, namespace, scope, COUNT);
        String eventResourceMetric = String.join(".", EVENT, counterResourceMetric);
        String eventNamespaceMetric = String.join(".", EVENT, counterNamespaceMetric);
        String eventScopeMetric = String.join(".", EVENT, counterScopeMetric);
        assertFalse(counter(counterResourceMetric).isPresent());
        assertFalse(counter(counterNamespaceMetric).isPresent());
        assertFalse(counter(counterScopeMetric).isPresent());
        assertFalse(counter(eventResourceMetric).isPresent());
        assertFalse(counter(eventNamespaceMetric).isPresent());
        assertFalse(counter(eventScopeMetric).isPresent());
        ResourceEvent resourceEvent =
                new ResourceEvent(ResourceAction.ADDED, resourceID, flinkDeployment);
        operatorMetrics.receivedEvent(resourceEvent);
        assertEquals(1, counter(counterResourceMetric).get().getCount());
        assertEquals(1, counter(counterNamespaceMetric).get().getCount());
        assertEquals(1, counter(counterScopeMetric).get().getCount());
        assertEquals(1, counter(eventResourceMetric).get().getCount());
        assertEquals(1, counter(eventNamespaceMetric).get().getCount());
        assertEquals(1, counter(eventScopeMetric).get().getCount());
    }

    private Optional<Counter> counter(String metricName) {
        return metricListener.getCounter(metricName);
    }
}
