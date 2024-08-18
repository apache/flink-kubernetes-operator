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

package org.apache.flink.kubernetes.operator.metrics.lifecycle;

import org.apache.flink.api.common.JobStatus;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.kubernetes.operator.TestUtils;
import org.apache.flink.kubernetes.operator.api.AbstractFlinkResource;
import org.apache.flink.kubernetes.operator.api.FlinkDeployment;
import org.apache.flink.kubernetes.operator.api.FlinkSessionJob;
import org.apache.flink.kubernetes.operator.api.lifecycle.ResourceLifecycleState;
import org.apache.flink.kubernetes.operator.api.spec.JobState;
import org.apache.flink.kubernetes.operator.api.status.ReconciliationState;
import org.apache.flink.kubernetes.operator.config.FlinkOperatorConfiguration;
import org.apache.flink.kubernetes.operator.metrics.CustomResourceMetrics;
import org.apache.flink.kubernetes.operator.metrics.KubernetesOperatorMetricOptions;
import org.apache.flink.kubernetes.operator.metrics.MetricManager;
import org.apache.flink.kubernetes.operator.metrics.OperatorMetricUtils;
import org.apache.flink.kubernetes.operator.metrics.TestingMetricListener;
import org.apache.flink.kubernetes.operator.reconciler.ReconciliationUtils;
import org.apache.flink.metrics.Histogram;

import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.LongStream;

import static org.apache.flink.kubernetes.operator.api.lifecycle.ResourceLifecycleState.CREATED;
import static org.apache.flink.kubernetes.operator.api.lifecycle.ResourceLifecycleState.DEPLOYED;
import static org.apache.flink.kubernetes.operator.api.lifecycle.ResourceLifecycleState.FAILED;
import static org.apache.flink.kubernetes.operator.api.lifecycle.ResourceLifecycleState.ROLLED_BACK;
import static org.apache.flink.kubernetes.operator.api.lifecycle.ResourceLifecycleState.ROLLING_BACK;
import static org.apache.flink.kubernetes.operator.api.lifecycle.ResourceLifecycleState.STABLE;
import static org.apache.flink.kubernetes.operator.api.lifecycle.ResourceLifecycleState.SUSPENDED;
import static org.apache.flink.kubernetes.operator.api.lifecycle.ResourceLifecycleState.UPGRADING;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Test for resource lifecycle metrics. */
public class ResourceLifecycleMetricsTest {

    @Test
    public void lifecycleStateTest() {
        var application = TestUtils.buildApplicationCluster();
        assertEquals(CREATED, application.getStatus().getLifecycleState());

        ReconciliationUtils.updateStatusBeforeDeploymentAttempt(application, new Configuration());
        assertEquals(UPGRADING, application.getStatus().getLifecycleState());

        ReconciliationUtils.updateStatusForDeployedSpec(application, new Configuration());
        assertEquals(DEPLOYED, application.getStatus().getLifecycleState());

        application.getStatus().getReconciliationStatus().markReconciledSpecAsStable();
        assertEquals(STABLE, application.getStatus().getLifecycleState());

        application.getStatus().setError("errr");
        assertEquals(STABLE, application.getStatus().getLifecycleState());

        application.getStatus().getJobStatus().setState(JobStatus.FAILED);
        assertEquals(FAILED, application.getStatus().getLifecycleState());

        application.getStatus().setError(null);

        application
                .getStatus()
                .getReconciliationStatus()
                .setState(ReconciliationState.ROLLING_BACK);
        assertEquals(ROLLING_BACK, application.getStatus().getLifecycleState());

        application.getStatus().getJobStatus().setState(JobStatus.RECONCILING);
        application.getStatus().getReconciliationStatus().setState(ReconciliationState.ROLLED_BACK);
        assertEquals(ROLLED_BACK, application.getStatus().getLifecycleState());

        application.getStatus().getJobStatus().setState(JobStatus.FAILED);
        assertEquals(FAILED, application.getStatus().getLifecycleState());

        application.getStatus().getJobStatus().setState(JobStatus.RUNNING);
        application.getSpec().getJob().setState(JobState.SUSPENDED);
        ReconciliationUtils.updateStatusForDeployedSpec(application, new Configuration());
        assertEquals(SUSPENDED, application.getStatus().getLifecycleState());
    }

    @Test
    public void testLifecycleTracker() {
        var transitionHistos = initTransitionHistos();
        var timeHistos = initTimeHistos();

        var lifecycleTracker =
                new ResourceLifecycleMetricTracker(
                        CREATED, Instant.ofEpochMilli(0), transitionHistos, timeHistos);

        long ts = 1000;
        lifecycleTracker.onUpdate(CREATED, Instant.ofEpochMilli(ts));
        lifecycleTracker.onUpdate(UPGRADING, Instant.ofEpochMilli(ts += 1000));
        lifecycleTracker.onUpdate(UPGRADING, Instant.ofEpochMilli(ts += 1000));
        lifecycleTracker.onUpdate(DEPLOYED, Instant.ofEpochMilli(ts += 1000));
        lifecycleTracker.onUpdate(DEPLOYED, Instant.ofEpochMilli(ts += 1000));
        lifecycleTracker.onUpdate(STABLE, Instant.ofEpochMilli(ts += 1000));

        lifecycleTracker.onUpdate(UPGRADING, Instant.ofEpochMilli(ts += 1000));
        lifecycleTracker.onUpdate(UPGRADING, Instant.ofEpochMilli(ts += 1000));
        lifecycleTracker.onUpdate(DEPLOYED, Instant.ofEpochMilli(ts += 1000));
        lifecycleTracker.onUpdate(ROLLING_BACK, Instant.ofEpochMilli(ts += 1000));
        lifecycleTracker.onUpdate(ROLLED_BACK, Instant.ofEpochMilli(ts += 1000));

        lifecycleTracker.onUpdate(SUSPENDED, Instant.ofEpochMilli(ts += 1000));
        lifecycleTracker.onUpdate(UPGRADING, Instant.ofEpochMilli(ts += 1000));
        lifecycleTracker.onUpdate(DEPLOYED, Instant.ofEpochMilli(ts += 1000));
        lifecycleTracker.onUpdate(DEPLOYED, Instant.ofEpochMilli(ts += 1000));
        lifecycleTracker.onUpdate(STABLE, Instant.ofEpochMilli(ts += 1000));
        lifecycleTracker.onUpdate(UPGRADING, Instant.ofEpochMilli(ts += 1000));
        lifecycleTracker.onUpdate(UPGRADING, Instant.ofEpochMilli(ts += 1000));
        lifecycleTracker.onUpdate(DEPLOYED, Instant.ofEpochMilli(ts += 1000));
        lifecycleTracker.onUpdate(DEPLOYED, Instant.ofEpochMilli(ts += 1000));
        lifecycleTracker.onUpdate(STABLE, Instant.ofEpochMilli(ts + 1000));

        validateTransition(transitionHistos, "Resume", 1, 4);
        validateTransition(transitionHistos, "Upgrade", 1, 5);
        validateTransition(transitionHistos, "Suspend", 3, 1);
        validateTransition(transitionHistos, "Stabilization", 3, 2);
        validateTransition(transitionHistos, "Rollback", 1, 2);
        validateTransition(transitionHistos, "Submission", 5, 1);

        validateTime(timeHistos, CREATED, 1, 1);
        validateTime(timeHistos, UPGRADING, 4, 3);
    }

    @Test
    public void testLifecycleMetricsConfig() {
        var dep1 = TestUtils.buildApplicationCluster();
        dep1.getMetadata().setNamespace("ns1");
        dep1.getMetadata().setName("n1");
        var dep2 = TestUtils.buildApplicationCluster();
        dep2.getMetadata().setNamespace("ns1");
        dep2.getMetadata().setName("n2");
        var dep3 = TestUtils.buildApplicationCluster();
        dep3.getMetadata().setNamespace("ns2");
        dep3.getMetadata().setName("n3");

        var conf = new Configuration();
        var metricManager =
                MetricManager.createFlinkDeploymentMetricManager(
                        conf, TestUtils.createTestMetricGroup(conf));
        var lifeCycleMetrics = getLifeCycleMetrics(metricManager);

        metricManager.onUpdate(dep1);
        metricManager.onUpdate(dep2);
        metricManager.onUpdate(dep3);

        var trackers = lifeCycleMetrics.getLifecycleTrackers();
        var tracker1 = trackers.get(Tuple2.of("ns1", "n1"));
        var tracker2 = trackers.get(Tuple2.of("ns1", "n2"));
        var tracker3 = trackers.get(Tuple2.of("ns2", "n3"));

        assertEquals(tracker1.getStateTimeHistos(), tracker2.getStateTimeHistos());
        assertEquals(tracker1.getTransitionHistos(), tracker2.getTransitionHistos());
        assertNotEquals(tracker1.getStateTimeHistos(), tracker3.getStateTimeHistos());
        assertNotEquals(tracker1.getTransitionHistos(), tracker3.getTransitionHistos());
        tracker1.getStateTimeHistos()
                .forEach(
                        (k, l) -> {
                            assertEquals(2, l.size());
                            assertEquals(l.get(0), tracker3.getStateTimeHistos().get(k).get(0));
                        });

        tracker1.getTransitionHistos()
                .forEach(
                        (k, l) -> {
                            assertEquals(2, l.size());
                            assertEquals(l.get(0), tracker3.getTransitionHistos().get(k).get(0));
                        });

        conf.set(
                KubernetesOperatorMetricOptions.OPERATOR_LIFECYCLE_NAMESPACE_HISTOGRAMS_ENABLED,
                false);
        metricManager =
                MetricManager.createFlinkDeploymentMetricManager(
                        conf, TestUtils.createTestMetricGroup(conf));
        lifeCycleMetrics = getLifeCycleMetrics(metricManager);

        metricManager.onUpdate(dep1);
        metricManager.onUpdate(dep2);
        metricManager.onUpdate(dep3);

        trackers = lifeCycleMetrics.getLifecycleTrackers();
        assertEquals(
                trackers.get(Tuple2.of("ns1", "n1")).getStateTimeHistos(),
                trackers.get(Tuple2.of("ns2", "n3")).getStateTimeHistos());
        assertEquals(
                trackers.get(Tuple2.of("ns1", "n1")).getTransitionHistos(),
                trackers.get(Tuple2.of("ns2", "n3")).getTransitionHistos());
        trackers.get(Tuple2.of("ns1", "n1"))
                .getStateTimeHistos()
                .forEach((k, l) -> assertEquals(1, l.size()));

        trackers.get(Tuple2.of("ns1", "n1"))
                .getTransitionHistos()
                .forEach((k, l) -> assertEquals(1, l.size()));

        conf.set(KubernetesOperatorMetricOptions.OPERATOR_LIFECYCLE_METRICS_ENABLED, false);
        metricManager =
                MetricManager.createFlinkDeploymentMetricManager(
                        conf, TestUtils.createTestMetricGroup(conf));
        assertNull(getLifeCycleMetrics(metricManager));

        metricManager.onUpdate(dep1);
        metricManager.onUpdate(dep2);
        metricManager.onUpdate(dep3);
    }

    @Test
    public void testGlobalHistoNames() {
        var conf = new Configuration();
        var testingMetricListener = new TestingMetricListener(new Configuration());
        var deploymentMetricManager =
                MetricManager.createFlinkDeploymentMetricManager(
                        conf, testingMetricListener.getMetricGroup());
        var deploymentLifecycleMetrics = getLifeCycleMetrics(deploymentMetricManager);
        deploymentLifecycleMetrics.onUpdate(TestUtils.buildApplicationCluster());
        testGlobalHistoNames(testingMetricListener, FlinkDeployment.class);

        var sessionJobMetricManager =
                MetricManager.createFlinkSessionJobMetricManager(
                        conf, testingMetricListener.getMetricGroup());
        var sessionJobLifecycleMetrics = getLifeCycleMetrics(sessionJobMetricManager);
        sessionJobLifecycleMetrics.onUpdate(TestUtils.buildSessionJob());

        testGlobalHistoNames(testingMetricListener, FlinkSessionJob.class);
    }

    private void testGlobalHistoNames(TestingMetricListener metricListener, Class<?> resoureClass) {
        for (var state : ResourceLifecycleState.values()) {
            assertTrue(
                    metricListener
                            .getHistogram(
                                    String.format(
                                            metricListener.getMetricId(
                                                    "%s.Lifecycle.State.%s.TimeSeconds"),
                                            resoureClass.getSimpleName(),
                                            state))
                            .isPresent());
        }

        for (var transition : LifecycleMetrics.TRACKED_TRANSITIONS) {
            assertTrue(
                    metricListener
                            .getHistogram(
                                    String.format(
                                            metricListener.getMetricId(
                                                    "%s.Lifecycle.Transition.%s.TimeSeconds"),
                                            resoureClass.getSimpleName(),
                                            transition.metricName))
                            .isPresent());
        }
    }

    public static <T extends AbstractFlinkResource<?, ?>> LifecycleMetrics<T> getLifeCycleMetrics(
            MetricManager<T> metricManager) {
        for (CustomResourceMetrics<?> metrics : metricManager.getRegisteredMetrics()) {
            if (metrics instanceof LifecycleMetrics) {
                return (LifecycleMetrics<T>) metrics;
            }
        }
        return null;
    }

    private void validateTransition(
            Map<String, List<Histogram>> histos, String name, int size, long mean) {
        histos.get(name)
                .forEach(
                        h -> {
                            var stat = h.getStatistics();
                            assertEquals(size, stat.size());
                            assertEquals(mean, stat.getMean());
                        });
    }

    private void validateTime(
            Map<ResourceLifecycleState, List<Histogram>> histos,
            ResourceLifecycleState state,
            int size,
            long sum) {
        histos.get(state)
                .forEach(
                        h -> {
                            var stat = h.getStatistics();
                            assertEquals(size, stat.size());
                            assertEquals(sum, LongStream.of(stat.getValues()).sum());
                        });
    }

    private Map<String, List<Histogram>> initTransitionHistos() {
        var histos = new ConcurrentHashMap<String, List<Histogram>>();
        LifecycleMetrics.TRACKED_TRANSITIONS.forEach(
                t ->
                        histos.computeIfAbsent(
                                t.metricName,
                                name ->
                                        List.of(
                                                OperatorMetricUtils.createHistogram(
                                                        FlinkOperatorConfiguration
                                                                .fromConfiguration(
                                                                        new Configuration())))));
        return histos;
    }

    private Map<ResourceLifecycleState, List<Histogram>> initTimeHistos() {
        var histos = new ConcurrentHashMap<ResourceLifecycleState, List<Histogram>>();
        for (ResourceLifecycleState state : ResourceLifecycleState.values()) {
            histos.put(
                    state,
                    List.of(
                            OperatorMetricUtils.createHistogram(
                                    FlinkOperatorConfiguration.fromConfiguration(
                                            new Configuration()))));
        }
        return histos;
    }
}
