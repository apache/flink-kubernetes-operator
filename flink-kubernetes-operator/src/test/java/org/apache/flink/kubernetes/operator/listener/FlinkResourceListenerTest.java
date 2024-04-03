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

package org.apache.flink.kubernetes.operator.listener;

import org.apache.flink.kubernetes.operator.TestUtils;
import org.apache.flink.kubernetes.operator.api.FlinkDeployment;
import org.apache.flink.kubernetes.operator.api.listener.FlinkResourceListener;
import org.apache.flink.kubernetes.operator.api.status.FlinkDeploymentStatus;
import org.apache.flink.kubernetes.operator.api.status.JobManagerDeploymentStatus;
import org.apache.flink.kubernetes.operator.metrics.MetricManager;
import org.apache.flink.kubernetes.operator.utils.EventRecorder;
import org.apache.flink.kubernetes.operator.utils.StatusRecorder;

import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.server.mock.EnableKubernetesMockClient;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Tests for {@link FlinkResourceListener}. */
@EnableKubernetesMockClient(crud = true)
public class FlinkResourceListenerTest {

    private KubernetesClient kubernetesClient;

    @BeforeEach
    public void before() {
        kubernetesClient.resource(TestUtils.buildApplicationCluster()).createOrReplace();
    }

    @Test
    public void testListeners() {
        var listener1 = new TestingListener();
        var listener2 = new TestingListener();
        var listeners = List.<FlinkResourceListener>of(listener1, listener2);

        StatusRecorder<FlinkDeployment, FlinkDeploymentStatus> statusRecorder =
                StatusRecorder.create(kubernetesClient, new MetricManager<>(), listeners);
        var eventRecorder = EventRecorder.create(kubernetesClient, listeners);

        var deployment = TestUtils.buildApplicationCluster();

        assertTrue(listener1.flinkResourceUpdates.isEmpty());
        assertTrue(listener2.flinkResourceUpdates.isEmpty());
        assertTrue(listener1.flinkResourceEvents.isEmpty());
        assertTrue(listener2.flinkResourceEvents.isEmpty());

        statusRecorder.updateStatusFromCache(deployment);
        assertEquals(1, listener1.flinkResourceUpdates.size());
        statusRecorder.updateStatusFromCache(deployment);
        assertEquals(1, listener1.flinkResourceUpdates.size());
        assertEquals(deployment, listener1.flinkResourceUpdates.get(0).getFlinkResource());

        deployment.getStatus().setJobManagerDeploymentStatus(JobManagerDeploymentStatus.ERROR);
        statusRecorder.patchAndCacheStatus(deployment, kubernetesClient);
        assertEquals(2, listener1.flinkResourceUpdates.size());
        assertEquals(deployment, listener1.flinkResourceUpdates.get(1).getFlinkResource());

        deployment.getStatus().setJobManagerDeploymentStatus(JobManagerDeploymentStatus.DEPLOYING);
        statusRecorder.patchAndCacheStatus(deployment, kubernetesClient);
        assertEquals(3, listener1.flinkResourceUpdates.size());
        assertEquals(deployment, listener1.flinkResourceUpdates.get(2).getFlinkResource());

        for (int i = 0; i < listener1.flinkResourceUpdates.size(); i++) {
            assertEquals(
                    listener1.flinkResourceUpdates.get(i).getTimestamp(),
                    listener2.flinkResourceUpdates.get(i).getTimestamp());
            assertEquals(
                    listener1.flinkResourceUpdates.get(i).getFlinkResource(),
                    listener2.flinkResourceUpdates.get(i).getFlinkResource());
        }

        var updateContext =
                (FlinkResourceListener.StatusUpdateContext<FlinkDeployment, FlinkDeploymentStatus>)
                        listener1.flinkResourceUpdates.get(2);
        assertEquals(
                JobManagerDeploymentStatus.ERROR,
                updateContext.getPreviousStatus().getJobManagerDeploymentStatus());
        assertEquals(
                JobManagerDeploymentStatus.DEPLOYING,
                updateContext.getNewStatus().getJobManagerDeploymentStatus());

        eventRecorder.triggerEvent(
                deployment,
                EventRecorder.Type.Warning,
                EventRecorder.Reason.SavepointError,
                EventRecorder.Component.Operator,
                "err",
                kubernetesClient);
        assertEquals(1, listener1.flinkResourceEvents.size());
        eventRecorder.triggerEvent(
                deployment,
                EventRecorder.Type.Warning,
                EventRecorder.Reason.SavepointError,
                EventRecorder.Component.Operator,
                "err",
                kubernetesClient);
        assertEquals(2, listener1.flinkResourceEvents.size());

        for (int i = 0; i < listener1.flinkResourceEvents.size(); i++) {
            assertEquals(
                    listener1.flinkResourceEvents.get(i).getEvent(),
                    listener2.flinkResourceEvents.get(i).getEvent());
            assertEquals(
                    listener1.flinkResourceEvents.get(i).getTimestamp(),
                    Instant.parse(
                            listener1.flinkResourceEvents.get(i).getEvent().getLastTimestamp()));
            assertEquals(
                    listener1.flinkResourceEvents.get(i).getTimestamp(),
                    listener2.flinkResourceEvents.get(i).getTimestamp());
        }
    }
}
