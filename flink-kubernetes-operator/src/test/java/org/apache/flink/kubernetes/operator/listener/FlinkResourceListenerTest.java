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

import org.apache.flink.configuration.Configuration;
import org.apache.flink.kubernetes.operator.TestUtils;
import org.apache.flink.kubernetes.operator.crd.FlinkDeployment;
import org.apache.flink.kubernetes.operator.crd.status.FlinkDeploymentStatus;
import org.apache.flink.kubernetes.operator.crd.status.JobManagerDeploymentStatus;
import org.apache.flink.kubernetes.operator.utils.EventRecorder;
import org.apache.flink.kubernetes.operator.utils.StatusRecorder;

import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.server.mock.EnableKubernetesMockClient;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

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

        var statusRecorder =
                StatusRecorder.<FlinkDeploymentStatus>create(
                        kubernetesClient,
                        TestUtils.createTestMetricManager(new Configuration()),
                        listeners);
        var eventRecorder = EventRecorder.create(kubernetesClient, listeners);

        var deployment = TestUtils.buildApplicationCluster();
        statusRecorder.updateStatusFromCache(deployment);

        statusRecorder.patchAndCacheStatus(deployment);
        assertTrue(listener1.updates.isEmpty());
        assertTrue(listener2.updates.isEmpty());
        assertTrue(listener1.events.isEmpty());
        assertTrue(listener2.events.isEmpty());

        deployment.getStatus().setJobManagerDeploymentStatus(JobManagerDeploymentStatus.ERROR);

        statusRecorder.patchAndCacheStatus(deployment);
        assertEquals(1, listener1.updates.size());
        assertEquals(deployment, listener1.updates.get(0).getFlinkResource());

        assertEquals(1, listener2.updates.size());
        assertEquals(deployment, listener2.updates.get(0).getFlinkResource());

        deployment.getStatus().setJobManagerDeploymentStatus(JobManagerDeploymentStatus.DEPLOYING);
        statusRecorder.patchAndCacheStatus(deployment);

        assertEquals(2, listener1.updates.size());
        assertEquals(deployment, listener1.updates.get(0).getFlinkResource());
        assertEquals(2, listener2.updates.size());
        assertEquals(deployment, listener2.updates.get(0).getFlinkResource());

        var updateContext =
                (FlinkResourceListener.StatusUpdateContext<FlinkDeployment, FlinkDeploymentStatus>)
                        listener1.updates.get(1);
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
                "err");
        assertEquals(1, listener1.events.size());
        eventRecorder.triggerEvent(
                deployment,
                EventRecorder.Type.Warning,
                EventRecorder.Reason.SavepointError,
                EventRecorder.Component.Operator,
                "err");
        assertEquals(2, listener1.events.size());

        for (int i = 0; i < listener1.events.size(); i++) {
            assertEquals(listener1.events.get(i).getEvent(), listener2.events.get(i).getEvent());
        }
    }
}
