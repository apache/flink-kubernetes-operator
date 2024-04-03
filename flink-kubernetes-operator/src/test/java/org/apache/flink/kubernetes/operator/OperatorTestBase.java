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

package org.apache.flink.kubernetes.operator;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.kubernetes.operator.api.AbstractFlinkResource;
import org.apache.flink.kubernetes.operator.config.FlinkConfigManager;
import org.apache.flink.kubernetes.operator.controller.FlinkResourceContext;
import org.apache.flink.kubernetes.operator.metrics.KubernetesOperatorMetricGroup;
import org.apache.flink.kubernetes.operator.utils.EventRecorder;
import org.apache.flink.kubernetes.operator.utils.FlinkResourceEventCollector;
import org.apache.flink.kubernetes.operator.utils.FlinkStateSnapshotEventCollector;

import io.fabric8.kubernetes.client.KubernetesClient;
import io.javaoperatorsdk.operator.api.reconciler.Context;
import org.junit.jupiter.api.BeforeEach;

/**
 * @link JobStatusObserver unit tests
 */
public abstract class OperatorTestBase {

    protected Configuration conf = new Configuration();
    protected FlinkConfigManager configManager = new FlinkConfigManager(conf);
    protected TestingFlinkService flinkService;
    protected FlinkResourceEventCollector flinkResourceEventCollector =
            new FlinkResourceEventCollector();
    protected FlinkStateSnapshotEventCollector flinkStateSnapshotEventCollector =
            new FlinkStateSnapshotEventCollector();
    protected EventRecorder eventRecorder;
    protected TestingStatusRecorder statusRecorder = new TestingStatusRecorder();
    protected KubernetesOperatorMetricGroup operatorMetricGroup;

    protected Context<?> context;

    @BeforeEach
    public void prepare() {
        getKubernetesClient().resource(TestUtils.buildApplicationCluster()).createOrReplace();
        getKubernetesClient().resource(TestUtils.buildSessionJob()).createOrReplace();
        flinkService = new TestingFlinkService(getKubernetesClient());
        context = flinkService.getContext();
        eventRecorder =
                new EventRecorder(flinkResourceEventCollector, flinkStateSnapshotEventCollector);
        operatorMetricGroup = TestUtils.createTestMetricGroup(configManager.getDefaultConfig());
        setup();
    }

    protected abstract void setup();

    protected abstract KubernetesClient getKubernetesClient();

    public <CR extends AbstractFlinkResource<?, ?>> FlinkResourceContext<CR> getResourceContext(
            CR cr) {
        return getResourceContext(cr, context);
    }

    public <CR extends AbstractFlinkResource<?, ?>> FlinkResourceContext<CR> getResourceContext(
            CR cr, Context josdkContext) {
        var ctxFactory =
                new TestingFlinkResourceContextFactory(
                        configManager, operatorMetricGroup, flinkService, eventRecorder);
        return ctxFactory.getResourceContext(cr, josdkContext);
    }
}
