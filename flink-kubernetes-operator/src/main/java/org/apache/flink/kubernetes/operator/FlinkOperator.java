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
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.plugin.PluginManager;
import org.apache.flink.core.plugin.PluginUtils;
import org.apache.flink.kubernetes.operator.config.FlinkConfigManager;
import org.apache.flink.kubernetes.operator.controller.FlinkDeploymentController;
import org.apache.flink.kubernetes.operator.controller.FlinkSessionJobController;
import org.apache.flink.kubernetes.operator.crd.status.FlinkDeploymentStatus;
import org.apache.flink.kubernetes.operator.crd.status.FlinkSessionJobStatus;
import org.apache.flink.kubernetes.operator.listener.FlinkResourceListener;
import org.apache.flink.kubernetes.operator.listener.ListenerUtils;
import org.apache.flink.kubernetes.operator.metrics.FlinkOperatorMetrics;
import org.apache.flink.kubernetes.operator.metrics.MetricManager;
import org.apache.flink.kubernetes.operator.metrics.OperatorMetricUtils;
import org.apache.flink.kubernetes.operator.observer.deployment.ObserverFactory;
import org.apache.flink.kubernetes.operator.observer.sessionjob.SessionJobObserver;
import org.apache.flink.kubernetes.operator.reconciler.deployment.ReconcilerFactory;
import org.apache.flink.kubernetes.operator.reconciler.sessionjob.SessionJobReconciler;
import org.apache.flink.kubernetes.operator.service.FlinkService;
import org.apache.flink.kubernetes.operator.utils.EnvUtils;
import org.apache.flink.kubernetes.operator.utils.EventRecorder;
import org.apache.flink.kubernetes.operator.utils.StatusRecorder;
import org.apache.flink.kubernetes.operator.utils.ValidatorUtils;
import org.apache.flink.kubernetes.operator.validation.FlinkResourceValidator;
import org.apache.flink.metrics.MetricGroup;

import io.fabric8.kubernetes.client.DefaultKubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.javaoperatorsdk.operator.Operator;
import io.javaoperatorsdk.operator.RegisteredController;
import io.javaoperatorsdk.operator.api.config.ConfigurationServiceOverrider;
import io.javaoperatorsdk.operator.api.config.ControllerConfigurationOverrider;
import io.javaoperatorsdk.operator.processing.retry.GenericRetry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.Executors;

/** Main Class for Flink native k8s operator. */
public class FlinkOperator {
    private static final Logger LOG = LoggerFactory.getLogger(FlinkOperator.class);

    private final Operator operator;

    private final KubernetesClient client;
    private final FlinkService flinkService;
    private final FlinkConfigManager configManager;
    private final Set<FlinkResourceValidator> validators;
    private final Set<RegisteredController> registeredControllers = new HashSet<>();
    private final MetricGroup metricGroup;
    private final Collection<FlinkResourceListener> listeners;

    private static final String OPERATOR_SDK_GROUP = "operator.sdk";

    public FlinkOperator(@Nullable Configuration conf) {
        this.client = new DefaultKubernetesClient();
        this.configManager =
                conf != null
                        ? new FlinkConfigManager(conf) // For testing only
                        : new FlinkConfigManager(this::handleNamespaceChanges);
        this.metricGroup =
                OperatorMetricUtils.initOperatorMetrics(configManager.getDefaultConfig());
        this.operator = new Operator(client, this::overrideOperatorConfigs);
        this.flinkService = new FlinkService(client, configManager);
        this.validators = ValidatorUtils.discoverValidators(configManager);
        this.listeners = ListenerUtils.discoverListeners(configManager);
        PluginManager pluginManager =
                PluginUtils.createPluginManagerFromRootFolder(configManager.getDefaultConfig());
        FileSystem.initialize(configManager.getDefaultConfig(), pluginManager);
    }

    private void handleNamespaceChanges(Set<String> namespaces) {
        registeredControllers.forEach(
                controller -> {
                    if (controller.allowsNamespaceChanges()) {
                        LOG.info("Changing namespaces on {} to {}", controller, namespaces);
                        controller.changeNamespaces(namespaces);
                    }
                });
    }

    private void overrideOperatorConfigs(ConfigurationServiceOverrider overrider) {
        int parallelism = configManager.getOperatorConfiguration().getReconcilerMaxParallelism();
        if (parallelism == -1) {
            LOG.info("Configuring operator with unbounded reconciliation thread pool.");
            overrider.withExecutorService(Executors.newCachedThreadPool());
        } else {
            LOG.info("Configuring operator with {} reconciliation threads.", parallelism);
            overrider.withConcurrentReconciliationThreads(parallelism);
        }
        if (configManager.getOperatorConfiguration().getJosdkMetricsEnabled()) {
            overrider.withMetrics(
                    new FlinkOperatorMetrics(metricGroup.addGroup(OPERATOR_SDK_GROUP)));
        }
    }

    private void registerDeploymentController() {
        var statusRecorder = StatusRecorder.<FlinkDeploymentStatus>create(client, listeners);
        var eventRecorder = EventRecorder.create(client, listeners);
        var reconcilerFactory =
                new ReconcilerFactory(client, flinkService, configManager, eventRecorder);
        var observerFactory =
                new ObserverFactory(flinkService, configManager, statusRecorder, eventRecorder);

        var controller =
                new FlinkDeploymentController(
                        configManager,
                        validators,
                        reconcilerFactory,
                        observerFactory,
                        new MetricManager<>(metricGroup),
                        statusRecorder,
                        eventRecorder);
        registeredControllers.add(operator.register(controller, this::overrideControllerConfigs));
    }

    private void registerSessionJobController() {
        var eventRecorder = EventRecorder.create(client, listeners);
        var reconciler =
                new SessionJobReconciler(client, flinkService, configManager, eventRecorder);
        var statusRecorder = StatusRecorder.<FlinkSessionJobStatus>create(client, listeners);
        var observer =
                new SessionJobObserver(flinkService, configManager, statusRecorder, eventRecorder);
        var controller =
                new FlinkSessionJobController(
                        configManager,
                        validators,
                        reconciler,
                        observer,
                        new MetricManager<>(metricGroup),
                        statusRecorder);

        registeredControllers.add(operator.register(controller, this::overrideControllerConfigs));
    }

    private void overrideControllerConfigs(ControllerConfigurationOverrider<?> overrider) {
        overrider.settingNamespaces(
                configManager.getOperatorConfiguration().getWatchedNamespaces());
        overrider.withRetry(
                GenericRetry.fromConfiguration(
                        configManager.getOperatorConfiguration().getRetryConfiguration()));
    }

    public void run() {
        registerDeploymentController();
        registerSessionJobController();
        operator.installShutdownHook();
        operator.start();
    }

    public static void main(String... args) {
        EnvUtils.logEnvironmentInfo(LOG, "Flink Kubernetes Operator", args);
        new FlinkOperator(null).run();
    }
}
