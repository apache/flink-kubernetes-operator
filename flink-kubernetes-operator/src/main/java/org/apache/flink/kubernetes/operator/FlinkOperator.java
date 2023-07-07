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

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.plugin.PluginManager;
import org.apache.flink.core.plugin.PluginUtils;
import org.apache.flink.kubernetes.operator.api.FlinkDeployment;
import org.apache.flink.kubernetes.operator.api.FlinkSessionJob;
import org.apache.flink.kubernetes.operator.api.listener.FlinkResourceListener;
import org.apache.flink.kubernetes.operator.config.FlinkConfigManager;
import org.apache.flink.kubernetes.operator.config.FlinkOperatorConfiguration;
import org.apache.flink.kubernetes.operator.config.KubernetesOperatorConfigOptions;
import org.apache.flink.kubernetes.operator.controller.FlinkDeploymentController;
import org.apache.flink.kubernetes.operator.controller.FlinkSessionJobController;
import org.apache.flink.kubernetes.operator.health.CanaryResourceManager;
import org.apache.flink.kubernetes.operator.health.HealthProbe;
import org.apache.flink.kubernetes.operator.health.OperatorHealthService;
import org.apache.flink.kubernetes.operator.listener.ListenerUtils;
import org.apache.flink.kubernetes.operator.metrics.KubernetesOperatorMetricGroup;
import org.apache.flink.kubernetes.operator.metrics.MetricManager;
import org.apache.flink.kubernetes.operator.metrics.OperatorJosdkMetrics;
import org.apache.flink.kubernetes.operator.metrics.OperatorMetricUtils;
import org.apache.flink.kubernetes.operator.observer.deployment.FlinkDeploymentObserverFactory;
import org.apache.flink.kubernetes.operator.observer.sessionjob.FlinkSessionJobObserver;
import org.apache.flink.kubernetes.operator.reconciler.deployment.ReconcilerFactory;
import org.apache.flink.kubernetes.operator.reconciler.sessionjob.SessionJobReconciler;
import org.apache.flink.kubernetes.operator.service.FlinkResourceContextFactory;
import org.apache.flink.kubernetes.operator.utils.AutoscalerLoader;
import org.apache.flink.kubernetes.operator.utils.EnvUtils;
import org.apache.flink.kubernetes.operator.utils.EventRecorder;
import org.apache.flink.kubernetes.operator.utils.KubernetesClientUtils;
import org.apache.flink.kubernetes.operator.utils.StatusRecorder;
import org.apache.flink.kubernetes.operator.utils.ValidatorUtils;
import org.apache.flink.kubernetes.operator.validation.FlinkResourceValidator;

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
    private final FlinkResourceContextFactory ctxFactory;
    private final FlinkConfigManager configManager;
    private final Set<FlinkResourceValidator> validators;
    @VisibleForTesting final Set<RegisteredController<?>> registeredControllers = new HashSet<>();
    private final KubernetesOperatorMetricGroup metricGroup;
    private final Collection<FlinkResourceListener> listeners;
    private final OperatorHealthService operatorHealthService;

    private final EventRecorder eventRecorder;
    private final Configuration baseConfig;

    public FlinkOperator(@Nullable Configuration conf) {
        this.configManager =
                conf != null
                        ? new FlinkConfigManager(conf) // For testing only
                        : new FlinkConfigManager(this::handleNamespaceChanges);

        baseConfig = configManager.getDefaultConfig();
        this.metricGroup = OperatorMetricUtils.initOperatorMetrics(baseConfig);
        this.client =
                KubernetesClientUtils.getKubernetesClient(
                        configManager.getOperatorConfiguration(), this.metricGroup);
        this.operator = createOperator();
        this.validators = ValidatorUtils.discoverValidators(configManager);
        this.listeners = ListenerUtils.discoverListeners(configManager);
        this.eventRecorder = EventRecorder.create(client, listeners);
        this.ctxFactory =
                new FlinkResourceContextFactory(client, configManager, metricGroup, eventRecorder);
        PluginManager pluginManager = PluginUtils.createPluginManagerFromRootFolder(baseConfig);
        FileSystem.initialize(baseConfig, pluginManager);
        this.operatorHealthService = OperatorHealthService.fromConfig(configManager);
    }

    @VisibleForTesting
    protected Operator createOperator() {
        return new Operator(client, this::overrideOperatorConfigs);
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
        var conf = configManager.getDefaultConfig();
        var operatorConf = FlinkOperatorConfiguration.fromConfiguration(conf);
        int parallelism = operatorConf.getReconcilerMaxParallelism();
        if (parallelism == -1) {
            LOG.info("Configuring operator with unbounded reconciliation thread pool.");
            overrider.withExecutorService(Executors.newCachedThreadPool());
        } else {
            LOG.info("Configuring operator with {} reconciliation threads.", parallelism);
            overrider.withConcurrentReconciliationThreads(parallelism);
        }

        if (operatorConf.isJosdkMetricsEnabled()) {
            overrider.withMetrics(new OperatorJosdkMetrics(metricGroup, configManager));
        }

        overrider.withTerminationTimeoutSeconds(
                (int)
                        conf.get(KubernetesOperatorConfigOptions.OPERATOR_TERMINATION_TIMEOUT)
                                .toSeconds());

        overrider.withStopOnInformerErrorDuringStartup(
                conf.get(KubernetesOperatorConfigOptions.OPERATOR_STOP_ON_INFORMER_ERROR));

        var leaderElectionConf = operatorConf.getLeaderElectionConfiguration();
        if (leaderElectionConf != null) {
            overrider.withLeaderElectionConfiguration(leaderElectionConf);
            LOG.info("Operator leader election is enabled.");
        } else {
            LOG.info("Operator leader election is disabled.");
        }
    }

    @VisibleForTesting
    void registerDeploymentController() {
        var metricManager =
                MetricManager.createFlinkDeploymentMetricManager(baseConfig, metricGroup);
        var statusRecorder = StatusRecorder.create(client, metricManager, listeners);
        var autoscalerFactory = AutoscalerLoader.loadJobAutoscalerFactory();
        var reconcilerFactory =
                new ReconcilerFactory(
                        client, configManager, eventRecorder, statusRecorder, autoscalerFactory);
        var observerFactory = new FlinkDeploymentObserverFactory(eventRecorder);
        var canaryResourceManager =
                new CanaryResourceManager<FlinkDeployment>(configManager, client);
        HealthProbe.INSTANCE.registerCanaryResourceManager(canaryResourceManager);

        var controller =
                new FlinkDeploymentController(
                        validators,
                        ctxFactory,
                        reconcilerFactory,
                        observerFactory,
                        statusRecorder,
                        eventRecorder,
                        canaryResourceManager);
        registeredControllers.add(operator.register(controller, this::overrideControllerConfigs));
    }

    @VisibleForTesting
    void registerSessionJobController() {
        var eventRecorder = EventRecorder.create(client, listeners);
        var metricManager =
                MetricManager.createFlinkSessionJobMetricManager(baseConfig, metricGroup);
        var statusRecorder = StatusRecorder.create(client, metricManager, listeners);
        var reconciler =
                new SessionJobReconciler(client, eventRecorder, statusRecorder, configManager);
        var observer = new FlinkSessionJobObserver(eventRecorder);
        var canaryResourceManager =
                new CanaryResourceManager<FlinkSessionJob>(configManager, client);
        HealthProbe.INSTANCE.registerCanaryResourceManager(canaryResourceManager);

        var controller =
                new FlinkSessionJobController(
                        validators,
                        ctxFactory,
                        reconciler,
                        observer,
                        statusRecorder,
                        eventRecorder,
                        canaryResourceManager);
        registeredControllers.add(operator.register(controller, this::overrideControllerConfigs));
    }

    private void overrideControllerConfigs(ControllerConfigurationOverrider<?> overrider) {
        var operatorConf = configManager.getOperatorConfiguration();
        var watchNamespaces = operatorConf.getWatchedNamespaces();
        LOG.info("Configuring operator to watch the following namespaces: {}.", watchNamespaces);
        overrider.settingNamespaces(operatorConf.getWatchedNamespaces());

        overrider.withRetry(GenericRetry.fromConfiguration(operatorConf.getRetryConfiguration()));

        var labelSelector = operatorConf.getLabelSelector();
        LOG.info(
                "Configuring operator to select custom resources with the {} labels.",
                labelSelector);
        overrider.withLabelSelector(labelSelector);
    }

    public void run() {
        registerDeploymentController();
        registerSessionJobController();
        operator.installShutdownHook();
        operator.start();
        if (operatorHealthService != null) {
            HealthProbe.INSTANCE.setRuntimeInfo(operator.getRuntimeInfo());
            Runtime.getRuntime().addShutdownHook(new Thread(operatorHealthService::stop));
            operatorHealthService.start();
        }
    }

    public void stop() {
        operator.stop();
    }

    public static void main(String... args) {
        EnvUtils.logEnvironmentInfo(LOG, "Flink Kubernetes Operator", args);
        new FlinkOperator(null).run();
    }
}
