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
import org.apache.flink.kubernetes.operator.config.FlinkConfigManager;
import org.apache.flink.kubernetes.operator.config.FlinkOperatorConfiguration;
import org.apache.flink.kubernetes.operator.controller.FlinkControllerConfig;
import org.apache.flink.kubernetes.operator.controller.FlinkDeploymentController;
import org.apache.flink.kubernetes.operator.controller.FlinkSessionJobController;
import org.apache.flink.kubernetes.operator.crd.FlinkDeployment;
import org.apache.flink.kubernetes.operator.crd.FlinkSessionJob;
import org.apache.flink.kubernetes.operator.crd.status.FlinkDeploymentStatus;
import org.apache.flink.kubernetes.operator.crd.status.FlinkSessionJobStatus;
import org.apache.flink.kubernetes.operator.informer.InformerManager;
import org.apache.flink.kubernetes.operator.metrics.MetricManager;
import org.apache.flink.kubernetes.operator.metrics.OperatorMetricUtils;
import org.apache.flink.kubernetes.operator.observer.Observer;
import org.apache.flink.kubernetes.operator.observer.deployment.ObserverFactory;
import org.apache.flink.kubernetes.operator.observer.sessionjob.SessionJobObserver;
import org.apache.flink.kubernetes.operator.reconciler.Reconciler;
import org.apache.flink.kubernetes.operator.reconciler.deployment.ReconcilerFactory;
import org.apache.flink.kubernetes.operator.reconciler.sessionjob.FlinkSessionJobReconciler;
import org.apache.flink.kubernetes.operator.service.FlinkService;
import org.apache.flink.kubernetes.operator.utils.EnvUtils;
import org.apache.flink.kubernetes.operator.utils.StatusHelper;
import org.apache.flink.kubernetes.operator.utils.ValidatorUtils;
import org.apache.flink.kubernetes.operator.validation.FlinkResourceValidator;
import org.apache.flink.metrics.MetricGroup;

import io.fabric8.kubernetes.client.DefaultKubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.javaoperatorsdk.operator.Operator;
import io.javaoperatorsdk.operator.api.config.ConfigurationService;
import io.javaoperatorsdk.operator.api.config.ConfigurationServiceOverrider;
import io.javaoperatorsdk.operator.config.runtime.DefaultConfigurationService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.Set;
import java.util.concurrent.Executors;

/** Main Class for Flink native k8s operator. */
public class FlinkOperator {
    private static final Logger LOG = LoggerFactory.getLogger(FlinkOperator.class);

    private final Operator operator;

    private final KubernetesClient client;
    private final FlinkService flinkService;
    private final ConfigurationService configurationService;
    private final FlinkConfigManager configManager;
    private final Set<FlinkResourceValidator> validators;
    private final MetricGroup metricGroup;
    private final InformerManager informerManager;

    public FlinkOperator(@Nullable Configuration conf) {
        this.client = new DefaultKubernetesClient();
        this.configManager = conf != null ? new FlinkConfigManager(conf) : new FlinkConfigManager();
        this.configurationService =
                getConfigurationService(configManager.getOperatorConfiguration());
        this.operator = new Operator(client, configurationService);
        this.flinkService = new FlinkService(client, configManager);
        this.validators = ValidatorUtils.discoverValidators(configManager);
        this.metricGroup =
                OperatorMetricUtils.initOperatorMetrics(configManager.getDefaultConfig());
        Set<String> watchedNamespaces =
                configManager.getOperatorConfiguration().getWatchedNamespaces();
        this.informerManager = new InformerManager(watchedNamespaces, client);
        PluginManager pluginManager =
                PluginUtils.createPluginManagerFromRootFolder(configManager.getDefaultConfig());
        FileSystem.initialize(configManager.getDefaultConfig(), pluginManager);
    }

    private void registerDeploymentController() {
        StatusHelper<FlinkDeploymentStatus> statusHelper = new StatusHelper<>(client);
        ReconcilerFactory reconcilerFactory =
                new ReconcilerFactory(client, flinkService, configManager, informerManager);
        ObserverFactory observerFactory =
                new ObserverFactory(client, flinkService, configManager, statusHelper);

        FlinkDeploymentController controller =
                new FlinkDeploymentController(
                        configManager,
                        client,
                        validators,
                        reconcilerFactory,
                        observerFactory,
                        new MetricManager<>(metricGroup),
                        statusHelper);

        FlinkControllerConfig<FlinkDeployment> controllerConfig =
                new FlinkControllerConfig<>(
                        controller,
                        configManager.getOperatorConfiguration().getWatchedNamespaces());
        controllerConfig.setConfigurationService(configurationService);
        operator.register(controller, controllerConfig);
    }

    private void registerSessionJobController() {
        Reconciler<FlinkSessionJob> reconciler =
                new FlinkSessionJobReconciler(client, flinkService, configManager);
        StatusHelper<FlinkSessionJobStatus> statusHelper = new StatusHelper<>(client);
        Observer<FlinkSessionJob> observer =
                new SessionJobObserver(flinkService, configManager, statusHelper);
        FlinkSessionJobController controller =
                new FlinkSessionJobController(
                        configManager,
                        client,
                        validators,
                        reconciler,
                        observer,
                        new MetricManager<>(metricGroup),
                        statusHelper,
                        informerManager);

        FlinkControllerConfig<FlinkSessionJob> controllerConfig =
                new FlinkControllerConfig<>(
                        controller,
                        configManager.getOperatorConfiguration().getWatchedNamespaces());

        controllerConfig.setConfigurationService(configurationService);
        operator.register(controller, controllerConfig);
    }

    private ConfigurationService getConfigurationService(
            FlinkOperatorConfiguration operatorConfiguration) {

        ConfigurationServiceOverrider configOverrider =
                new ConfigurationServiceOverrider(DefaultConfigurationService.instance())
                        .checkingCRDAndValidateLocalModel(false);

        int parallelism = operatorConfiguration.getReconcilerMaxParallelism();
        if (parallelism == -1) {
            LOG.info("Configuring operator with unbounded reconciliation thread pool.");
            configOverrider = configOverrider.withExecutorService(Executors.newCachedThreadPool());
        } else {
            LOG.info("Configuring operator with {} reconciliation threads.", parallelism);
            configOverrider = configOverrider.withConcurrentReconciliationThreads(parallelism);
        }
        return configOverrider.build();
    }

    public void run() {
        registerDeploymentController();
        registerSessionJobController();
        operator.installShutdownHook();
        operator.start();
    }

    @VisibleForTesting
    protected Operator getOperator() {
        return operator;
    }

    public static void main(String... args) {
        EnvUtils.logEnvironmentInfo(LOG, "Flink Kubernetes Operator", args);
        new FlinkOperator(null).run();
    }
}
