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
import org.apache.flink.kubernetes.operator.config.DefaultConfig;
import org.apache.flink.kubernetes.operator.config.FlinkOperatorConfiguration;
import org.apache.flink.kubernetes.operator.controller.FlinkControllerConfig;
import org.apache.flink.kubernetes.operator.controller.FlinkDeploymentController;
import org.apache.flink.kubernetes.operator.metrics.OperatorMetricUtils;
import org.apache.flink.kubernetes.operator.observer.ObserverFactory;
import org.apache.flink.kubernetes.operator.reconciler.ReconcilerFactory;
import org.apache.flink.kubernetes.operator.service.FlinkService;
import org.apache.flink.kubernetes.operator.utils.FlinkUtils;
import org.apache.flink.kubernetes.operator.validation.DefaultDeploymentValidator;
import org.apache.flink.kubernetes.operator.validation.FlinkDeploymentValidator;

import io.fabric8.kubernetes.client.DefaultKubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.javaoperatorsdk.operator.Operator;
import io.javaoperatorsdk.operator.api.config.ConfigurationService;
import io.javaoperatorsdk.operator.api.config.ConfigurationServiceOverrider;
import io.javaoperatorsdk.operator.config.runtime.DefaultConfigurationService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Executors;

/** Main Class for Flink native k8s operator. */
public class FlinkOperator {
    private static final Logger LOG = LoggerFactory.getLogger(FlinkOperator.class);

    private final Operator operator;
    private final FlinkDeploymentController controller;
    private final FlinkControllerConfig controllerConfig;

    public FlinkOperator() {
        this(FlinkUtils.loadDefaultConfig());
    }

    public FlinkOperator(DefaultConfig defaultConfig) {

        LOG.info("Starting Flink Kubernetes Operator");
        OperatorMetricUtils.initOperatorMetrics(defaultConfig.getOperatorConfig());

        KubernetesClient client = new DefaultKubernetesClient();
        String namespace = client.getNamespace();
        if (namespace == null) {
            namespace = "default";
        }

        FlinkOperatorConfiguration operatorConfiguration =
                FlinkOperatorConfiguration.fromConfiguration(defaultConfig.getOperatorConfig());

        ConfigurationService configurationService = getConfigurationService(operatorConfiguration);

        operator = new Operator(client, configurationService);

        FlinkService flinkService = new FlinkService(client, operatorConfiguration);

        FlinkDeploymentValidator validator = new DefaultDeploymentValidator();
        ReconcilerFactory reconcilerFactory =
                new ReconcilerFactory(client, flinkService, operatorConfiguration);
        ObserverFactory observerFactory = new ObserverFactory(flinkService, operatorConfiguration);

        controller =
                new FlinkDeploymentController(
                        defaultConfig,
                        operatorConfiguration,
                        client,
                        namespace,
                        validator,
                        reconcilerFactory,
                        observerFactory);

        controllerConfig =
                new FlinkControllerConfig(controller, operatorConfiguration.getWatchedNamespaces());
        controller.setControllerConfig(controllerConfig);
        controllerConfig.setConfigurationService(configurationService);
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
        operator.register(controller, controllerConfig);
        operator.installShutdownHook();
        operator.start();
    }

    @VisibleForTesting
    protected Operator getOperator() {
        return operator;
    }

    public static void main(String... args) {
        new FlinkOperator().run();
    }
}
