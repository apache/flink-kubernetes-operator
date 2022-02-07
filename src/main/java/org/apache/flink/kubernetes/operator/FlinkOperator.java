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

import org.apache.flink.kubernetes.operator.controller.FlinkDeploymentController;
import org.apache.flink.kubernetes.operator.observer.JobStatusObserver;
import org.apache.flink.kubernetes.operator.reconciler.JobReconciler;
import org.apache.flink.kubernetes.operator.reconciler.SessionReconciler;
import org.apache.flink.kubernetes.operator.service.FlinkService;

import io.fabric8.kubernetes.client.DefaultKubernetesClient;
import io.javaoperatorsdk.operator.Operator;
import io.javaoperatorsdk.operator.api.config.ConfigurationServiceOverrider;
import io.javaoperatorsdk.operator.config.runtime.DefaultConfigurationService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.takes.facets.fork.FkRegex;
import org.takes.facets.fork.TkFork;
import org.takes.http.Exit;
import org.takes.http.FtBasic;

import java.io.IOException;

/** Main Class for Flink native k8s operator. */
public class FlinkOperator {
    private static final Logger LOG = LoggerFactory.getLogger(FlinkOperator.class);

    public static void main(String... args) throws IOException {

        LOG.info("Starting Flink Kubernetes Operator");

        DefaultKubernetesClient client = new DefaultKubernetesClient();
        String namespace = client.getNamespace();
        if (namespace == null) {
            namespace = "default";
        }
        Operator operator =
                new Operator(
                        client,
                        new ConfigurationServiceOverrider(DefaultConfigurationService.instance())
                                .build());

        FlinkService flinkService = new FlinkService(client);

        JobStatusObserver observer = new JobStatusObserver(flinkService);
        JobReconciler jobReconciler = new JobReconciler(client, flinkService);
        SessionReconciler sessionReconciler = new SessionReconciler(client, flinkService);

        FlinkDeploymentController controller =
                new FlinkDeploymentController(
                        client, namespace, observer, jobReconciler, sessionReconciler);

        operator.register(controller);
        operator.installShutdownHook();
        operator.start();

        new FtBasic(new TkFork(new FkRegex("/health", "ALL GOOD!")), 8080).start(Exit.NEVER);
    }
}
