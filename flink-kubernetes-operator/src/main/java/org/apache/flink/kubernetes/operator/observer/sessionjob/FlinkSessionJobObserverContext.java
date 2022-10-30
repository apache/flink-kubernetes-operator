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

package org.apache.flink.kubernetes.operator.observer.sessionjob;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.kubernetes.operator.api.FlinkDeployment;
import org.apache.flink.kubernetes.operator.api.FlinkSessionJob;
import org.apache.flink.kubernetes.operator.config.FlinkConfigManager;
import org.apache.flink.kubernetes.operator.observer.ObserverContext;
import org.apache.flink.kubernetes.operator.reconciler.sessionjob.SessionJobReconciler;
import org.apache.flink.kubernetes.operator.service.FlinkService;
import org.apache.flink.kubernetes.operator.service.FlinkServiceFactory;

import io.javaoperatorsdk.operator.api.reconciler.Context;
import lombok.Value;

/** Context for observing {@link FlinkSessionJob} resources. */
@Value
public class FlinkSessionJobObserverContext implements ObserverContext {

    boolean readyToReconcile;

    FlinkService flinkService;

    Configuration deployedConfig;

    public FlinkSessionJobObserverContext(
            FlinkSessionJob resource,
            Context<?> resourceContext,
            FlinkServiceFactory flinkServiceFactory,
            FlinkConfigManager configManager) {
        var flinkDepOpt = resourceContext.getSecondaryResource(FlinkDeployment.class);
        this.readyToReconcile = SessionJobReconciler.sessionClusterReady(flinkDepOpt);
        if (readyToReconcile) {
            var flinkDeployment = flinkDepOpt.get();
            flinkService = flinkServiceFactory.getOrCreate(flinkDeployment);
            deployedConfig = configManager.getSessionJobConfig(flinkDeployment, resource.getSpec());
        } else {
            flinkService = null;
            deployedConfig = null;
        }
    }
}
