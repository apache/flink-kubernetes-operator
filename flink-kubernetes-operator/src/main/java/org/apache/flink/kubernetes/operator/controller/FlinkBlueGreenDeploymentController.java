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

package org.apache.flink.kubernetes.operator.controller;

import org.apache.flink.kubernetes.operator.api.FlinkBlueGreenDeployment;
import org.apache.flink.kubernetes.operator.api.status.FlinkBlueGreenDeploymentState;
import org.apache.flink.kubernetes.operator.api.status.FlinkBlueGreenDeploymentStatus;
import org.apache.flink.kubernetes.operator.config.FlinkConfigManager;
import org.apache.flink.kubernetes.operator.controller.bluegreen.BlueGreenContext;
import org.apache.flink.kubernetes.operator.controller.bluegreen.BlueGreenDeploymentService;
import org.apache.flink.kubernetes.operator.controller.bluegreen.BlueGreenStateHandlerRegistry;
import org.apache.flink.kubernetes.operator.controller.bluegreen.handlers.BlueGreenStateHandler;
import org.apache.flink.kubernetes.operator.service.FlinkResourceContextFactory;
import org.apache.flink.kubernetes.operator.utils.EventSourceUtils;
import org.apache.flink.kubernetes.operator.utils.StatusRecorder;

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.javaoperatorsdk.operator.api.config.informer.InformerEventSourceConfiguration;
import io.javaoperatorsdk.operator.api.reconciler.Context;
import io.javaoperatorsdk.operator.api.reconciler.ControllerConfiguration;
import io.javaoperatorsdk.operator.api.reconciler.EventSourceContext;
import io.javaoperatorsdk.operator.api.reconciler.Reconciler;
import io.javaoperatorsdk.operator.api.reconciler.UpdateControl;
import io.javaoperatorsdk.operator.processing.event.source.EventSource;
import io.javaoperatorsdk.operator.processing.event.source.informer.InformerEventSource;
import io.javaoperatorsdk.operator.processing.event.source.informer.Mappers;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

import static org.apache.flink.kubernetes.operator.api.status.FlinkBlueGreenDeploymentState.INITIALIZING_BLUE;

/**
 * Controller that runs the main reconcile loop for Flink Blue/Green deployments.
 *
 * <h2>State Machine Flow</h2>
 *
 * <p>Deployment states:
 *
 * <ol>
 *   <li>{@code INITIALIZING_BLUE}: First-time deployment setup.
 *   <li>{@code ACTIVE_BLUE}: Blue environment serving traffic, monitoring for updates.
 *   <li>{@code TRANSITIONING_TO_GREEN}: Deploying Green environment while Blue serves traffic.
 *   <li>{@code ACTIVE_GREEN}: Green environment serving traffic, monitoring for updates.
 *   <li>{@code TRANSITIONING_TO_BLUE}: Deploying Blue environment while Green serves traffic.
 * </ol>
 *
 * <h2>Orchestration Process</h2>
 *
 * <p>{@link #reconcile(FlinkBlueGreenDeployment, Context)} performs:
 *
 * <ol>
 *   <li>Create a {@link BlueGreenContext} with the current deployment state.
 *   <li>Query {@link BlueGreenStateHandlerRegistry} for the appropriate handler.
 *   <li>Delegate to {@link BlueGreenStateHandler#handle(BlueGreenContext)}.
 *   <li>The handler invokes {@link BlueGreenDeploymentService} operations.
 *   <li>Return an {@link UpdateControl} with the next reconciliation schedule.
 * </ol>
 */
@ControllerConfiguration
public class FlinkBlueGreenDeploymentController implements Reconciler<FlinkBlueGreenDeployment> {

    private static final Logger LOG =
            LoggerFactory.getLogger(FlinkBlueGreenDeploymentController.class);

    private final FlinkResourceContextFactory ctxFactory;
    private final BlueGreenStateHandlerRegistry handlerRegistry;
    private final FlinkConfigManager flinkConfigManager;
    private final StatusRecorder<FlinkBlueGreenDeployment, FlinkBlueGreenDeploymentStatus>
            statusRecorder;

    public FlinkBlueGreenDeploymentController(
            FlinkResourceContextFactory ctxFactory,
            FlinkConfigManager flinkConfigManager,
            StatusRecorder<FlinkBlueGreenDeployment, FlinkBlueGreenDeploymentStatus>
                    statusRecorder) {
        this.ctxFactory = ctxFactory;
        this.handlerRegistry = new BlueGreenStateHandlerRegistry();
        this.flinkConfigManager = flinkConfigManager;
        this.statusRecorder = statusRecorder;
    }

    @Override
    public List<EventSource<?, FlinkBlueGreenDeployment>> prepareEventSources(
            EventSourceContext<FlinkBlueGreenDeployment> context) {
        List<EventSource<?, FlinkBlueGreenDeployment>> eventSources = new ArrayList<>();
        eventSources.add(EventSourceUtils.getBlueGreenFlinkDeploymentInformerEventSource(context));

        // Advanced (FLIP-504): also watch the gate ConfigMap as a secondary resource.
        InformerEventSourceConfiguration<ConfigMap> configMapConfig =
                InformerEventSourceConfiguration.from(
                                ConfigMap.class, FlinkBlueGreenDeployment.class)
                        .withSecondaryToPrimaryMapper(
                                Mappers.fromOwnerReferences(context.getPrimaryResourceClass()))
                        .withNamespacesInheritedFromController()
                        .withFollowControllerNamespacesChanges(true)
                        .build();
        eventSources.add(new InformerEventSource<>(configMapConfig, context));
        if (flinkConfigManager.getOperatorConfiguration().isManageIngress()) {
            eventSources.add(EventSourceUtils.getBlueGreenIngressInformerEventSource(context));
        }
        return eventSources;
    }

    @Override
    public UpdateControl<FlinkBlueGreenDeployment> reconcile(
            FlinkBlueGreenDeployment bgDeployment, Context<FlinkBlueGreenDeployment> josdkContext)
            throws Exception {

        FlinkBlueGreenDeploymentStatus deploymentStatus = bgDeployment.getStatus();

        if (deploymentStatus == null) {
            var context =
                    ctxFactory.getBlueGreenContext(
                            bgDeployment, new FlinkBlueGreenDeploymentStatus(), josdkContext, null);
            UpdateControl<FlinkBlueGreenDeployment> updateControl =
                    BlueGreenDeploymentService.patchStatusUpdateControl(
                                    context, INITIALIZING_BLUE, null, null)
                            .rescheduleAfter(0);
            statusRecorder.patchAndCacheStatus(bgDeployment, josdkContext.getClient());
            return updateControl;
        } else {
            FlinkBlueGreenDeploymentState currentState = deploymentStatus.getBlueGreenState();
            var context =
                    ctxFactory.getBlueGreenContext(
                            bgDeployment,
                            deploymentStatus,
                            josdkContext,
                            currentState == INITIALIZING_BLUE
                                    ? null
                                    : FlinkBlueGreenDeployments.fromSecondaryResources(
                                            josdkContext));

            LOG.debug(
                    "Processing state: {} for deployment: {}",
                    currentState,
                    context.getDeploymentName());

            BlueGreenStateHandler handler = handlerRegistry.getHandler(currentState);
            UpdateControl<FlinkBlueGreenDeployment> updateControl = handler.handle(context);
            statusRecorder.patchAndCacheStatus(bgDeployment, josdkContext.getClient());
            return updateControl;
        }
    }

    public static void logAndThrow(String message) {
        throw new RuntimeException(message);
    }
}
