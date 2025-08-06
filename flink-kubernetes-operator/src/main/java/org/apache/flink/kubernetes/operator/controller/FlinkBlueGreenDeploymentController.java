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
import org.apache.flink.kubernetes.operator.api.FlinkDeployment;
import org.apache.flink.kubernetes.operator.api.status.FlinkBlueGreenDeploymentState;
import org.apache.flink.kubernetes.operator.api.status.FlinkBlueGreenDeploymentStatus;
import org.apache.flink.kubernetes.operator.controller.bluegreen.BlueGreenContext;
import org.apache.flink.kubernetes.operator.controller.bluegreen.BlueGreenDeploymentService;
import org.apache.flink.kubernetes.operator.controller.bluegreen.BlueGreenStateHandlerRegistry;
import org.apache.flink.kubernetes.operator.controller.bluegreen.handlers.BlueGreenStateHandler;
import org.apache.flink.kubernetes.operator.service.FlinkResourceContextFactory;

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

import static org.apache.flink.kubernetes.operator.api.spec.FlinkBlueGreenDeploymentConfigOptions.ABORT_GRACE_PERIOD;
import static org.apache.flink.kubernetes.operator.api.status.FlinkBlueGreenDeploymentState.INITIALIZING_BLUE;

/** Controller that runs the main reconcile loop for Flink Blue/Green deployments. */
@ControllerConfiguration
public class FlinkBlueGreenDeploymentController implements Reconciler<FlinkBlueGreenDeployment> {

    private static final Logger LOG = LoggerFactory.getLogger(FlinkDeploymentController.class);

    private final FlinkResourceContextFactory ctxFactory;
    private final BlueGreenStateHandlerRegistry handlerRegistry;

    public static long minimumAbortGracePeriodMs = ABORT_GRACE_PERIOD.defaultValue().toMillis();

    public FlinkBlueGreenDeploymentController(FlinkResourceContextFactory ctxFactory) {
        this.ctxFactory = ctxFactory;
        this.handlerRegistry = new BlueGreenStateHandlerRegistry();
    }

    @Override
    public List<EventSource<?, FlinkBlueGreenDeployment>> prepareEventSources(
            EventSourceContext<FlinkBlueGreenDeployment> context) {
        List<EventSource<?, FlinkBlueGreenDeployment>> eventSources = new ArrayList<>();

        InformerEventSourceConfiguration<FlinkDeployment> config =
                InformerEventSourceConfiguration.from(
                                FlinkDeployment.class, FlinkBlueGreenDeployment.class)
                        .withSecondaryToPrimaryMapper(
                                Mappers.fromOwnerReferences(context.getPrimaryResourceClass()))
                        .withNamespacesInheritedFromController()
                        .withFollowControllerNamespacesChanges(true)
                        .build();

        eventSources.add(new InformerEventSource<>(config, context));

        return eventSources;
    }

    @Override
    public UpdateControl<FlinkBlueGreenDeployment> reconcile(
            FlinkBlueGreenDeployment bgDeployment, Context<FlinkBlueGreenDeployment> josdkContext)
            throws Exception {

        FlinkBlueGreenDeploymentStatus deploymentStatus = bgDeployment.getStatus();

        if (deploymentStatus == null) {
            var context =
                    new BlueGreenContext(
                            bgDeployment,
                            new FlinkBlueGreenDeploymentStatus(),
                            josdkContext,
                            null,
                            ctxFactory);
            return BlueGreenDeploymentService.patchStatusUpdateControl(
                            context, INITIALIZING_BLUE, null)
                    .rescheduleAfter(100);
        } else {
            FlinkBlueGreenDeploymentState currentState = deploymentStatus.getBlueGreenState();
            var context =
                    new BlueGreenContext(
                            bgDeployment,
                            deploymentStatus,
                            josdkContext,
                            currentState == INITIALIZING_BLUE
                                    ? null
                                    : FlinkBlueGreenDeployments.fromSecondaryResources(
                                            josdkContext),
                            ctxFactory);

            LOG.debug(
                    "Processing state: {} for deployment: {}",
                    currentState,
                    context.getDeploymentName());

            BlueGreenStateHandler handler = handlerRegistry.getHandler(currentState);
            return handler.handle(context);
            //            return stateMachine.processState(context);
        }
    }

    public static void logAndThrow(String message) {
        throw new RuntimeException(message);
    }
}
