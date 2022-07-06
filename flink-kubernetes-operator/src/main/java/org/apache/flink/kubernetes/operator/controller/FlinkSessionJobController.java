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

import org.apache.flink.kubernetes.operator.config.FlinkConfigManager;
import org.apache.flink.kubernetes.operator.crd.FlinkDeployment;
import org.apache.flink.kubernetes.operator.crd.FlinkSessionJob;
import org.apache.flink.kubernetes.operator.crd.status.FlinkSessionJobStatus;
import org.apache.flink.kubernetes.operator.exception.ReconciliationException;
import org.apache.flink.kubernetes.operator.observer.Observer;
import org.apache.flink.kubernetes.operator.reconciler.Reconciler;
import org.apache.flink.kubernetes.operator.reconciler.ReconciliationUtils;
import org.apache.flink.kubernetes.operator.utils.EventSourceUtils;
import org.apache.flink.kubernetes.operator.utils.StatusRecorder;
import org.apache.flink.kubernetes.operator.validation.FlinkResourceValidator;

import io.javaoperatorsdk.operator.api.reconciler.Cleaner;
import io.javaoperatorsdk.operator.api.reconciler.Context;
import io.javaoperatorsdk.operator.api.reconciler.ControllerConfiguration;
import io.javaoperatorsdk.operator.api.reconciler.DeleteControl;
import io.javaoperatorsdk.operator.api.reconciler.ErrorStatusHandler;
import io.javaoperatorsdk.operator.api.reconciler.ErrorStatusUpdateControl;
import io.javaoperatorsdk.operator.api.reconciler.EventSourceContext;
import io.javaoperatorsdk.operator.api.reconciler.EventSourceInitializer;
import io.javaoperatorsdk.operator.api.reconciler.UpdateControl;
import io.javaoperatorsdk.operator.processing.event.source.EventSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Optional;
import java.util.Set;

/** Controller that runs the main reconcile loop for {@link FlinkSessionJob}. */
@ControllerConfiguration()
public class FlinkSessionJobController
        implements io.javaoperatorsdk.operator.api.reconciler.Reconciler<FlinkSessionJob>,
                ErrorStatusHandler<FlinkSessionJob>,
                EventSourceInitializer<FlinkSessionJob>,
                Cleaner<FlinkSessionJob> {

    private static final Logger LOG = LoggerFactory.getLogger(FlinkSessionJobController.class);

    private final FlinkConfigManager configManager;
    private final Set<FlinkResourceValidator> validators;
    private final Reconciler<FlinkSessionJob> reconciler;
    private final Observer<FlinkSessionJob> observer;
    private final StatusRecorder<FlinkSessionJobStatus> statusRecorder;

    public FlinkSessionJobController(
            FlinkConfigManager configManager,
            Set<FlinkResourceValidator> validators,
            Reconciler<FlinkSessionJob> reconciler,
            Observer<FlinkSessionJob> observer,
            StatusRecorder<FlinkSessionJobStatus> statusRecorder) {
        this.configManager = configManager;
        this.validators = validators;
        this.reconciler = reconciler;
        this.observer = observer;
        this.statusRecorder = statusRecorder;
    }

    @Override
    public UpdateControl<FlinkSessionJob> reconcile(
            FlinkSessionJob flinkSessionJob, Context context) {
        LOG.info("Starting reconciliation");

        statusRecorder.updateStatusFromCache(flinkSessionJob);
        FlinkSessionJob previousJob = ReconciliationUtils.clone(flinkSessionJob);

        observer.observe(flinkSessionJob, context);
        if (!validateSessionJob(flinkSessionJob, context)) {
            statusRecorder.patchAndCacheStatus(flinkSessionJob);
            return ReconciliationUtils.toUpdateControl(
                    configManager.getOperatorConfiguration(), flinkSessionJob, previousJob, false);
        }

        try {
            statusRecorder.patchAndCacheStatus(flinkSessionJob);
            reconciler.reconcile(flinkSessionJob, context);
        } catch (Exception e) {
            throw new ReconciliationException(e);
        }
        statusRecorder.patchAndCacheStatus(flinkSessionJob);
        return ReconciliationUtils.toUpdateControl(
                configManager.getOperatorConfiguration(), flinkSessionJob, previousJob, true);
    }

    @Override
    public DeleteControl cleanup(FlinkSessionJob sessionJob, Context context) {
        LOG.info("Deleting FlinkSessionJob");
        statusRecorder.removeCachedStatus(sessionJob);
        return reconciler.cleanup(sessionJob, context);
    }

    @Override
    public ErrorStatusUpdateControl<FlinkSessionJob> updateErrorStatus(
            FlinkSessionJob sessionJob, Context<FlinkSessionJob> context, Exception e) {
        return ReconciliationUtils.toErrorStatusUpdateControl(
                sessionJob, context.getRetryInfo(), e, statusRecorder);
    }

    @Override
    public Map<String, EventSource> prepareEventSources(
            EventSourceContext<FlinkSessionJob> context) {
        return EventSourceInitializer.nameEventSources(
                EventSourceUtils.getFlinkDeploymentInformerEventSource(context));
    }

    private boolean validateSessionJob(FlinkSessionJob sessionJob, Context context) {
        for (FlinkResourceValidator validator : validators) {
            Optional<String> validationError =
                    validator.validateSessionJob(
                            sessionJob, context.getSecondaryResource(FlinkDeployment.class));
            if (validationError.isPresent()) {
                return ReconciliationUtils.applyValidationErrorAndResetSpec(
                        sessionJob, validationError.get());
            }
        }
        return true;
    }
}
