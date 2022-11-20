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

package org.apache.flink.kubernetes.operator.observer;

import org.apache.flink.kubernetes.operator.api.AbstractFlinkResource;
import org.apache.flink.kubernetes.operator.api.status.ReconciliationState;
import org.apache.flink.kubernetes.operator.config.FlinkConfigManager;
import org.apache.flink.kubernetes.operator.reconciler.ReconciliationUtils;
import org.apache.flink.kubernetes.operator.utils.EventRecorder;
import org.apache.flink.kubernetes.operator.utils.SavepointUtils;

import io.javaoperatorsdk.operator.api.reconciler.Context;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Base observer for all Flink resources. */
public abstract class AbstractFlinkResourceObserver<
                CR extends AbstractFlinkResource<?, ?>, CTX extends ObserverContext>
        implements Observer<CR> {

    protected final Logger logger = LoggerFactory.getLogger(this.getClass());

    protected final FlinkConfigManager configManager;
    protected final EventRecorder eventRecorder;

    public AbstractFlinkResourceObserver(
            FlinkConfigManager configManager, EventRecorder eventRecorder) {
        this.configManager = configManager;
        this.eventRecorder = eventRecorder;
    }

    @Override
    public final void observe(CR resource, Context<?> context) {
        var observerContext = getObserverContext(resource, context);

        if (!isResourceReadyToBeObserved(resource, context, observerContext)) {
            return;
        }

        // If the CR has lingering savepoint trigger data at deprecated fields, migrate them.
        if (SavepointUtils.savepointInProgress(resource.getStatus().getJobStatus())) {
            SavepointUtils.checkAndMigrateDeprecatedTriggerFields(
                    resource.getStatus().getJobStatus().getSavepointInfo(),
                    resource.getSpec().getJob().getSavepointTriggerNonce());
        }

        // Trigger resource specific observe logic
        observeInternal(resource, context, observerContext);

        SavepointUtils.resetTriggerIfJobNotRunning(resource, eventRecorder);
    }

    /**
     * Get the observer context for the current resource.
     *
     * @param resource Resource being observed
     * @param context Resource context
     * @return Observer context
     */
    protected abstract CTX getObserverContext(CR resource, Context<?> context);

    /**
     * Check whether the resource should be observed. In certain states such as suspended
     * applications or in-progress upgrades and rollbacks, observing is not necessary.
     *
     * @param resource Current resource
     * @param resourceContext Resource context
     * @param observerContext Observer context
     * @return True if we should observe the resource
     */
    protected boolean isResourceReadyToBeObserved(
            CR resource, Context<?> resourceContext, CTX observerContext) {
        var reconciliationStatus = resource.getStatus().getReconciliationStatus();

        if (reconciliationStatus.isBeforeFirstDeployment()) {
            logger.debug("Skipping observe before first deployment");
            return false;
        }

        if (reconciliationStatus.getState() == ReconciliationState.ROLLING_BACK) {
            logger.debug("Skipping observe during rollback operation");
            return false;
        }

        // We are in the middle or possibly right after an upgrade
        if (reconciliationStatus.getState() == ReconciliationState.UPGRADING) {
            // We must check if the upgrade went through without the status upgrade for some reason
            updateStatusToDeployedIfAlreadyUpgraded(resource, resourceContext, observerContext);
            if (reconciliationStatus.getState() == ReconciliationState.UPGRADING) {
                ReconciliationUtils.clearLastReconciledSpecIfFirstDeploy(resource);
                logger.debug("Skipping observe before resource is deployed during upgrade");
                return false;
            }
        }

        return true;
    }

    /**
     * Internal observer logic specific to each resource type.
     *
     * @param resource Resource to be observed
     * @param resourceContext Resource context
     * @param observerContext Observer context
     */
    protected abstract void observeInternal(
            CR resource, Context<?> resourceContext, CTX observerContext);

    /**
     * Checks a resource that is currently in the UPGRADING state whether it was already deployed
     * but we simply miss the status information. After comparing the target resource generation
     * with the one from the possible deployment if they match we update the status to the already
     * DEPLOYED state.
     *
     * @param resource Flink resource.
     * @param resourceContext Context for resource.
     * @param observerContext Context for observer.
     */
    protected abstract void updateStatusToDeployedIfAlreadyUpgraded(
            CR resource, Context<?> resourceContext, CTX observerContext);
}
