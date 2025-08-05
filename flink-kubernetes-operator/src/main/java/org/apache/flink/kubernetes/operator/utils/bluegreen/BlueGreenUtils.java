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

package org.apache.flink.kubernetes.operator.utils.bluegreen;

import org.apache.flink.kubernetes.operator.controller.bluegreen.BlueGreenContext;
import org.apache.flink.kubernetes.operator.controller.FlinkBlueGreenDeploymentController;

import java.time.Instant;

import static org.apache.flink.kubernetes.operator.api.spec.FlinkBlueGreenDeploymentConfigOptions.ABORT_GRACE_PERIOD;
import static org.apache.flink.kubernetes.operator.api.spec.FlinkBlueGreenDeploymentConfigOptions.DEPLOYMENT_DELETION_DELAY;
import static org.apache.flink.kubernetes.operator.api.spec.FlinkBlueGreenDeploymentConfigOptions.RECONCILIATION_RESCHEDULING_INTERVAL;

/** Utility methods for time calculations and general Blue/Green deployment operations. */
public class BlueGreenUtils {

    /**
     * Converts milliseconds to ISO instant string.
     *
     * @param millis the milliseconds since epoch
     * @return ISO instant string representation
     */
    public static String millisToInstantStr(long millis) {
        return Instant.ofEpochMilli(millis).toString();
    }

    /**
     * Converts ISO instant string to milliseconds.
     *
     * @param instant the ISO instant string
     * @return milliseconds since epoch, or 0 if instant is null
     */
    public static long instantStrToMillis(String instant) {
        if (instant == null) {
            return 0;
        }
        return Instant.parse(instant).toEpochMilli();
    }

    /**
     * Gets the reconciliation rescheduling interval for the Blue/Green deployment.
     *
     * @param context the Blue/Green transition context
     * @return reconciliation interval in milliseconds
     */
    public static long getReconciliationReschedInterval(BlueGreenContext context) {
        return Math.max(
                BlueGreenSpecUtils.getConfigOption(
                                context.getBgDeployment(), RECONCILIATION_RESCHEDULING_INTERVAL)
                        .toMillis(),
                0);
    }

    /**
     * Gets the deployment deletion delay for the Blue/Green deployment.
     *
     * @param context the Blue/Green transition context
     * @return deletion delay in milliseconds
     */
    public static long getDeploymentDeletionDelay(BlueGreenContext context) {
        return Math.max(
                BlueGreenSpecUtils.getConfigOption(
                                context.getBgDeployment(), DEPLOYMENT_DELETION_DELAY)
                        .toMillis(),
                0);
    }

    /**
     * Gets the abort grace period for the Blue/Green deployment.
     *
     * @param context the Blue/Green transition context
     * @return abort grace period in milliseconds
     */
    public static long getAbortGracePeriod(BlueGreenContext context) {
        long abortGracePeriod =
                BlueGreenSpecUtils.getConfigOption(context.getBgDeployment(), ABORT_GRACE_PERIOD)
                        .toMillis();
        return Math.max(
                abortGracePeriod, FlinkBlueGreenDeploymentController.minimumAbortGracePeriodMs);
    }

    /**
     * Sets the abort timestamp in the deployment status based on current time and grace period.
     *
     * @param context the Blue/Green transition context
     */
    public static void setAbortTimestamp(BlueGreenContext context) {
        context.getDeploymentStatus()
                .setAbortTimestamp(
                        millisToInstantStr(
                                System.currentTimeMillis() + getAbortGracePeriod(context)));
    }
}
