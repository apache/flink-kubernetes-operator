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

package org.apache.flink.kubernetes.operator.observer.deployment;

import org.apache.flink.kubernetes.operator.api.FlinkDeployment;
import org.apache.flink.kubernetes.operator.api.status.ReconciliationState;
import org.apache.flink.kubernetes.operator.controller.FlinkResourceContext;
import org.apache.flink.kubernetes.operator.utils.EventRecorder;

/** The observer of the {@link org.apache.flink.kubernetes.operator.api.Mode#SESSION} cluster. */
public class SessionObserver extends AbstractFlinkDeploymentObserver {

    public SessionObserver(EventRecorder eventRecorder) {
        super(eventRecorder);
    }

    @Override
    protected void observeJob(FlinkResourceContext<FlinkDeployment> ctx) {
        // A session cluster has no job of its own
        var rs = ctx.getResource().getStatus().getReconciliationStatus();
        if (rs.getState() == ReconciliationState.DEPLOYED) {
            rs.markReconciledSpecAsStable();
        }
    }
}
