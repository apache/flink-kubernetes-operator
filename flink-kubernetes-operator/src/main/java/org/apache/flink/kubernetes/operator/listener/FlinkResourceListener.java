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

package org.apache.flink.kubernetes.operator.listener;

import org.apache.flink.core.plugin.Plugin;
import org.apache.flink.kubernetes.operator.crd.AbstractFlinkResource;
import org.apache.flink.kubernetes.operator.crd.FlinkDeployment;
import org.apache.flink.kubernetes.operator.crd.FlinkSessionJob;
import org.apache.flink.kubernetes.operator.crd.status.CommonStatus;
import org.apache.flink.kubernetes.operator.crd.status.FlinkDeploymentStatus;
import org.apache.flink.kubernetes.operator.crd.status.FlinkSessionJobStatus;

import io.fabric8.kubernetes.api.model.Event;
import io.fabric8.kubernetes.client.KubernetesClient;

/** Listener interface for Flink resource related events and status changes. */
public interface FlinkResourceListener extends Plugin {

    void onDeploymentStatusUpdate(StatusUpdateContext<FlinkDeployment, FlinkDeploymentStatus> ctx);

    void onDeploymentEvent(ResourceEventContext<FlinkDeployment> ctx);

    void onSessionJobStatusUpdate(StatusUpdateContext<FlinkSessionJob, FlinkSessionJobStatus> ctx);

    void onSessionJobEvent(ResourceEventContext<FlinkSessionJob> ctx);

    /** Base for Resource Event and StatusUpdate contexts. */
    interface ResourceContext<R extends AbstractFlinkResource<?, ?>> {
        R getFlinkResource();

        KubernetesClient getKubernetesClient();
    }

    /** Context for Resource Event listener methods. */
    interface ResourceEventContext<R extends AbstractFlinkResource<?, ?>>
            extends ResourceContext<R> {
        Event getEvent();
    }

    /** Context for Status listener methods. */
    interface StatusUpdateContext<R extends AbstractFlinkResource<?, S>, S extends CommonStatus<?>>
            extends ResourceContext<R> {

        default S getNewStatus() {
            return getFlinkResource().getStatus();
        }

        S getPreviousStatus();
    }
}
