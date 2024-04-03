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

package org.apache.flink.kubernetes.operator.api.listener;

import org.apache.flink.core.plugin.Plugin;
import org.apache.flink.kubernetes.operator.api.AbstractFlinkResource;
import org.apache.flink.kubernetes.operator.api.FlinkDeployment;
import org.apache.flink.kubernetes.operator.api.FlinkSessionJob;
import org.apache.flink.kubernetes.operator.api.FlinkStateSnapshot;
import org.apache.flink.kubernetes.operator.api.status.CommonStatus;
import org.apache.flink.kubernetes.operator.api.status.FlinkDeploymentStatus;
import org.apache.flink.kubernetes.operator.api.status.FlinkSessionJobStatus;
import org.apache.flink.kubernetes.operator.api.status.FlinkStateSnapshotStatus;

import io.fabric8.kubernetes.api.model.Event;
import io.fabric8.kubernetes.client.KubernetesClient;

import java.time.Instant;

/** Listener interface for Flink resource related events and status changes. */
public interface FlinkResourceListener extends Plugin {

    void onDeploymentStatusUpdate(StatusUpdateContext<FlinkDeployment, FlinkDeploymentStatus> ctx);

    void onDeploymentEvent(ResourceEventContext<FlinkDeployment> ctx);

    void onSessionJobStatusUpdate(StatusUpdateContext<FlinkSessionJob, FlinkSessionJobStatus> ctx);

    void onSessionJobEvent(ResourceEventContext<FlinkSessionJob> ctx);

    /**
     * Called when a new FlinkStateSnapshot event is triggered.
     *
     * @param ctx Context of the event and resource
     */
    void onStateSnapshotEvent(FlinkStateSnapshotEventContext ctx);

    /**
     * Called when a FlinkStateSnapshot status gets updated.
     *
     * @param ctx Context of the new status and resource
     */
    void onStateSnapshotStatusUpdate(FlinkStateSnapshotStatusUpdateContext ctx);

    /** Base for Resource Event and StatusUpdate contexts. */
    interface ResourceContext {
        KubernetesClient getKubernetesClient();

        Instant getTimestamp();
    }

    /** Base for Flink resource Event and StatusUpdate contexts. */
    interface FlinkResourceContext<R extends AbstractFlinkResource<?, ?>> extends ResourceContext {
        R getFlinkResource();
    }

    /** Context for Flink resource Event listener methods. */
    interface ResourceEventContext<R extends AbstractFlinkResource<?, ?>>
            extends FlinkResourceContext<R> {
        Event getEvent();

        @Override
        default Instant getTimestamp() {
            return Instant.parse(getEvent().getLastTimestamp());
        }
    }

    /** Context for Flink resource Status listener methods. */
    interface StatusUpdateContext<R extends AbstractFlinkResource<?, S>, S extends CommonStatus<?>>
            extends FlinkResourceContext<R> {

        default S getNewStatus() {
            return getFlinkResource().getStatus();
        }

        S getPreviousStatus();
    }

    /** Context for FlinkStateSnapshot Status listener methods. */
    interface FlinkStateSnapshotStatusUpdateContext extends FlinkStateSnapshotResourceContext {

        default FlinkStateSnapshotStatus getNewStatus() {
            return getStateSnapshot().getStatus();
        }

        FlinkStateSnapshotStatus getPreviousStatus();
    }

    /** Context for FlinkStateSnapshot Event listener methods. */
    interface FlinkStateSnapshotEventContext extends FlinkStateSnapshotResourceContext {
        Event getEvent();

        @Override
        default Instant getTimestamp() {
            return Instant.parse(getEvent().getLastTimestamp());
        }
    }

    /** Base for FlinkStateSnapshot Event and StatusUpdate contexts. */
    interface FlinkStateSnapshotResourceContext extends ResourceContext {
        FlinkStateSnapshot getStateSnapshot();
    }
}
