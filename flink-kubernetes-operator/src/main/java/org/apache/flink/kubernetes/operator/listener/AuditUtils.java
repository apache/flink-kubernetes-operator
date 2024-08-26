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

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.kubernetes.operator.api.AbstractFlinkResource;
import org.apache.flink.kubernetes.operator.api.listener.FlinkResourceListener;
import org.apache.flink.kubernetes.operator.api.status.CommonStatus;
import org.apache.flink.kubernetes.operator.api.status.FlinkStateSnapshotStatus;

import io.fabric8.kubernetes.api.model.Event;
import lombok.NonNull;
import org.apache.commons.lang3.ObjectUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Responsible for logging resource event/status updates. */
public class AuditUtils {
    private static final Logger LOG = LoggerFactory.getLogger(AuditUtils.class);

    public static void logContext(FlinkResourceListener.FlinkStateSnapshotStatusUpdateContext ctx) {
        LOG.info(format(ctx.getNewStatus()));
    }

    public static void logContext(FlinkResourceListener.FlinkStateSnapshotEventContext ctx) {
        LOG.info(format(ctx.getEvent(), "Snapshot"));
    }

    public static <R extends AbstractFlinkResource<?, S>, S extends CommonStatus<?>>
            void logContext(FlinkResourceListener.StatusUpdateContext<R, S> ctx) {
        if (ctx.getPreviousStatus().getLifecycleState() == ctx.getNewStatus().getLifecycleState()) {
            // Unchanged state, nothing to log
            return;
        }
        LOG.info(format(ctx.getNewStatus()));
    }

    public static <R extends AbstractFlinkResource<?, ?>> void logContext(
            FlinkResourceListener.ResourceEventContext<R> ctx) {
        LOG.info(format(ctx.getEvent(), "Job"));
    }

    private static String format(@NonNull CommonStatus<?> status) {
        var lifeCycleState = status.getLifecycleState();
        return String.format(
                ">>> %-16s | %-7s | %-15s | %s ",
                "Status[Job]",
                StringUtils.isEmpty(status.getError()) ? "Info" : "Error",
                lifeCycleState,
                StringUtils.isEmpty(status.getError())
                        ? lifeCycleState.getDescription()
                        : status.getError());
    }

    private static String format(@NonNull FlinkStateSnapshotStatus status) {
        String message = ObjectUtils.firstNonNull(status.getError(), status.getPath(), "");
        return String.format(
                ">>> %-16s | %-7s | %s", "Status[Snapshot]", status.getState(), message);
    }

    @VisibleForTesting
    public static String format(@NonNull Event event, String component) {
        var componentMessage = String.format("Event[%s]", component);
        return String.format(
                ">>> %-16s | %-7s | %-15s | %s",
                componentMessage,
                event.getType().equals("Normal") ? "Info" : event.getType(),
                event.getReason().toUpperCase(),
                event.getMessage());
    }
}
