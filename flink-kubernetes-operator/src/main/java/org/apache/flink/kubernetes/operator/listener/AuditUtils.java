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

import org.apache.flink.kubernetes.operator.api.AbstractFlinkResource;
import org.apache.flink.kubernetes.operator.api.status.CommonStatus;

import io.fabric8.kubernetes.api.model.Event;
import lombok.NonNull;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Responsible for logging resource event/status updates. */
public class AuditUtils {
    private static final Logger LOG = LoggerFactory.getLogger(AuditUtils.class);

    public static <R extends AbstractFlinkResource<?, S>, S extends CommonStatus<?>>
            void logContext(FlinkResourceListener.StatusUpdateContext<R, S> ctx) {
        LOG.info(format(ctx.getNewStatus()));
    }

    public static <R extends AbstractFlinkResource<?, ?>> void logContext(
            FlinkResourceListener.ResourceEventContext<R> ctx) {
        LOG.info(format(ctx.getEvent()));
    }

    private static String format(@NonNull CommonStatus<?> status) {
        var lifeCycleState = status.getLifecycleState();
        return String.format(
                ">>> Status | %-7s | %-15s | %s ",
                StringUtils.isEmpty(status.getError()) ? "Info" : "Error",
                lifeCycleState,
                StringUtils.isEmpty(status.getError())
                        ? lifeCycleState.getDescription()
                        : status.getError());
    }

    private static String format(@NonNull Event event) {
        return String.format(
                ">>> Event  | %-7s | %-15s | %s",
                event.getType().equals("Normal") ? "Info" : event.getType(),
                event.getReason().toUpperCase(),
                event.getMessage());
    }
}
