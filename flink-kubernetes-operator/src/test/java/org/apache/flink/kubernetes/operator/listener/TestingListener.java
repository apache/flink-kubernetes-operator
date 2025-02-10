/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

import org.apache.flink.configuration.Configuration;
import org.apache.flink.kubernetes.operator.api.FlinkDeployment;
import org.apache.flink.kubernetes.operator.api.FlinkSessionJob;
import org.apache.flink.kubernetes.operator.api.listener.FlinkResourceListener;
import org.apache.flink.kubernetes.operator.api.status.FlinkDeploymentStatus;
import org.apache.flink.kubernetes.operator.api.status.FlinkSessionJobStatus;

import java.util.ArrayList;
import java.util.List;

/** Listener implementation for testing. */
public class TestingListener implements FlinkResourceListener {

    public List<StatusUpdateContext<?, ?>> flinkResourceUpdates = new ArrayList<>();
    public List<ResourceEventContext<?>> flinkResourceEvents = new ArrayList<>();
    public List<FlinkStateSnapshotStatusUpdateContext> flinkStateSnapshotUpdates =
            new ArrayList<>();
    public List<FlinkStateSnapshotEventContext> flinkStateSnapshotEvents = new ArrayList<>();
    public Configuration config;

    @Override
    public void onDeploymentStatusUpdate(
            StatusUpdateContext<FlinkDeployment, FlinkDeploymentStatus> ctx) {
        flinkResourceUpdates.add(ctx);
    }

    @Override
    public void onDeploymentEvent(ResourceEventContext<FlinkDeployment> ctx) {
        flinkResourceEvents.add(ctx);
    }

    @Override
    public void onSessionJobStatusUpdate(
            StatusUpdateContext<FlinkSessionJob, FlinkSessionJobStatus> ctx) {
        flinkResourceUpdates.add(ctx);
    }

    @Override
    public void onSessionJobEvent(ResourceEventContext<FlinkSessionJob> ctx) {
        flinkResourceEvents.add(ctx);
    }

    @Override
    public void onStateSnapshotEvent(FlinkStateSnapshotEventContext ctx) {
        flinkStateSnapshotEvents.add(ctx);
    }

    @Override
    public void onStateSnapshotStatusUpdate(FlinkStateSnapshotStatusUpdateContext ctx) {
        flinkStateSnapshotUpdates.add(ctx);
    }

    @Override
    public void configure(Configuration config) {
        this.config = config;
    }
}
