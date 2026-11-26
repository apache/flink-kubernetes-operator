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
import org.apache.flink.kubernetes.operator.api.status.TaskManagerInfo;
import org.apache.flink.kubernetes.operator.controller.FlinkResourceContext;
import org.apache.flink.kubernetes.operator.observer.AbstractClusterDeploymentObserver;
import org.apache.flink.kubernetes.operator.utils.EventRecorder;
import org.apache.flink.kubernetes.operator.utils.FlinkUtils;

import io.fabric8.kubernetes.api.model.PodList;

/** Base observer for session and application clusters. */
public abstract class AbstractFlinkDeploymentObserver
        extends AbstractClusterDeploymentObserver<FlinkDeployment> {

    public AbstractFlinkDeploymentObserver(EventRecorder eventRecorder) {
        super(eventRecorder);
    }

    @Override
    protected void observeClusterInfo(FlinkResourceContext<FlinkDeployment> ctx) {
        var flinkApp = ctx.getResource();
        try {
            var clusterInfo =
                    ctx.getFlinkService()
                            .getClusterInfo(
                                    ctx.getObserveConfig(),
                                    flinkApp.getStatus().getJobStatus().getJobId());
            flinkApp.getStatus().getClusterInfo().putAll(clusterInfo.getInfo());
            logger.debug("ClusterInfo: {}", flinkApp.getStatus().getClusterInfo());

            var labelSelector =
                    FlinkUtils.getTaskManagerLabelSelector(flinkApp.getMetadata().getName());
            flinkApp.getStatus()
                    .setTaskManager(
                            new TaskManagerInfo(
                                    labelSelector, clusterInfo.getTaskManagerReplicas()));
        } catch (Exception e) {
            logger.warn("Exception while fetching cluster info", e);
        }
    }

    @Override
    protected PodList getJmPodList(FlinkResourceContext<FlinkDeployment> ctx) {
        return ctx.getFlinkService().getJmPodList(ctx.getResource(), ctx.getObserveConfig());
    }

    @Override
    protected String getJmDescription() {
        return "JobManager";
    }
}
