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

package org.apache.flink.kubernetes.operator.reconciler.deployment;

import org.apache.flink.autoscaler.JobAutoScalerContext;
import org.apache.flink.autoscaler.handler.AutoScalerEventHandler;
import org.apache.flink.kubernetes.operator.api.AbstractFlinkResource;
import org.apache.flink.kubernetes.operator.utils.EventRecorder;

import io.fabric8.kubernetes.client.KubernetesClient;
import io.javaoperatorsdk.operator.processing.event.ResourceID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/** The kubernetes auto scaler event handler. */
public class KubernetesAutoScalerEventHandler<CR extends AbstractFlinkResource<?, ?>>
        implements AutoScalerEventHandler<ResourceID, CR> {

    private static final Logger LOG =
            LoggerFactory.getLogger(KubernetesAutoScalerEventHandler.class);

    private final KubernetesClient kubernetesClient;

    private final EventRecorder eventRecorder;

    public KubernetesAutoScalerEventHandler(
            KubernetesClient kubernetesClient, EventRecorder eventRecorder) {
        this.kubernetesClient = kubernetesClient;
        this.eventRecorder = eventRecorder;
    }

    @Override
    public void handlerScalingFailure(
            JobAutoScalerContext<ResourceID, CR> context,
            FailureReason failureReason,
            String errorMessage) {
        eventRecorder.triggerEvent(
                context.getExtraJobInfo(),
                failureReason.isError() ? EventRecorder.Type.Warning : EventRecorder.Type.Normal,
                failureReason.toString(),
                errorMessage,
                EventRecorder.Component.Operator);
    }

    @Override
    public void handlerScalingReport(
            JobAutoScalerContext<ResourceID, CR> context, String scalingReportMessage) {
        eventRecorder.triggerEvent(
                context.getExtraJobInfo(),
                EventRecorder.Type.Normal,
                EventRecorder.Reason.ScalingReport,
                EventRecorder.Component.Operator,
                scalingReportMessage,
                "ScalingExecutor");
    }

    @Override
    public void handlerRecommendedParallelism(
            JobAutoScalerContext<ResourceID, CR> context,
            Map<String, String> recommendedParallelism) {}
}
