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

package org.apache.flink.kubernetes.operator;

import org.apache.flink.kubernetes.operator.hooks.FlinkResourceHook;
import org.apache.flink.kubernetes.operator.hooks.FlinkResourceHookStatus;
import org.apache.flink.kubernetes.operator.hooks.FlinkResourceHookType;
import org.apache.flink.kubernetes.operator.hooks.FlinkResourceHooksManager;
import org.apache.flink.kubernetes.operator.utils.EventCollector;
import org.apache.flink.kubernetes.operator.utils.EventRecorder;

import lombok.Getter;
import lombok.Setter;

import java.util.Collections;

/** Test implementation of FlinkResourceHooksManager that allows controlling hook behavior. */
public class TestFlinkResourceHooksManager extends FlinkResourceHooksManager {
    @Setter private FlinkResourceHookStatus hookStatus;
    @Getter private int executionCount = 0;

    private final EventRecorder recorder;

    public TestFlinkResourceHooksManager(EventRecorder eventRecorder) {
        super(Collections.emptyList(), new EventRecorder(new EventCollector()));
        this.recorder = eventRecorder;
    }

    @Override
    public FlinkResourceHookStatus executeAllHooks(
            FlinkResourceHookType hookType, FlinkResourceHook.FlinkResourceHookContext context) {
        executionCount++;
        switch (hookStatus.getStatus()) {
            case PENDING:
                recorder.triggerEvent(
                        context.getFlinkSessionJob(),
                        EventRecorder.Type.Normal,
                        EventRecorder.Reason.FlinkResourceHookFinished,
                        EventRecorder.Component.Job,
                        "Test hook pending",
                        context.getKubernetesClient());
                break;
            case COMPLETED:
                recorder.triggerEvent(
                        context.getFlinkSessionJob(),
                        EventRecorder.Type.Normal,
                        EventRecorder.Reason.FlinkResourceHookFinished,
                        EventRecorder.Component.Job,
                        "Test hook completed",
                        context.getKubernetesClient());
                break;
            case FAILED:
                recorder.triggerEvent(
                        context.getFlinkSessionJob(),
                        EventRecorder.Type.Warning,
                        EventRecorder.Reason.FlinkResourceHookFailed,
                        EventRecorder.Component.Job,
                        "Test hook failed",
                        context.getKubernetesClient());
                break;
            default:
                break;
        }
        return hookStatus;
    }
}
