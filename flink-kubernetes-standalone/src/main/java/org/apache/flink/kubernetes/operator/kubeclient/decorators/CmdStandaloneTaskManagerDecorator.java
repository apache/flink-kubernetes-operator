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

package org.apache.flink.kubernetes.operator.kubeclient.decorators;

import org.apache.flink.kubernetes.kubeclient.FlinkPod;
import org.apache.flink.kubernetes.kubeclient.decorators.AbstractKubernetesStepDecorator;
import org.apache.flink.kubernetes.operator.kubeclient.parameters.StandaloneKubernetesTaskManagerParameters;
import org.apache.flink.kubernetes.shaded.io.fabric8.kubernetes.api.model.Container;
import org.apache.flink.kubernetes.shaded.io.fabric8.kubernetes.api.model.ContainerBuilder;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Attach the command and args to the main container for running the TaskManager in standalone mode.
 */
public class CmdStandaloneTaskManagerDecorator extends AbstractKubernetesStepDecorator {
    public static final String TASKMANAGER_ENTRYPOINT_ARG = "taskmanager";

    private final StandaloneKubernetesTaskManagerParameters kubernetesTaskManagerParameters;

    public CmdStandaloneTaskManagerDecorator(
            StandaloneKubernetesTaskManagerParameters kubernetesTaskManagerParameters) {
        this.kubernetesTaskManagerParameters = checkNotNull(kubernetesTaskManagerParameters);
    }

    @Override
    public FlinkPod decorateFlinkPod(FlinkPod flinkPod) {
        final Container mainContainerWithStartCmd =
                new ContainerBuilder(flinkPod.getMainContainer())
                        .withCommand(kubernetesTaskManagerParameters.getContainerEntrypoint())
                        .withArgs(TASKMANAGER_ENTRYPOINT_ARG)
                        .build();
        return new FlinkPod.Builder(flinkPod).withMainContainer(mainContainerWithStartCmd).build();
    }
}
