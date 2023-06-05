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
import org.apache.flink.kubernetes.operator.kubeclient.parameters.StandaloneKubernetesJobManagerParameters;
import org.apache.flink.kubernetes.shaded.io.fabric8.kubernetes.api.model.Container;
import org.apache.flink.kubernetes.shaded.io.fabric8.kubernetes.api.model.ContainerBuilder;
import org.apache.flink.kubernetes.utils.Constants;

import java.util.ArrayList;
import java.util.List;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Attach the command and args to the main container for running the JobManager in standalone mode.
 */
public class CmdStandaloneJobManagerDecorator extends AbstractKubernetesStepDecorator {

    public static final String JOBMANAGER_ENTRYPOINT_ARG = "jobmanager";
    public static final String APPLICATION_MODE_ARG = "standalone-job";
    public static final String POD_IP_ARG =
            String.format("$(%s)", Constants.ENV_FLINK_POD_IP_ADDRESS);

    private final StandaloneKubernetesJobManagerParameters kubernetesJobManagerParameters;

    public CmdStandaloneJobManagerDecorator(
            StandaloneKubernetesJobManagerParameters kubernetesJobManagerParameters) {
        this.kubernetesJobManagerParameters = checkNotNull(kubernetesJobManagerParameters);
    }

    @Override
    public FlinkPod decorateFlinkPod(FlinkPod flinkPod) {
        final Container mainContainerWithStartCmd;
        if (kubernetesJobManagerParameters.isApplicationCluster()) {
            mainContainerWithStartCmd = decorateApplicationContainer(flinkPod.getMainContainer());
        } else {
            mainContainerWithStartCmd = decorateSessionContainer(flinkPod.getMainContainer());
        }
        return new FlinkPod.Builder(flinkPod).withMainContainer(mainContainerWithStartCmd).build();
    }

    private Container decorateSessionContainer(Container mainContainer) {
        ContainerBuilder containerBuilder =
                new ContainerBuilder(mainContainer)
                        .withCommand(kubernetesJobManagerParameters.getContainerEntrypoint())
                        .addToArgs(JOBMANAGER_ENTRYPOINT_ARG);

        if (kubernetesJobManagerParameters.isHAEnabled()) {
            containerBuilder.addToArgs(POD_IP_ARG);
        }

        return containerBuilder.build();
    }

    private Container decorateApplicationContainer(Container mainContainer) {
        return new ContainerBuilder(mainContainer)
                .withCommand(kubernetesJobManagerParameters.getContainerEntrypoint())
                .withArgs(getApplicationClusterArgs())
                .build();
    }

    private List<String> getApplicationClusterArgs() {
        List<String> args = new ArrayList<>();
        args.add(APPLICATION_MODE_ARG);

        String mainClass = kubernetesJobManagerParameters.getMainClass();
        if (mainClass != null) {
            args.add("--job-classname");
            args.add(mainClass);
        }

        Boolean allowNonRestoredState = kubernetesJobManagerParameters.getAllowNonRestoredState();
        if (allowNonRestoredState != null && allowNonRestoredState) {
            args.add("--allowNonRestoredState");
        }

        String savepointPath = kubernetesJobManagerParameters.getSavepointPath();
        if (savepointPath != null) {
            args.add("--fromSavepoint");
            args.add(savepointPath);
        }

        if (kubernetesJobManagerParameters.isHAEnabled()) {
            args.add("--host");
            args.add(POD_IP_ARG);
        }

        List<String> jobSpecArgs = kubernetesJobManagerParameters.getJobSpecArgs();
        if (jobSpecArgs != null) {
            args.addAll(kubernetesJobManagerParameters.getJobSpecArgs());
        }

        return args;
    }
}
