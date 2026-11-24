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

package org.apache.flink.kubernetes.operator.service.minicluster;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.PipelineOptionsInternal;
import org.apache.flink.kubernetes.kubeclient.FlinkPod;
import org.apache.flink.kubernetes.kubeclient.decorators.AbstractKubernetesStepDecorator;
import org.apache.flink.kubernetes.kubeclient.parameters.KubernetesJobManagerParameters;
import org.apache.flink.kubernetes.operator.api.spec.JobSpec;
import org.apache.flink.kubernetes.shaded.io.fabric8.kubernetes.api.model.ContainerBuilder;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Runs the Flink {@code bin/minicluster.sh} launcher in the main container, through the image
 * entrypoint in the same way as a FlinkDeployment's JobManager, so that it runs as the flink user
 * rather than root in the Flink images, and so ENABLE_BUILT_IN_PLUGINS and jemalloc are consistent.
 */
class CmdMiniClusterDecorator extends AbstractKubernetesStepDecorator {

    @VisibleForTesting static final String LAUNCHER_PATH = "/opt/flink/bin/minicluster.sh";

    private final KubernetesJobManagerParameters kubernetesJobManagerParameters;
    private final JobSpec job;

    CmdMiniClusterDecorator(
            KubernetesJobManagerParameters kubernetesJobManagerParameters, JobSpec job) {
        this.kubernetesJobManagerParameters = checkNotNull(kubernetesJobManagerParameters);
        this.job = checkNotNull(job);
    }

    @Override
    public FlinkPod decorateFlinkPod(FlinkPod flinkPod) {
        var mainContainer =
                new ContainerBuilder(flinkPod.getMainContainer())
                        .withCommand(kubernetesJobManagerParameters.getContainerEntrypoint())
                        .withArgs(
                                getLauncherArgs(
                                        job,
                                        kubernetesJobManagerParameters.getFlinkConfiguration()))
                        .addNewEnv()
                        .withName(ConfigConstants.ENV_FLINK_CONF_DIR)
                        .withValue(kubernetesJobManagerParameters.getFlinkConfDirInPod())
                        .endEnv()
                        .build();
        return new FlinkPod.Builder(flinkPod).withMainContainer(mainContainer).build();
    }

    /**
     * Arguments for {@code bin/minicluster.sh}.
     *
     * <p>Config that identifies the job is read from named options in the command line: job jar,
     * entry class, and fixed JobID.
     *
     * <p>Everything else, including the restore path, is read from the config file. The job's own
     * arguments go after {@code --} to separate them from launcher options.
     */
    @VisibleForTesting
    static List<String> getLauncherArgs(JobSpec job, Configuration deployConfig) {
        var args = new ArrayList<String>();
        args.add(LAUNCHER_PATH);
        if (job.getJarURI() != null) {
            args.add("--jars");
            args.add(job.getJarURI());
        }
        if (job.getEntryClass() != null) {
            args.add("--job-classname");
            args.add(job.getEntryClass());
        }
        deployConfig
                .getOptional(PipelineOptionsInternal.PIPELINE_FIXED_JOB_ID)
                .ifPresent(
                        jobId -> {
                            args.add("--job-id");
                            args.add(jobId);
                        });
        if (job.getArgs() != null && job.getArgs().length > 0) {
            args.add("--");
            Collections.addAll(args, job.getArgs());
        }
        return args;
    }
}
