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

package org.apache.flink.kubernetes.operator.standalone;

import org.apache.flink.client.deployment.ClusterDeploymentException;
import org.apache.flink.client.deployment.ClusterDescriptor;
import org.apache.flink.client.deployment.ClusterRetrieveException;
import org.apache.flink.client.deployment.ClusterSpecification;
import org.apache.flink.client.deployment.application.ApplicationConfiguration;
import org.apache.flink.client.program.ClusterClient;
import org.apache.flink.client.program.ClusterClientProvider;
import org.apache.flink.client.program.rest.RestClusterClient;
import org.apache.flink.configuration.BlobServerOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.HighAvailabilityOptions;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.kubernetes.KubernetesClusterDescriptor;
import org.apache.flink.kubernetes.artifact.DefaultKubernetesArtifactUploader;
import org.apache.flink.kubernetes.configuration.KubernetesConfigOptions;
import org.apache.flink.kubernetes.kubeclient.Endpoint;
import org.apache.flink.kubernetes.kubeclient.FlinkKubeClientFactory;
import org.apache.flink.kubernetes.kubeclient.FlinkPod;
import org.apache.flink.kubernetes.kubeclient.KubernetesJobManagerSpecification;
import org.apache.flink.kubernetes.operator.kubeclient.FlinkStandaloneKubeClient;
import org.apache.flink.kubernetes.operator.kubeclient.factory.StandaloneKubernetesJobManagerFactory;
import org.apache.flink.kubernetes.operator.kubeclient.factory.StandaloneKubernetesTaskManagerFactory;
import org.apache.flink.kubernetes.operator.kubeclient.parameters.StandaloneKubernetesJobManagerParameters;
import org.apache.flink.kubernetes.operator.kubeclient.parameters.StandaloneKubernetesTaskManagerParameters;
import org.apache.flink.kubernetes.shaded.io.fabric8.kubernetes.api.model.apps.Deployment;
import org.apache.flink.kubernetes.utils.Constants;
import org.apache.flink.kubernetes.utils.KubernetesUtils;
import org.apache.flink.runtime.highavailability.HighAvailabilityServicesUtils;
import org.apache.flink.runtime.highavailability.nonha.standalone.StandaloneClientHAServices;
import org.apache.flink.runtime.jobmanager.HighAvailabilityMode;
import org.apache.flink.runtime.rpc.AddressResolution;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Optional;

import static org.apache.flink.util.Preconditions.checkNotNull;

/** Standalone Kubernetes specific {@link ClusterDescriptor} implementation. */
public class KubernetesStandaloneClusterDescriptor extends KubernetesClusterDescriptor {

    private static final Logger LOG =
            LoggerFactory.getLogger(KubernetesStandaloneClusterDescriptor.class);

    private static final String CLUSTER_DESCRIPTION = "Standalone Kubernetes cluster";

    private final Configuration flinkConfig;

    private final FlinkStandaloneKubeClient standaloneKubeClient;

    private final String clusterId;

    public KubernetesStandaloneClusterDescriptor(
            Configuration flinkConfig, FlinkStandaloneKubeClient client) {
        super(
                flinkConfig,
                FlinkKubeClientFactory.getInstance(),
                new DefaultKubernetesArtifactUploader());
        this.flinkConfig = checkNotNull(flinkConfig);
        this.standaloneKubeClient = checkNotNull(client);
        this.clusterId =
                checkNotNull(
                        flinkConfig.getString(KubernetesConfigOptions.CLUSTER_ID),
                        "ClusterId must be specified!");
    }

    @Override
    public String getClusterDescription() {
        return CLUSTER_DESCRIPTION;
    }

    @Override
    public ClusterClientProvider<String> deploySessionCluster(
            ClusterSpecification clusterSpecification) throws ClusterDeploymentException {
        flinkConfig.set(
                StandaloneKubernetesConfigOptionsInternal.CLUSTER_MODE,
                StandaloneKubernetesConfigOptionsInternal.ClusterMode.SESSION);

        var clusterClientProvider = deployClusterInternal(clusterSpecification);

        try (ClusterClient<String> clusterClient = clusterClientProvider.getClusterClient()) {
            LOG.info(
                    "Created flink session cluster {} successfully, JobManager Web Interface: {}",
                    clusterId,
                    clusterClient.getWebInterfaceURL());
        }
        return clusterClientProvider;
    }

    @Override
    public ClusterClientProvider<String> deployApplicationCluster(
            ClusterSpecification clusterSpecification,
            ApplicationConfiguration applicationConfiguration)
            throws ClusterDeploymentException {
        flinkConfig.set(
                StandaloneKubernetesConfigOptionsInternal.CLUSTER_MODE,
                StandaloneKubernetesConfigOptionsInternal.ClusterMode.APPLICATION);
        applicationConfiguration.applyToConfiguration(flinkConfig);
        var clusterClientProvider = deployClusterInternal(clusterSpecification);

        try (ClusterClient<String> clusterClient = clusterClientProvider.getClusterClient()) {
            LOG.info(
                    "Created flink application cluster {} successfully, JobManager Web Interface: {}",
                    clusterId,
                    clusterClient.getWebInterfaceURL());
        }
        return clusterClientProvider;
    }

    private ClusterClientProvider<String> deployClusterInternal(
            ClusterSpecification clusterSpecification) throws ClusterDeploymentException {

        // Rpc, blob, rest, taskManagerRpc ports need to be exposed, so update them to fixed values.
        KubernetesUtils.checkAndUpdatePortConfigOption(
                flinkConfig, BlobServerOptions.PORT, Constants.BLOB_SERVER_PORT);
        KubernetesUtils.checkAndUpdatePortConfigOption(
                flinkConfig, TaskManagerOptions.RPC_PORT, Constants.TASK_MANAGER_RPC_PORT);
        KubernetesUtils.checkAndUpdatePortConfigOption(
                flinkConfig, RestOptions.BIND_PORT, Constants.REST_PORT);

        if (HighAvailabilityMode.isHighAvailabilityModeActivated(flinkConfig)) {
            flinkConfig.setString(HighAvailabilityOptions.HA_CLUSTER_ID, clusterId);
            KubernetesUtils.checkAndUpdatePortConfigOption(
                    flinkConfig,
                    HighAvailabilityOptions.HA_JOB_MANAGER_PORT_RANGE,
                    flinkConfig.get(JobManagerOptions.PORT));
        }

        // Deploy JM + resources
        try {
            KubernetesJobManagerSpecification jmSpec = getJobManagerSpec(clusterSpecification);
            Deployment tmDeployment = getTaskManagerDeployment(clusterSpecification);

            standaloneKubeClient.createJobManagerComponent(jmSpec);
            standaloneKubeClient.createTaskManagerDeployment(tmDeployment);

            return createClusterClientProvider(clusterId);
        } catch (Exception e) {
            try {
                LOG.warn(
                        "Failed to create the Kubernetes cluster \"{}\", try to clean up the residual resources.",
                        clusterId);
                standaloneKubeClient.stopAndCleanupCluster(clusterId);
            } catch (Exception e1) {
                LOG.info(
                        "Failed to stop and clean up the Kubernetes cluster \"{}\".",
                        clusterId,
                        e1);
            }
            throw new ClusterDeploymentException(
                    "Could not create Kubernetes cluster \"" + clusterId + "\".", e);
        }
    }

    private KubernetesJobManagerSpecification getJobManagerSpec(
            ClusterSpecification clusterSpecification) throws IOException {
        final StandaloneKubernetesJobManagerParameters kubernetesJobManagerParameters =
                new StandaloneKubernetesJobManagerParameters(flinkConfig, clusterSpecification);

        final FlinkPod podTemplate =
                kubernetesJobManagerParameters
                        .getPodTemplateFilePath()
                        .map(
                                file ->
                                        KubernetesUtils.loadPodFromTemplateFile(
                                                standaloneKubeClient,
                                                file,
                                                Constants.MAIN_CONTAINER_NAME))
                        .orElse(new FlinkPod.Builder().build());

        return StandaloneKubernetesJobManagerFactory.buildKubernetesJobManagerSpecification(
                podTemplate, kubernetesJobManagerParameters);
    }

    private Deployment getTaskManagerDeployment(ClusterSpecification clusterSpecification) {
        final StandaloneKubernetesTaskManagerParameters kubernetesTaskManagerParameters =
                new StandaloneKubernetesTaskManagerParameters(flinkConfig, clusterSpecification);

        final FlinkPod podTemplate =
                kubernetesTaskManagerParameters
                        .getPodTemplateFilePath()
                        .map(
                                file ->
                                        KubernetesUtils.loadPodFromTemplateFile(
                                                standaloneKubeClient,
                                                file,
                                                Constants.MAIN_CONTAINER_NAME))
                        .orElse(new FlinkPod.Builder().build());

        return StandaloneKubernetesTaskManagerFactory.buildKubernetesTaskManagerDeployment(
                podTemplate, kubernetesTaskManagerParameters);
    }

    private ClusterClientProvider<String> createClusterClientProvider(String clusterId) {
        return () -> {
            final Configuration configuration = new Configuration(flinkConfig);

            final Optional<Endpoint> restEndpoint = standaloneKubeClient.getRestEndpoint(clusterId);

            if (restEndpoint.isPresent()) {
                configuration.setString(RestOptions.ADDRESS, restEndpoint.get().getAddress());
                configuration.setInteger(RestOptions.PORT, restEndpoint.get().getPort());
            } else {
                throw new RuntimeException(
                        new ClusterRetrieveException(
                                "Could not get the rest endpoint of " + clusterId));
            }

            try {
                // Flink client will always use Kubernetes service to contact with jobmanager. So we
                // have a pre-configured web monitor address. Using StandaloneClientHAServices to
                // create RestClusterClient is reasonable.
                return new RestClusterClient<>(
                        configuration,
                        clusterId,
                        (effectiveConfiguration, fatalErrorHandler) ->
                                new StandaloneClientHAServices(
                                        getWebMonitorAddress(effectiveConfiguration)));
            } catch (Exception e) {
                throw new RuntimeException(
                        new ClusterRetrieveException("Could not create the RestClusterClient.", e));
            }
        };
    }

    private String getWebMonitorAddress(Configuration configuration) throws Exception {
        AddressResolution resolution = AddressResolution.TRY_ADDRESS_RESOLUTION;
        final KubernetesConfigOptions.ServiceExposedType serviceType =
                configuration.get(KubernetesConfigOptions.REST_SERVICE_EXPOSED_TYPE);
        if (serviceType.isClusterIP()) {
            resolution = AddressResolution.NO_ADDRESS_RESOLUTION;
            LOG.warn(
                    "Please note that Flink client operations(e.g. cancel, list, stop,"
                            + " savepoint, etc.) won't work from outside the Kubernetes cluster"
                            + " since '{}' has been set to {}.",
                    KubernetesConfigOptions.REST_SERVICE_EXPOSED_TYPE.key(),
                    serviceType);
        }
        return HighAvailabilityServicesUtils.getWebMonitorAddress(configuration, resolution);
    }
}
