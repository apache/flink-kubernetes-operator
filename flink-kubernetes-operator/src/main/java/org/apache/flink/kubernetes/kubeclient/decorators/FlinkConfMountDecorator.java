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

package org.apache.flink.kubernetes.kubeclient.decorators;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ConfigurationUtils;
import org.apache.flink.configuration.DeploymentOptionsInternal;
import org.apache.flink.configuration.IllegalConfigurationException;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.kubernetes.configuration.KubernetesConfigOptions;
import org.apache.flink.kubernetes.kubeclient.FlinkPod;
import org.apache.flink.kubernetes.kubeclient.parameters.AbstractKubernetesParameters;
import org.apache.flink.kubernetes.operator.api.spec.FlinkVersion;
import org.apache.flink.kubernetes.shaded.io.fabric8.kubernetes.api.model.ConfigMap;
import org.apache.flink.kubernetes.shaded.io.fabric8.kubernetes.api.model.ConfigMapBuilder;
import org.apache.flink.kubernetes.shaded.io.fabric8.kubernetes.api.model.Container;
import org.apache.flink.kubernetes.shaded.io.fabric8.kubernetes.api.model.ContainerBuilder;
import org.apache.flink.kubernetes.shaded.io.fabric8.kubernetes.api.model.HasMetadata;
import org.apache.flink.kubernetes.shaded.io.fabric8.kubernetes.api.model.KeyToPath;
import org.apache.flink.kubernetes.shaded.io.fabric8.kubernetes.api.model.KeyToPathBuilder;
import org.apache.flink.kubernetes.shaded.io.fabric8.kubernetes.api.model.Pod;
import org.apache.flink.kubernetes.shaded.io.fabric8.kubernetes.api.model.PodBuilder;
import org.apache.flink.kubernetes.shaded.io.fabric8.kubernetes.api.model.Volume;
import org.apache.flink.kubernetes.shaded.io.fabric8.kubernetes.api.model.VolumeBuilder;
import org.apache.flink.kubernetes.utils.Constants;

import org.apache.flink.shaded.guava31.com.google.common.io.Files;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.flink.kubernetes.operator.config.FlinkConfigBuilder.FLINK_VERSION;
import static org.apache.flink.kubernetes.utils.Constants.CONFIG_FILE_LOG4J_NAME;
import static org.apache.flink.kubernetes.utils.Constants.CONFIG_FILE_LOGBACK_NAME;
import static org.apache.flink.kubernetes.utils.Constants.CONFIG_MAP_PREFIX;
import static org.apache.flink.kubernetes.utils.Constants.FLINK_CONF_VOLUME;
import static org.apache.flink.util.Preconditions.checkNotNull;

/** Copied from Flink core and modified to handle all Flink versions. */
public class FlinkConfMountDecorator extends AbstractKubernetesStepDecorator {

    private final AbstractKubernetesParameters kubernetesComponentConf;

    public FlinkConfMountDecorator(AbstractKubernetesParameters kubernetesComponentConf) {
        this.kubernetesComponentConf = checkNotNull(kubernetesComponentConf);
    }

    @Override
    public FlinkPod decorateFlinkPod(FlinkPod flinkPod) {
        final Pod mountedPod = decoratePod(flinkPod.getPodWithoutMainContainer());

        final Container mountedMainContainer =
                new ContainerBuilder(flinkPod.getMainContainer())
                        .addNewVolumeMount()
                        .withName(FLINK_CONF_VOLUME)
                        .withMountPath(kubernetesComponentConf.getFlinkConfDirInPod())
                        .endVolumeMount()
                        .build();

        return new FlinkPod.Builder(flinkPod)
                .withPod(mountedPod)
                .withMainContainer(mountedMainContainer)
                .build();
    }

    private Pod decoratePod(Pod pod) {
        final List<KeyToPath> keyToPaths =
                getLocalLogConfFiles().stream()
                        .map(
                                file ->
                                        new KeyToPathBuilder()
                                                .withKey(file.getName())
                                                .withPath(file.getName())
                                                .build())
                        .collect(Collectors.toList());
        keyToPaths.add(
                new KeyToPathBuilder()
                        .withKey(getFlinkConfFilename())
                        .withPath(getFlinkConfFilename())
                        .build());

        final Volume flinkConfVolume =
                new VolumeBuilder()
                        .withName(FLINK_CONF_VOLUME)
                        .withNewConfigMap()
                        .withName(getFlinkConfConfigMapName(kubernetesComponentConf.getClusterId()))
                        .withItems(keyToPaths)
                        .endConfigMap()
                        .build();

        return new PodBuilder(pod)
                .editSpec()
                .addNewVolumeLike(flinkConfVolume)
                .endVolume()
                .endSpec()
                .build();
    }

    @Override
    public List<HasMetadata> buildAccompanyingKubernetesResources() throws IOException {
        final String clusterId = kubernetesComponentConf.getClusterId();

        final Map<String, String> data = new HashMap<>();

        final List<File> localLogFiles = getLocalLogConfFiles();
        for (File file : localLogFiles) {
            data.put(file.getName(), Files.toString(file, StandardCharsets.UTF_8));
        }

        final List<String> confData =
                getClusterSideConfData(kubernetesComponentConf.getFlinkConfiguration());
        data.put(getFlinkConfFilename(), getFlinkConfData(confData));

        final ConfigMap flinkConfConfigMap =
                new ConfigMapBuilder()
                        .withApiVersion(Constants.API_VERSION)
                        .withNewMetadata()
                        .withName(getFlinkConfConfigMapName(clusterId))
                        .withLabels(kubernetesComponentConf.getCommonLabels())
                        .endMetadata()
                        .addToData(data)
                        .build();

        return Collections.singletonList(flinkConfConfigMap);
    }

    /** Get properties map for the cluster-side after removal of some keys. */
    private List<String> getClusterSideConfData(Configuration flinkConfig) {
        // For Flink versions that use the standard config we have to set the standardYaml flag in
        // the Configuration object manually instead of simply cloning, otherwise it would simply
        // inherit it from the base config (which would always be false currently).
        Configuration clusterSideConfig = new Configuration(useStandardYamlConfig());
        clusterSideConfig.addAll(flinkConfig);
        // Remove some configuration options that should not be taken to cluster side.
        clusterSideConfig.removeConfig(KubernetesConfigOptions.KUBE_CONFIG_FILE);
        clusterSideConfig.removeConfig(DeploymentOptionsInternal.CONF_DIR);
        clusterSideConfig.removeConfig(RestOptions.BIND_ADDRESS);
        clusterSideConfig.removeConfig(JobManagerOptions.BIND_HOST);
        clusterSideConfig.removeConfig(TaskManagerOptions.BIND_HOST);
        clusterSideConfig.removeConfig(TaskManagerOptions.HOST);

        validateConfigKeysForV2(clusterSideConfig);

        return ConfigurationUtils.convertConfigToWritableLines(clusterSideConfig, false);
    }

    private void validateConfigKeysForV2(Configuration clusterSideConfig) {

        // Only validate Flink 2.0 yaml configs
        if (!useStandardYamlConfig()) {
            return;
        }

        var keys = clusterSideConfig.keySet();

        for (var key1 : keys) {
            for (var key2 : keys) {
                if (key2.startsWith(key1 + ".")) {
                    throw new IllegalConfigurationException(
                            String.format(
                                    "Overlapping key prefixes detected (%s -> %s), please replace with Flink v2 compatible, non-deprecated keys.",
                                    key1, key2));
                }
            }
        }
    }

    @VisibleForTesting
    String getFlinkConfData(List<String> confData) throws IOException {
        try (StringWriter sw = new StringWriter();
                PrintWriter out = new PrintWriter(sw)) {
            confData.forEach(out::println);

            return sw.toString();
        }
    }

    private List<File> getLocalLogConfFiles() {
        final String confDir = kubernetesComponentConf.getConfigDirectory();
        final File logbackFile = new File(confDir, CONFIG_FILE_LOGBACK_NAME);
        final File log4jFile = new File(confDir, CONFIG_FILE_LOG4J_NAME);

        List<File> localLogConfFiles = new ArrayList<>();
        if (logbackFile.exists()) {
            localLogConfFiles.add(logbackFile);
        }
        if (log4jFile.exists()) {
            localLogConfFiles.add(log4jFile);
        }

        return localLogConfFiles;
    }

    @VisibleForTesting
    public static String getFlinkConfConfigMapName(String clusterId) {
        return CONFIG_MAP_PREFIX + clusterId;
    }

    /**
     * We have to override the GlobalConfiguration#getFlinkConfFilename() logic to make sure we can
     * separate operator level (Global) and Flink deployment specific config handling.
     *
     * @return conf file name
     */
    public String getFlinkConfFilename() {
        return useStandardYamlConfig() ? "config.yaml" : "flink-conf.yaml";
    }

    /**
     * Determine based on the Flink Version if we should use the new standard config.yaml vs the old
     * flink-conf.yaml. While technically 1.19+ could use this we don't want to change the behaviour
     * for already released Flink versions, so only switch to new yaml from Flink 2.0 onwards.
     *
     * @return True for Flink version 2.0 and above
     */
    boolean useStandardYamlConfig() {
        return kubernetesComponentConf
                .getFlinkConfiguration()
                .get(FLINK_VERSION)
                .isEqualOrNewer(FlinkVersion.v2_0);
    }
}
