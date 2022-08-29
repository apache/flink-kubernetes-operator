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

package org.apache.flink.kubernetes.operator.utils;

import org.apache.flink.kubernetes.operator.kubeclient.FlinkStandaloneKubeClient;
import org.apache.flink.kubernetes.utils.Constants;

import io.fabric8.kubernetes.api.model.PersistentVolumeClaim;

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** Standalone Kubernetes Utils. */
public class StandaloneKubernetesUtils {

    public static final String LABEL_TYPE_STANDALONE_TYPE = "flink-standalone-kubernetes";
    private static final String TM_DEPLOYMENT_POSTFIX = "-taskmanager";

    public static String getTaskManagerStatefulSetName(String clusterId) {
        return clusterId + TM_DEPLOYMENT_POSTFIX;
    }

    public static String getJobManagerStatefulSetName(String clusterId) {
        return clusterId;
    }

    public static Map<String, String> getCommonLabels(String clusterId) {
        Map<String, String> commonLabels = new HashMap<>(2);
        commonLabels.put(Constants.LABEL_TYPE_KEY, LABEL_TYPE_STANDALONE_TYPE);
        commonLabels.put(Constants.LABEL_APP_KEY, clusterId);
        return commonLabels;
    }

    public static Map<String, String> getTaskManagerSelectors(String clusterId) {
        Map<String, String> labels = getCommonLabels(clusterId);
        labels.put(Constants.LABEL_COMPONENT_KEY, Constants.LABEL_COMPONENT_TASK_MANAGER);
        return Collections.unmodifiableMap(labels);
    }

    public static Map<String, String> getJobManagerSelectors(String clusterId) {
        final Map<String, String> labels = getCommonLabels(clusterId);
        labels.put(Constants.LABEL_COMPONENT_KEY, Constants.LABEL_COMPONENT_JOB_MANAGER);
        return Collections.unmodifiableMap(labels);
    }

    public static List<PersistentVolumeClaim> loadPvcFromTemplateFile(
            FlinkStandaloneKubeClient kubeClient, List<File> pvcFiles) {
        if (null == pvcFiles) {
            return null;
        }
        List<PersistentVolumeClaim> volumeClaims = new ArrayList<>();
        pvcFiles.forEach(
                file -> {
                    volumeClaims.add(kubeClient.loadVolumeClaimTemplates(file));
                });
        return volumeClaims;
    }
}
