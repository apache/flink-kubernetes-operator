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

package org.apache.flink.kubernetes.operator.kubeclient.utils;

import org.apache.flink.client.deployment.ClusterSpecification;
import org.apache.flink.configuration.BlobServerOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.kubernetes.configuration.KubernetesConfigOptions;
import org.apache.flink.kubernetes.utils.Constants;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

/** Testing utilities. */
public class TestUtils {

    public static final String CLUSTER_ID = "test-cluster";
    public static final String SERVICE_ACCOUNT = "flink-operator";
    public static final String TEST_NAMESPACE = "flink-operator-test";

    public static final String TASK_MANAGER_MEMORY = "2048m";
    public static final String JOB_MANAGER_MEMORY = "1024m";

    public static final String FLINK_VERSION = "latest";
    public static final String IMAGE = String.format("flink:%s", FLINK_VERSION);
    public static final String IMAGE_POLICY = "IfNotPresent";

    public static final int TASK_MANAGER_MEMORY_MB =
            MemorySize.parse(TASK_MANAGER_MEMORY).getMebiBytes();
    public static final int JOB_MANAGER_MEMORY_MB =
            MemorySize.parse(JOB_MANAGER_MEMORY).getMebiBytes();

    public static final int SLOTS_PER_TASK_MANAGER = 2;

    public static final double TASK_MANAGER_CPU = 4;
    public static final double JOB_MANAGER_CPU = 2;

    public static final String USER_ENV_VAR = "USER_ENV";

    public static final String JM_ENV_VALUE = "TEST_JM";
    public static final String TM_ENV_VALUE = "TEST_TM";

    public static Map<String, String> generateTestStringStringMap(
            String keyPrefix, String valuePrefix, int entries) {
        Map<String, String> map = new HashMap<>();
        for (int i = 1; i <= entries; i++) {
            map.put(keyPrefix + i, valuePrefix + i);
        }
        return map;
    }

    public static Map<String, String> generateTestOwnerReferenceMap(String kind) {
        return Map.of(
                "apiVersion",
                "flink.apache.org/v1beta1",
                "kind",
                kind,
                "name",
                CLUSTER_ID,
                "uid",
                UUID.randomUUID().toString(),
                "blockOwnerDeletion",
                "false",
                "controller",
                "false");
    }

    public static ClusterSpecification createClusterSpecification() {
        return new ClusterSpecification.ClusterSpecificationBuilder()
                .setMasterMemoryMB(JOB_MANAGER_MEMORY_MB)
                .setTaskManagerMemoryMB(TASK_MANAGER_MEMORY_MB)
                .setSlotsPerTaskManager(SLOTS_PER_TASK_MANAGER)
                .createClusterSpecification();
    }

    public static Configuration createTestFlinkConfig() {
        Configuration flinkConf = new Configuration();
        flinkConf.set(KubernetesConfigOptions.CLUSTER_ID, CLUSTER_ID);
        flinkConf.set(KubernetesConfigOptions.NAMESPACE, TEST_NAMESPACE);
        flinkConf.set(KubernetesConfigOptions.KUBERNETES_SERVICE_ACCOUNT, SERVICE_ACCOUNT);
        flinkConf.set(KubernetesConfigOptions.CONTAINER_IMAGE, IMAGE);
        flinkConf.set(
                KubernetesConfigOptions.CONTAINER_IMAGE_PULL_POLICY,
                KubernetesConfigOptions.ImagePullPolicy.valueOf(IMAGE_POLICY));

        flinkConf.set(KubernetesConfigOptions.JOB_MANAGER_CPU, JOB_MANAGER_CPU);
        flinkConf.set(KubernetesConfigOptions.TASK_MANAGER_CPU, TASK_MANAGER_CPU);

        flinkConf.setString(
                TaskManagerOptions.RPC_PORT, String.valueOf(Constants.TASK_MANAGER_RPC_PORT));
        flinkConf.setString(BlobServerOptions.PORT, String.valueOf(Constants.BLOB_SERVER_PORT));
        flinkConf.setString(RestOptions.BIND_PORT, String.valueOf(Constants.REST_PORT));
        return flinkConf;
    }
}
