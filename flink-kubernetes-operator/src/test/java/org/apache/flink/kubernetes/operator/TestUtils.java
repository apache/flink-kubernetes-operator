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

import org.apache.flink.kubernetes.configuration.KubernetesConfigOptions;
import org.apache.flink.kubernetes.operator.crd.FlinkDeployment;
import org.apache.flink.kubernetes.operator.crd.spec.FlinkDeploymentSpec;
import org.apache.flink.kubernetes.operator.crd.spec.JobManagerSpec;
import org.apache.flink.kubernetes.operator.crd.spec.JobSpec;
import org.apache.flink.kubernetes.operator.crd.spec.JobState;
import org.apache.flink.kubernetes.operator.crd.spec.Resource;
import org.apache.flink.kubernetes.operator.crd.spec.TaskManagerSpec;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.fabric8.kubernetes.api.model.ObjectMetaBuilder;

import java.util.Collections;

/** Testing utilities. */
public class TestUtils {

    private static final ObjectMapper objectMapper = new ObjectMapper();

    public static final String TEST_NAMESPACE = "flink-operator-test";
    public static final String SERVICE_ACCOUNT = "flink-operator";
    public static final String FLINK_VERSION = "latest";
    public static final String IMAGE = String.format("flink:%s", FLINK_VERSION);

    public static FlinkDeployment buildSessionCluster() {
        FlinkDeployment deployment = new FlinkDeployment();
        deployment.setMetadata(
                new ObjectMetaBuilder()
                        .withName("test-cluster")
                        .withNamespace(TEST_NAMESPACE)
                        .build());
        deployment.setSpec(
                FlinkDeploymentSpec.builder()
                        .image(IMAGE)
                        .flinkVersion(FLINK_VERSION)
                        .flinkConfiguration(
                                Collections.singletonMap(
                                        KubernetesConfigOptions.JOB_MANAGER_SERVICE_ACCOUNT.key(),
                                        SERVICE_ACCOUNT))
                        .jobManager(new JobManagerSpec(new Resource(1, "2048m"), 1, null))
                        .taskManager(new TaskManagerSpec(new Resource(1, "2048m"), 2, null))
                        .build());
        return deployment;
    }

    public static FlinkDeployment buildApplicationCluster() {
        FlinkDeployment deployment = buildSessionCluster();
        deployment
                .getSpec()
                .setJob(
                        JobSpec.builder()
                                .jarURI("local:///tmp/sample.jar")
                                .state(JobState.RUNNING)
                                .build());
        return deployment;
    }

    public static <T> T clone(T object) {
        if (object == null) {
            return null;
        }
        try {
            return (T)
                    objectMapper.readValue(
                            objectMapper.writeValueAsString(object), object.getClass());
        } catch (JsonProcessingException e) {
            throw new IllegalStateException(e);
        }
    }
}
