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

import org.apache.flink.client.deployment.ClusterSpecification;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.kubernetes.configuration.KubernetesConfigOptions;
import org.apache.flink.kubernetes.kubeclient.FlinkPod;
import org.apache.flink.kubernetes.operator.kubeclient.parameters.StandaloneKubernetesTaskManagerParameters;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;

/**
 * @link CmdStandaloneTaskManagerDecorator unit tests
 */
public class CmdStandaloneTaskManagerDecoratorTest {

    private static final String MOCK_ENTRYPATH = "./docker-entrypath";

    private StandaloneKubernetesTaskManagerParameters tmParameters;
    private Configuration configuration;
    private CmdStandaloneTaskManagerDecorator decorator;

    @BeforeEach
    public void setup() {
        configuration = new Configuration();
        configuration.setString(KubernetesConfigOptions.KUBERNETES_ENTRY_PATH, MOCK_ENTRYPATH);
        tmParameters =
                new StandaloneKubernetesTaskManagerParameters(
                        configuration,
                        new ClusterSpecification.ClusterSpecificationBuilder()
                                .createClusterSpecification());

        decorator = new CmdStandaloneTaskManagerDecorator(tmParameters);
    }

    @Test
    public void testCommandAdded() {
        FlinkPod decoratedPod = decorator.decorateFlinkPod(new FlinkPod.Builder().build());

        assertThat(
                decoratedPod.getMainContainer().getCommand(), containsInAnyOrder(MOCK_ENTRYPATH));
        assertThat(
                decoratedPod.getMainContainer().getArgs(),
                containsInAnyOrder(CmdStandaloneTaskManagerDecorator.TASKMANAGER_ENTRYPOINT_ARG));
    }
}
