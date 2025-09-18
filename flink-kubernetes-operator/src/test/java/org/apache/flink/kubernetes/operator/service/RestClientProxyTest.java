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

package org.apache.flink.kubernetes.operator.service;

import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.kubernetes.configuration.KubernetesConfigOptions;
import org.apache.flink.kubernetes.operator.TestUtils;
import org.apache.flink.kubernetes.operator.api.spec.FlinkVersion;
import org.apache.flink.kubernetes.operator.config.FlinkConfigManager;
import org.apache.flink.util.concurrent.ExecutorThreadFactory;
import org.apache.flink.util.concurrent.Executors;

import org.apache.flink.shaded.netty4.io.netty.channel.EventLoopGroup;
import org.apache.flink.shaded.netty4.io.netty.channel.nio.NioEventLoopGroup;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.concurrent.ExecutorService;

import static org.apache.flink.kubernetes.operator.config.FlinkConfigBuilder.FLINK_VERSION;
import static org.junit.jupiter.api.Assertions.assertFalse;

/**
 * @link RestClientProxy unit tests
 */
public class RestClientProxyTest {

    private final Configuration configuration = new Configuration();

    private final FlinkConfigManager configManager = new FlinkConfigManager(configuration);
    private ExecutorService executorService;

    private EventLoopGroup flinkClientEventLoopGroup;

    @BeforeEach
    public void setup() {
        configuration.set(KubernetesConfigOptions.CLUSTER_ID, TestUtils.TEST_DEPLOYMENT_NAME);
        configuration.set(KubernetesConfigOptions.NAMESPACE, TestUtils.TEST_NAMESPACE);
        configuration.set(FLINK_VERSION, FlinkVersion.v1_18);

        executorService = Executors.newDirectExecutorService();

        flinkClientEventLoopGroup =
                new NioEventLoopGroup(
                        configManager.getOperatorConfiguration().getFlinkClientIOThreads(),
                        new ExecutorThreadFactory("flink-rest-client-netty-shared"));
    }

    @Test
    public void testClose() throws Exception {
        RestClientProxy restClientProxy =
                new RestClientProxy(
                        configuration, executorService, "localhost", -1, flinkClientEventLoopGroup);

        restClientProxy.close();
        assertFalse(flinkClientEventLoopGroup.isShutdown());
    }

    @Test
    public void testShutdown() throws Exception {
        RestClientProxy restClientProxy =
                new RestClientProxy(
                        configuration, executorService, "localhost", -1, flinkClientEventLoopGroup);

        restClientProxy.shutdown(Time.seconds(5));
        assertFalse(flinkClientEventLoopGroup.isShutdown());
    }
}
