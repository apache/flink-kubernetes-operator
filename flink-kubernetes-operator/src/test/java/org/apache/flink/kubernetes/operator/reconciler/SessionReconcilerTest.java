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

package org.apache.flink.kubernetes.operator.reconciler;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.kubernetes.operator.TestUtils;
import org.apache.flink.kubernetes.operator.TestingFlinkService;
import org.apache.flink.kubernetes.operator.config.FlinkOperatorConfiguration;
import org.apache.flink.kubernetes.operator.crd.FlinkDeployment;

import io.javaoperatorsdk.operator.api.reconciler.Context;
import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;

/** Tests for {@link SessionReconciler}. */
public class SessionReconcilerTest {

    private final FlinkOperatorConfiguration operatorConfiguration =
            FlinkOperatorConfiguration.fromConfiguration(new Configuration());

    @Test
    public void testStartSession() throws Exception {
        Context context = TestUtils.createEmptyContext();
        var count = new AtomicInteger();
        TestingFlinkService flinkService =
                new TestingFlinkService() {
                    @Override
                    public void submitSessionCluster(
                            FlinkDeployment deployment, Configuration conf) {
                        super.submitSessionCluster(deployment, conf);
                        count.addAndGet(1);
                    }
                };

        SessionReconciler reconciler =
                new SessionReconciler(null, flinkService, operatorConfiguration);
        FlinkDeployment deployment = TestUtils.buildSessionCluster();
        reconciler.reconcile(deployment, context, new Configuration());
        assertEquals(count.get(), 1);
    }
}
