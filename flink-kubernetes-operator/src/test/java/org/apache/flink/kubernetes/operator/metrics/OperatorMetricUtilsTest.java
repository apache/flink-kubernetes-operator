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

package org.apache.flink.kubernetes.operator.metrics;

import org.apache.flink.configuration.Configuration;

import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

/** {@link OperatorMetricUtils} tests. */
public class OperatorMetricUtilsTest {

    @Test
    public void testMetricConfig() {
        Configuration configuration =
                Configuration.fromMap(
                        Map.of(
                                "k",
                                "v",
                                "metrics.flink.metric.conf",
                                "fmc",
                                "kubernetes.operator.metrics.op.metric.conf",
                                "omc"));

        Configuration expectedMetricConf =
                Configuration.fromMap(Map.of("k", "v", "metrics.op.metric.conf", "omc"));

        assertEquals(expectedMetricConf, OperatorMetricUtils.createMetricConfig(configuration));
    }
}
