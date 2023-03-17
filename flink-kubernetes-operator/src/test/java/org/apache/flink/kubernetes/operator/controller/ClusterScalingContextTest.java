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

package org.apache.flink.kubernetes.operator.controller;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.kubernetes.operator.config.FlinkOperatorConfiguration;
import org.apache.flink.kubernetes.operator.config.KubernetesOperatorConfigOptions;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.util.concurrent.atomic.AtomicBoolean;

/** Tests for ClusterScalingContext. */
public class ClusterScalingContextTest {

    private final Clock clock = Clock.fixed(Instant.ofEpochMilli(0), ZoneId.systemDefault());

    @Test
    void testAlwaysAllowScalingWithZeroTimeout() {
        var toBeNamedContext = new ClusterScalingContext(Duration.ZERO, clock);
        var flag = new AtomicBoolean();

        Assertions.assertFalse(
                toBeNamedContext.maybeExecuteScalingLogic(() -> flag.getAndSet(true)));
        Assertions.assertTrue(flag.get());
        Assertions.assertTrue(toBeNamedContext.maybeExecuteScalingLogic(flag::get));
    }

    @Test
    void testDeferScaling() {
        var toBeNamedContext = new ClusterScalingContext(Duration.ofSeconds(1), clock);
        var flag = new AtomicBoolean();

        Assertions.assertFalse(
                toBeNamedContext.maybeExecuteScalingLogic(
                        () -> {
                            flag.set(true);
                            return true;
                        }));
        Assertions.assertFalse(flag.get());
    }

    @Test
    void testAllowScalingWhenCoolDownFinishes() {
        var toBeNamedContext = new ClusterScalingContext(Duration.ofMillis(1), clock);
        Assertions.assertFalse(toBeNamedContext.maybeExecuteScalingLogic(() -> true));

        toBeNamedContext.updateClock(Clock.fixed(Instant.ofEpochMilli(1), ZoneId.systemDefault()));

        var flag = new AtomicBoolean();
        Assertions.assertTrue(
                toBeNamedContext.maybeExecuteScalingLogic(
                        () -> {
                            flag.set(true);
                            return true;
                        }));
        Assertions.assertTrue(flag.get());

        Assertions.assertFalse(toBeNamedContext.maybeExecuteScalingLogic(() -> true));
    }

    @Test
    void testUsesCorrectConfigEntry() {
        Configuration config = new Configuration();
        config.set(
                KubernetesOperatorConfigOptions.OPERATOR_RESCALING_CLUSTER_COOLDOWN,
                Duration.ofSeconds(42));
        var operatorConfig = FlinkOperatorConfiguration.fromConfiguration(config);

        var toBeNamedContext = new ClusterScalingContext(operatorConfig);

        Assertions.assertEquals(
                Duration.ofSeconds(42), toBeNamedContext.getClusterCoolDownDuration());
    }
}
