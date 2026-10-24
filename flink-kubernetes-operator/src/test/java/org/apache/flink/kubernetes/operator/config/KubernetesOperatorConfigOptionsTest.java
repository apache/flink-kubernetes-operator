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

package org.apache.flink.kubernetes.operator.config;

import org.apache.flink.configuration.Configuration;

import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.apache.flink.kubernetes.operator.config.KubernetesOperatorConfigOptions.OPERATOR_SAVEPOINT_HISTORY_MAX_AGE;
import static org.apache.flink.kubernetes.operator.config.KubernetesOperatorConfigOptions.OPERATOR_SAVEPOINT_HISTORY_MAX_AGE_THRESHOLD;
import static org.apache.flink.kubernetes.operator.config.KubernetesOperatorConfigOptions.OPERATOR_SAVEPOINT_HISTORY_MAX_COUNT;
import static org.apache.flink.kubernetes.operator.config.KubernetesOperatorConfigOptions.OPERATOR_SAVEPOINT_HISTORY_MAX_COUNT_THRESHOLD;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link KubernetesOperatorConfigOptions}. */
class KubernetesOperatorConfigOptionsTest {

    @Test
    void testSavepointHistoryThresholdKeysStillReadDeprecatedKeys() {
        final Configuration configuration = new Configuration();
        // Keys before FLINK-40873, kept so that existing configurations keep working.
        configuration.setString("kubernetes.operator.savepoint.history.max.count.threshold", "5");
        configuration.setString(
                "kubernetes.operator.savepoint.history.max.age.threshold", "10 min");

        assertThat(configuration.get(OPERATOR_SAVEPOINT_HISTORY_MAX_COUNT_THRESHOLD)).isEqualTo(5);
        assertThat(configuration.get(OPERATOR_SAVEPOINT_HISTORY_MAX_AGE_THRESHOLD))
                .isEqualTo(Duration.ofMinutes(10));
    }

    @Test
    void testSavepointHistoryThresholdKeysAreNotPrefixedByTheHistoryKeys() {
        assertThat(OPERATOR_SAVEPOINT_HISTORY_MAX_COUNT.key())
                .isEqualTo("kubernetes.operator.savepoint.history.max.count");
        assertThat(OPERATOR_SAVEPOINT_HISTORY_MAX_COUNT_THRESHOLD.key())
                .isEqualTo("kubernetes.operator.savepoint.history.threshold.max.count");

        assertThat(OPERATOR_SAVEPOINT_HISTORY_MAX_AGE.key())
                .isEqualTo("kubernetes.operator.savepoint.history.max.age");
        assertThat(OPERATOR_SAVEPOINT_HISTORY_MAX_AGE_THRESHOLD.key())
                .isEqualTo("kubernetes.operator.savepoint.history.threshold.max.age");
    }
}
