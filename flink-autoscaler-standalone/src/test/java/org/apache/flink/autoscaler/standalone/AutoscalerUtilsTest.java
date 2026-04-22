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

package org.apache.flink.autoscaler.standalone;

import org.apache.flink.autoscaler.metrics.FlinkAutoscalerEvaluator;
import org.apache.flink.autoscaler.metrics.TestCustomEvaluator;

import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/** Unit tests for {@link AutoscalerUtils}. */
class AutoscalerUtilsTest {

    @Test
    void testDiscoverCustomEvaluators() {
        Map<String, FlinkAutoscalerEvaluator> evaluators =
                AutoscalerUtils.discoverCustomEvaluators();

        assertThat(evaluators)
                .as(
                        "Expected to discover the TestCustomEvaluator registered under META-INF/services")
                .containsOnlyKeys("test-custom-evaluator");
        assertThat(evaluators.get("test-custom-evaluator")).isInstanceOf(TestCustomEvaluator.class);
    }
}
