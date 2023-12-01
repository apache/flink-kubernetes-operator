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

package org.apache.flink.kubernetes.operator.autoscaler;

import org.apache.flink.configuration.PipelineOptions;

import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for KubernetesScalingRealizer. */
public class KubernetesScalingRealizerTest {

    @Test
    public void testAutoscalerOverridesVertexIdsAreSorted() {

        KubernetesJobAutoScalerContext ctx =
                TestingKubernetesAutoscalerUtils.createContext("test", null);

        // Create map which returns keys unsorted
        Map<String, String> overrides = new LinkedHashMap<>();
        overrides.put("b", "2");
        overrides.put("a", "1");

        new KubernetesScalingRealizer().realize(ctx, overrides);

        assertThat(
                        ctx.getResource()
                                .getSpec()
                                .getFlinkConfiguration()
                                .get(PipelineOptions.PARALLELISM_OVERRIDES.key()))
                .isEqualTo("a:1,b:2");
    }
}
