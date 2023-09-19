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

package org.apache.flink.autoscaler.utils;

import org.apache.flink.autoscaler.config.AutoScalerOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.jobgraph.JobVertexID;

import org.junit.jupiter.api.Test;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Test for AutoScalerUtils. */
public class AutoScalerUtilsTest {
    @Test
    public void testVertexExclusion() {
        var conf = new Configuration();
        var v1 = new JobVertexID();
        var v2 = new JobVertexID();
        var v3 = new JobVertexID();

        assertTrue(AutoScalerUtils.excludeVertexFromScaling(conf, v1));
        assertEquals(List.of(v1.toString()), conf.get(AutoScalerOptions.VERTEX_EXCLUDE_IDS));

        assertFalse(AutoScalerUtils.excludeVertexFromScaling(conf, v1));
        assertEquals(List.of(v1.toString()), conf.get(AutoScalerOptions.VERTEX_EXCLUDE_IDS));

        assertTrue(AutoScalerUtils.excludeVerticesFromScaling(conf, List.of(v1, v2, v3)));
        assertEquals(
                Set.of(v1.toString(), v2.toString(), v3.toString()),
                new HashSet<>(conf.get(AutoScalerOptions.VERTEX_EXCLUDE_IDS)));

        assertFalse(AutoScalerUtils.excludeVerticesFromScaling(conf, List.of(v1, v2)));
        assertEquals(
                Set.of(v1.toString(), v2.toString(), v3.toString()),
                new HashSet<>(conf.get(AutoScalerOptions.VERTEX_EXCLUDE_IDS)));
    }
}
