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

package org.apache.flink.kubernetes.operator.bluegreen.agent;

import org.apache.flink.client.program.StreamContextEnvironment;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.graph.StreamGraph;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;

/**
 * Checks the Flink methods the agent depends on by name against the Flink version on the test
 * classpath. The agent matches them by name only, so a renamed or removed method would not fail the
 * job: the agent would silently intercept nothing. Run with {@code -Dflink.version=<2.x>} to check
 * Flink 2.x.
 */
public class FlinkContractTest {

    @Test
    public void environmentsDeclareTheInterceptedExecute() {
        // GateInjectionAgent instruments execute(StreamGraph) on both environments
        assertDoesNotThrow(
                () ->
                        StreamExecutionEnvironment.class.getDeclaredMethod(
                                "execute", StreamGraph.class));
        assertDoesNotThrow(
                () ->
                        StreamContextEnvironment.class.getDeclaredMethod(
                                "execute", StreamGraph.class));
    }

    @Test
    public void environmentConfigurationIsAConfiguration() throws Exception {
        // GateInjectionInterceptor reflects on getConfiguration() and casts the result
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        Object config = env.getClass().getMethod("getConfiguration").invoke(env);
        assertInstanceOf(Configuration.class, config);
    }
}
