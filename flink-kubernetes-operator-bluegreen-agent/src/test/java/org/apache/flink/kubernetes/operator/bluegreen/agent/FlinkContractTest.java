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

import net.bytebuddy.jar.asm.ClassReader;
import net.bytebuddy.jar.asm.ClassVisitor;
import net.bytebuddy.jar.asm.MethodVisitor;
import net.bytebuddy.jar.asm.Opcodes;
import net.bytebuddy.jar.asm.Type;
import org.junit.jupiter.api.Test;

import java.io.InputStream;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Checks the Flink methods the agent depends on by name against the Flink version on the test
 * classpath. The agent matches them by name only, so a renamed or removed method would not fail the
 * job: the agent would silently intercept nothing. Run with {@code -Dflink.version=<2.x>} to check
 * Flink 2.x.
 */
public class FlinkContractTest {

    @Test
    public void environmentDeclaresTheInterceptedExecuteAsync() {
        // GateInjectionAgent instruments executeAsync(StreamGraph) on this class only
        assertEquals(
                GateInjectionAgent.INSTRUMENTED_TYPE, StreamExecutionEnvironment.class.getName());
        assertDoesNotThrow(
                () ->
                        StreamExecutionEnvironment.class.getDeclaredMethod(
                                "executeAsync", StreamGraph.class));
    }

    @Test
    public void applicationModeEnvironmentSubmitsThroughTheInterceptedExecuteAsync()
            throws Exception {
        // StreamContextEnvironment overrides executeAsync(StreamGraph): the agent only reaches its
        // submissions, and GateInjectionAgentTest only stands for the agent, if it calls the base
        // method
        String descriptor =
                Type.getMethodDescriptor(
                        StreamExecutionEnvironment.class.getMethod(
                                "executeAsync", StreamGraph.class));
        String base = Type.getInternalName(StreamExecutionEnvironment.class);
        AtomicBoolean callsBase = new AtomicBoolean();
        try (InputStream bytecode =
                StreamContextEnvironment.class.getResourceAsStream(
                        StreamContextEnvironment.class.getSimpleName() + ".class")) {
            new ClassReader(bytecode)
                    .accept(
                            new ClassVisitor(Opcodes.ASM9) {
                                @Override
                                public MethodVisitor visitMethod(
                                        int access,
                                        String name,
                                        String desc,
                                        String signature,
                                        String[] exceptions) {
                                    if (!name.equals("executeAsync") || !desc.equals(descriptor)) {
                                        return null;
                                    }
                                    return new MethodVisitor(Opcodes.ASM9) {
                                        @Override
                                        public void visitMethodInsn(
                                                int opcode,
                                                String owner,
                                                String calledName,
                                                String calledDesc,
                                                boolean isInterface) {
                                            if (opcode == Opcodes.INVOKESPECIAL
                                                    && owner.equals(base)
                                                    && calledName.equals("executeAsync")
                                                    && calledDesc.equals(descriptor)) {
                                                callsBase.set(true);
                                            }
                                        }
                                    };
                                }
                            },
                            ClassReader.SKIP_DEBUG);
        }
        assertTrue(callsBase.get());
    }

    @Test
    public void environmentConfigurationIsAConfiguration() throws Exception {
        // GateInjectionInterceptor reflects on getConfiguration() and casts the result
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        Object config = env.getClass().getMethod("getConfiguration").invoke(env);
        assertInstanceOf(Configuration.class, config);
    }
}
