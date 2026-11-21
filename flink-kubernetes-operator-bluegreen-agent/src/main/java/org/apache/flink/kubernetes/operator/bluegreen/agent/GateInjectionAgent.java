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

import net.bytebuddy.agent.builder.AgentBuilder;
import net.bytebuddy.description.method.MethodDescription;
import net.bytebuddy.implementation.MethodDelegation;
import net.bytebuddy.matcher.ElementMatcher;

import java.io.File;
import java.lang.instrument.Instrumentation;
import java.util.jar.JarFile;

import static net.bytebuddy.matcher.ElementMatchers.named;
import static net.bytebuddy.matcher.ElementMatchers.takesArgument;
import static net.bytebuddy.matcher.ElementMatchers.takesArguments;

/**
 * Java agent that transparently injects BlueGreen gate operators into every Flink Application-mode
 * job, without requiring any user code changes.
 *
 * <p>Intercept point: {@code StreamExecutionEnvironment.executeAsync(StreamGraph)}, a public API
 * method present from Flink 1.14 through 2.x. Every submission reaches it exactly once, whether the
 * user invokes {@code execute} or {@code executeAsync}, with or without a job name or a {@code
 * StreamGraph}: the other variants call it, and the overrides of {@code StreamContextEnvironment},
 * the Application-mode environment, call it through {@code super}.
 *
 * <p>In production the agent JAR is injected into the JobManager pod by the BlueGreen controller
 * via an init container — users do not need to modify their Dockerfile or set {@code
 * env.java.opts.jobmanager} manually.
 */
public class GateInjectionAgent {

    static final String INSTRUMENTED_TYPE =
            "org.apache.flink.streaming.api.environment.StreamExecutionEnvironment";

    /** The method of {@link #INSTRUMENTED_TYPE} that the agent intercepts. */
    static ElementMatcher.Junction<MethodDescription> interceptedMethod() {
        return named("executeAsync")
                .and(takesArguments(1))
                .and(takesArgument(0, named("org.apache.flink.streaming.api.graph.StreamGraph")));
    }

    public static void premain(String agentArgs, Instrumentation inst) throws Exception {
        // The instrumented StreamExecutionEnvironment bytecode will reference
        // GateInjectionInterceptor by name.  Make the agent JAR visible to the
        // system classloader so that reference resolves at runtime.
        String jarPath =
                GateInjectionAgent.class
                        .getProtectionDomain()
                        .getCodeSource()
                        .getLocation()
                        .getPath();
        inst.appendToSystemClassLoaderSearch(new JarFile(new File(jarPath)));

        System.err.println("[BlueGreen Agent] premain started, registering transformer");

        new AgentBuilder.Default()
                // Only the base class: intercepting a subclass override too would inject the gates
                // twice, as each override calls the base method
                .type(named(INSTRUMENTED_TYPE))
                .transform(
                        (builder, typeDescription, classLoader, module, protectionDomain) ->
                                builder.method(interceptedMethod())
                                        .intercept(
                                                MethodDelegation.to(
                                                        GateInjectionInterceptor.class)))
                .installOn(inst);

        System.err.println(
                "[BlueGreen Agent] transformer registered for StreamExecutionEnvironment");
    }
}
