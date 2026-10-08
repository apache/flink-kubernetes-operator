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
import net.bytebuddy.implementation.MethodDelegation;

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
 * <p>Intercept point: {@code StreamExecutionEnvironment.execute(StreamGraph)}. This is a stable
 * public API method present from Flink 1.14 through 2.x. It is called regardless of whether the
 * user invokes {@code env.execute()}, {@code env.execute(String)}, or {@code
 * env.execute(StreamGraph)} directly.
 *
 * <p>In production the agent JAR is injected into the JobManager pod by the BlueGreen controller
 * via an init container — users do not need to modify their Dockerfile or set {@code
 * env.java.opts.jobmanager} manually.
 */
public class GateInjectionAgent {

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
                // Instrument both the base class and its Application-mode subclass.
                //
                // StreamContextEnvironment (flink-clients) overrides execute(StreamGraph)
                // and is the concrete type used in Application mode and client-side execution.
                // StreamExecutionEnvironment.execute(StreamGraph) is never reached via virtual
                // dispatch when the runtime type is StreamContextEnvironment, so we must
                // intercept the override directly.
                //
                // StreamExecutionEnvironment is kept as a fallback for local/test execution
                // where LocalStreamEnvironment does NOT override execute(StreamGraph).
                .type(
                        named(
                                        "org.apache.flink.streaming.api.environment.StreamExecutionEnvironment")
                                .or(
                                        named(
                                                "org.apache.flink.client.program.StreamContextEnvironment")))
                .transform(
                        (builder, typeDescription, classLoader, module, protectionDomain) ->
                                builder.method(
                                                named("execute")
                                                        .and(takesArguments(1))
                                                        .and(
                                                                takesArgument(
                                                                        0,
                                                                        named(
                                                                                "org.apache.flink.streaming.api.graph.StreamGraph"))))
                                        .intercept(
                                                MethodDelegation.to(
                                                        GateInjectionInterceptor.class)))
                .installOn(inst);

        System.err.println(
                "[BlueGreen Agent] transformer registered for StreamExecutionEnvironment + StreamContextEnvironment");
    }
}
