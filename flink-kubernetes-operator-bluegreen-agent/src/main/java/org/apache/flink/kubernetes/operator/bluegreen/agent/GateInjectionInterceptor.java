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

import org.apache.flink.configuration.Configuration;

import net.bytebuddy.implementation.bind.annotation.Argument;
import net.bytebuddy.implementation.bind.annotation.RuntimeType;
import net.bytebuddy.implementation.bind.annotation.SuperCall;
import net.bytebuddy.implementation.bind.annotation.This;

import java.lang.reflect.Method;
import java.util.concurrent.Callable;

/**
 * Byte Buddy interceptor invoked in place of {@code
 * StreamExecutionEnvironment.execute(StreamGraph)}.
 *
 * <p>When {@code bluegreen.gate.injection.enabled=true}:
 *
 * <ol>
 *   <li>Loads {@code GateInjectorExecutor} from the <em>user code classloader</em> (thread context
 *       CL at the time execute() is called). This ensures that the injected {@code
 *       WatermarkGateProcessFunction} instances are created by the user classloader and can be
 *       deserialized on TaskManagers from the user JAR.
 *   <li>Calls {@code GateInjectorExecutor.injectGates(StreamGraph, Configuration)} reflectively,
 *       mutating the graph in-place before the original execute() runs.
 * </ol>
 *
 * <p>Classloader safety:
 *
 * <ul>
 *   <li>{@code StreamGraph} and {@code Configuration} are in {@code flink-core} / {@code
 *       flink-streaming-java} — loaded by the system classloader. The user classloader delegates to
 *       the parent for those packages (no bundling), so both sides of the reflective call see the
 *       exact same {@code Class} objects. ✓
 *   <li>If {@code GateInjectorExecutor} is not found on the user classpath (e.g. a non-BlueGreen
 *       job), injection is silently skipped. ✓
 * </ul>
 */
public class GateInjectionInterceptor {

    private static final String GATE_INJECTION_ENABLED = "bluegreen.gate.injection.enabled";
    private static final String INJECTOR_CLASS =
            "org.apache.flink.kubernetes.operator.bluegreen.client.GateInjectorExecutor";

    @RuntimeType
    public static Object intercept(
            @This Object envInstance,
            @Argument(0) Object streamGraph,
            @SuperCall Callable<Object> superCall)
            throws Exception {

        try {
            // Read config from the live environment instance rather than re-loading from disk.
            // GlobalConfiguration.loadConfiguration() reads flink-conf.yaml but misses keys
            // injected at container start via FLINK_PROPERTIES (e.g. kubernetes.namespace) when
            // the config volume is read-only and the entrypoint script cannot write them.
            Configuration config =
                    (Configuration)
                            envInstance
                                    .getClass()
                                    .getMethod("getConfiguration")
                                    .invoke(envInstance);

            if (config.getBoolean(GATE_INJECTION_ENABLED, false)) {
                // The thread context classloader is the user code classloader at this
                // point — Flink sets it before invoking user main() in Application mode.
                ClassLoader userCL = Thread.currentThread().getContextClassLoader();

                try {
                    Class<?> injectorClass = userCL.loadClass(INJECTOR_CLASS);

                    // Locate injectGates by name to avoid any cross-CL type token issues.
                    Method injectGates = null;
                    for (Method m : injectorClass.getMethods()) {
                        if ("injectGates".equals(m.getName()) && m.getParameterCount() == 2) {
                            injectGates = m;
                            break;
                        }
                    }

                    if (injectGates != null) {
                        injectGates.invoke(null, streamGraph, config);
                        System.err.println(
                                "[BlueGreen Agent] Gate injection applied to StreamGraph");
                    } else {
                        System.err.println(
                                "[BlueGreen Agent] injectGates method not found on "
                                        + INJECTOR_CLASS);
                    }

                } catch (ClassNotFoundException e) {
                    // Not a BlueGreen-enabled job — skip silently.
                    System.err.println(
                            "[BlueGreen Agent] "
                                    + INJECTOR_CLASS
                                    + " not on user classpath, skipping injection");
                }
            }
        } catch (Exception e) {
            // Never fail the job due to agent errors — log and continue.
            // Method.invoke wraps exceptions in InvocationTargetException whose own message is
            // null; the real exception is in getCause().
            Throwable cause = (e.getCause() != null) ? e.getCause() : e;
            System.err.println(
                    "[BlueGreen Agent] ERROR during gate injection, continuing: " + cause);
        }

        return superCall.call();
    }
}
