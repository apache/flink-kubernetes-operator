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
 * <p>Failure contract:
 *
 * <ul>
 *   <li>If the agent cannot read the Flink configuration (unexpected agent bug), injection is
 *       skipped and the job continues — agent errors must never affect non-BlueGreen jobs.
 *   <li>If {@code bluegreen.gate.injection.enabled=true} and {@code GateInjectorExecutor} is not
 *       found, or its {@code injectGates} method is missing, the job <em>fails</em> with a clear
 *       message. This flag is only set for BlueGreen deployments, so a missing class means the
 *       bluegreen-client library is absent from the job JAR — a real misconfiguration.
 * </ul>
 *
 * <p>Classloader safety:
 *
 * <ul>
 *   <li>{@code StreamGraph} and {@code Configuration} are in {@code flink-core} / {@code
 *       flink-streaming-java} — loaded by the system classloader. The user classloader delegates to
 *       the parent for those packages (no bundling), so both sides of the reflective call see the
 *       exact same {@code Class} objects. ✓
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

        // Read config from the live environment instance rather than re-loading from disk.
        // GlobalConfiguration.loadConfiguration() reads flink-conf.yaml but misses keys
        // injected at container start via FLINK_PROPERTIES (e.g. kubernetes.namespace) when
        // the config volume is read-only and the entrypoint script cannot write them.
        // Guard this read: if it fails for an unexpected reason (agent bug), skip injection
        // rather than affecting the job.
        Configuration config = null;
        try {
            config =
                    (Configuration)
                            envInstance
                                    .getClass()
                                    .getMethod("getConfiguration")
                                    .invoke(envInstance);
        } catch (Exception e) {
            Throwable cause = (e.getCause() != null) ? e.getCause() : e;
            System.err.println(
                    "[BlueGreen Agent] Could not read Flink configuration, skipping gate injection: "
                            + cause);
        }

        if (config != null && config.getBoolean(GATE_INJECTION_ENABLED, false)) {
            // bluegreen.gate.injection.enabled is only set by the operator for BlueGreen
            // deployments. Any failure beyond this point is a real misconfiguration — fail the
            // job with a clear message so the operator can reflect the error in the deployment
            // status.
            ClassLoader userCL = Thread.currentThread().getContextClassLoader();

            Class<?> injectorClass;
            try {
                injectorClass = userCL.loadClass(INJECTOR_CLASS);
            } catch (ClassNotFoundException e) {
                String msg =
                        "[BlueGreen Agent] Gate injection is enabled but '"
                                + INJECTOR_CLASS
                                + "' was not found on the job classpath. "
                                + "Ensure the bluegreen-client library is included in your job JAR.";
                System.err.println(msg);
                throw new RuntimeException(msg, e);
            }

            // Locate injectGates by name to avoid any cross-CL type token issues.
            Method injectGates = null;
            for (Method m : injectorClass.getMethods()) {
                if ("injectGates".equals(m.getName()) && m.getParameterCount() == 2) {
                    injectGates = m;
                    break;
                }
            }

            if (injectGates == null) {
                String msg =
                        "[BlueGreen Agent] Gate injection is enabled but 'injectGates(StreamGraph, Configuration)' "
                                + "was not found on '"
                                + INJECTOR_CLASS
                                + "'. Verify the bluegreen-client version is compatible.";
                System.err.println(msg);
                throw new RuntimeException(msg);
            }

            injectGates.invoke(null, streamGraph, config);
            System.err.println("[BlueGreen Agent] Gate injection applied to StreamGraph");
        }

        return superCall.call();
    }
}
