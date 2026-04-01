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

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Tests for {@link GateInjectionInterceptor}. */
public class GateInjectionInterceptorTest {

    @BeforeEach
    public void resetStub() {
        org.apache.flink.kubernetes.operator.bluegreen.client.GateInjectorExecutor.injected = false;
        Thread.currentThread().setContextClassLoader(getClass().getClassLoader());
    }

    @Test
    public void testIntercept_injectionDisabled_superCallInvoked() throws Exception {
        Configuration config = new Configuration();
        // bluegreen.gate.injection.enabled not set — defaults to false
        AtomicBoolean superCallInvoked = new AtomicBoolean(false);

        GateInjectionInterceptor.intercept(
                new MockEnv(config), new Object(), superCallFlagWith(superCallInvoked));

        assertTrue(superCallInvoked.get());
        assertFalse(
                org.apache.flink.kubernetes.operator.bluegreen.client.GateInjectorExecutor
                        .injected);
    }

    @Test
    public void testIntercept_injectionEnabled_injectorFound_injectGatesCalled() throws Exception {
        Configuration config = new Configuration();
        config.setString("bluegreen.gate.injection.enabled", "true");
        AtomicBoolean superCallInvoked = new AtomicBoolean(false);

        // Test classloader has GateInjectorExecutor on the path — set in @BeforeEach
        GateInjectionInterceptor.intercept(
                new MockEnv(config), new Object(), superCallFlagWith(superCallInvoked));

        assertTrue(superCallInvoked.get(), "superCall must always be invoked");
        assertTrue(
                org.apache.flink.kubernetes.operator.bluegreen.client.GateInjectorExecutor.injected,
                "injectGates should have been called");
    }

    @Test
    public void testIntercept_injectionEnabled_classNotFound_jobFails() {
        Configuration config = new Configuration();
        config.setString("bluegreen.gate.injection.enabled", "true");

        // Swap to a classloader that does not have GateInjectorExecutor
        Thread.currentThread()
                .setContextClassLoader(
                        new ClassLoader(null) {
                            @Override
                            public Class<?> loadClass(String name) throws ClassNotFoundException {
                                throw new ClassNotFoundException(name);
                            }
                        });

        assertThrows(
                RuntimeException.class,
                () ->
                        GateInjectionInterceptor.intercept(
                                new MockEnv(config), new Object(), () -> null),
                "Missing bluegreen-client library must fail the job");
    }

    @Test
    public void testIntercept_exceptionDuringConfigRead_superCallStillInvoked() throws Exception {
        // An envInstance whose getConfiguration() throws — agent must never affect the job
        Object badEnv =
                new Object() {
                    @SuppressWarnings("unused")
                    public Configuration getConfiguration() {
                        throw new RuntimeException("simulated config failure");
                    }
                };
        AtomicBoolean superCallInvoked = new AtomicBoolean(false);

        GateInjectionInterceptor.intercept(
                badEnv, new Object(), superCallFlagWith(superCallInvoked));

        assertTrue(
                superCallInvoked.get(), "superCall must still be invoked when config read fails");
    }

    // ==================== Helpers ====================

    private static Callable<Object> superCallFlagWith(AtomicBoolean flag) {
        return () -> {
            flag.set(true);
            return null;
        };
    }

    /**
     * Minimal environment stub with a {@code getConfiguration()} method the agent can reflect on.
     */
    private static class MockEnv {
        private final Configuration config;

        MockEnv(Configuration config) {
            this.config = config;
        }

        @SuppressWarnings("unused")
        public Configuration getConfiguration() {
            return config;
        }
    }
}
