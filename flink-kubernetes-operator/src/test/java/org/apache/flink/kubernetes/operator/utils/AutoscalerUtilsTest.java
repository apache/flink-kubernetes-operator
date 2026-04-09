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

package org.apache.flink.kubernetes.operator.utils;

import org.apache.flink.configuration.Configuration;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link AutoscalerUtils}. */
public class AutoscalerUtilsTest {

    /**
     * When no plugin is found on the classpath for the given interface, the default supplier should
     * be used.
     */
    @Test
    void testDiscoverOrDefaultFallsBackToDefault() {
        var conf = new Configuration();
        NoPluginInterface defaultValue = new DefaultImpl();

        // NoPluginInterface is not registered in any plugin directory,
        // so discoverOrDefault must return the supplier's value.
        NoPluginInterface result =
                AutoscalerUtils.discoverOrDefault(
                        conf, NoPluginInterface.class, () -> defaultValue);

        assertThat(result).isSameAs(defaultValue);
    }

    /**
     * Verify that the default factory is invoked exactly once (not cached or called multiple
     * times).
     */
    @Test
    void testDefaultFactoryInvokedOnce() {
        var conf = new Configuration();
        int[] invocationCount = {0};

        AutoscalerUtils.discoverOrDefault(
                conf,
                NoPluginInterface.class,
                () -> {
                    invocationCount[0]++;
                    return new DefaultImpl();
                });

        assertThat(invocationCount[0]).isEqualTo(1);
    }

    /**
     * Verify that discoverOrDefault returns the exact instance from the default supplier, not a
     * copy.
     */
    @Test
    void testReturnsSameInstanceFromDefaultSupplier() {
        var conf = new Configuration();
        NoPluginInterface expected = new DefaultImpl();

        NoPluginInterface result =
                AutoscalerUtils.discoverOrDefault(conf, NoPluginInterface.class, () -> expected);

        assertThat(result).isSameAs(expected);
    }

    /** A dummy interface that will never be found via plugin discovery. */
    public interface NoPluginInterface {}

    /** A simple default implementation of {@link NoPluginInterface} for testing. */
    private static class DefaultImpl implements NoPluginInterface {}
}
