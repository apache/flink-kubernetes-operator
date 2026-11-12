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

import io.fabric8.kubernetes.client.http.HttpClient;
import org.junit.jupiter.api.Test;

import java.util.Locale;
import java.util.ServiceLoader;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Tests for {@link KubernetesClientUtils}. */
public class KubernetesClientUtilsTest {

    @Test
    public void testSelectedHttpClientIsTheOnlyFactoryOnClasspath() {
        // With several factories on the classpath fabric8 picks one of them at random, which
        // silently replaces the HTTP client the operator is built and tested with
        var factories =
                ServiceLoader.load(HttpClient.Factory.class).stream()
                        .map(provider -> provider.type().getSimpleName())
                        .toList();
        assertEquals(
                1,
                factories.size(),
                "Expected exactly one fabric8 HttpClient.Factory on the classpath, found "
                        + factories);

        // Set by surefire from the fabric8.httpclient.impl Maven property
        var selected = System.getProperty("fabric8.httpclient.impl", "okhttp");
        assertTrue(
                factories
                        .get(0)
                        .toLowerCase(Locale.ROOT)
                        .startsWith(selected.toLowerCase(Locale.ROOT)),
                "Expected the " + selected + " client, found " + factories.get(0));
    }
}
