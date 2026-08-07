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

import org.apache.flink.configuration.MemorySize;
import org.apache.flink.kubernetes.operator.api.CrdConstants;

import io.fabric8.kubernetes.api.model.Quantity;
import io.fabric8.kubernetes.api.model.ResourceRequirements;
import io.fabric8.kubernetes.api.model.ResourceRequirementsBuilder;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.OptionalDouble;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Test class for {@link ResourceConfigUtils}. */
public class ResourceConfigUtilsTest {

    @Test
    public void testParseResourceMemoryStringFlinkSyntax() {
        // Flink MemorySize syntax is parsed directly.
        assertEquals(
                MemorySize.parse("2048m").toString(),
                ResourceConfigUtils.parseResourceMemoryString("2048m"));
    }

    @Test
    public void testParseResourceMemoryStringKubernetesSyntax() {
        // "2Gi" is not valid Flink syntax, so it falls back to Kubernetes quantity parsing.
        assertEquals(
                String.valueOf(2L * 1024 * 1024 * 1024),
                ResourceConfigUtils.parseResourceMemoryString("2Gi"));
    }

    @Test
    public void testHasResourceRequirements() {
        assertFalse(ResourceConfigUtils.hasResourceRequirements(null));
        assertFalse(ResourceConfigUtils.hasResourceRequirements(new ResourceRequirements()));
        assertFalse(
                ResourceConfigUtils.hasResourceRequirements(
                        new ResourceRequirementsBuilder().withRequests(Map.of()).build()));
        assertTrue(
                ResourceConfigUtils.hasResourceRequirements(
                        new ResourceRequirementsBuilder()
                                .withRequests(Map.of(CrdConstants.MEMORY, new Quantity("1Gi")))
                                .build()));
        assertTrue(
                ResourceConfigUtils.hasResourceRequirements(
                        new ResourceRequirementsBuilder()
                                .withLimits(Map.of(CrdConstants.MEMORY, new Quantity("1Gi")))
                                .build()));
    }

    @Test
    public void testGetRequestOrLimitPrefersRequest() {
        ResourceRequirements resources =
                new ResourceRequirementsBuilder()
                        .withRequests(Map.of(CrdConstants.MEMORY, new Quantity("1Gi")))
                        .withLimits(Map.of(CrdConstants.MEMORY, new Quantity("2Gi")))
                        .build();
        assertEquals(
                new Quantity("1Gi"),
                ResourceConfigUtils.getRequestOrLimit(resources, CrdConstants.MEMORY));
    }

    @Test
    public void testGetRequestOrLimitFallsBackToLimit() {
        // Kubernetes defaults requests to limits when requests is omitted.
        ResourceRequirements resources =
                new ResourceRequirementsBuilder()
                        .withLimits(Map.of(CrdConstants.CPU, new Quantity("2")))
                        .build();
        assertEquals(
                new Quantity("2"),
                ResourceConfigUtils.getRequestOrLimit(resources, CrdConstants.CPU));
    }

    @Test
    public void testGetRequestOrLimitReturnsNullWhenAbsent() {
        assertNull(ResourceConfigUtils.getRequestOrLimit((ResourceRequirements) null, "memory"));
        assertNull(
                ResourceConfigUtils.getRequestOrLimit(
                        new ResourceRequirements(), CrdConstants.MEMORY));
    }

    @Test
    public void testComputeLimitFactor() {
        ResourceRequirements resources =
                new ResourceRequirementsBuilder()
                        .withRequests(Map.of(CrdConstants.CPU, new Quantity("1")))
                        .withLimits(Map.of(CrdConstants.CPU, new Quantity("2")))
                        .build();
        assertEquals(
                OptionalDouble.of(2.0),
                ResourceConfigUtils.computeLimitFactor(resources, CrdConstants.CPU));
    }

    @Test
    public void testComputeLimitFactorEmptyWhenRequestOrLimitMissing() {
        // Only a request, no limit.
        assertEquals(
                OptionalDouble.empty(),
                ResourceConfigUtils.computeLimitFactor(
                        new ResourceRequirementsBuilder()
                                .withRequests(Map.of(CrdConstants.CPU, new Quantity("1")))
                                .build(),
                        CrdConstants.CPU));
        // Only a limit, no request.
        assertEquals(
                OptionalDouble.empty(),
                ResourceConfigUtils.computeLimitFactor(
                        new ResourceRequirementsBuilder()
                                .withLimits(Map.of(CrdConstants.CPU, new Quantity("2")))
                                .build(),
                        CrdConstants.CPU));
    }

    @Test
    public void testComputeLimitFactorEmptyWhenRequestIsZero() {
        ResourceRequirements resources =
                new ResourceRequirementsBuilder()
                        .withRequests(Map.of(CrdConstants.CPU, new Quantity("0")))
                        .withLimits(Map.of(CrdConstants.CPU, new Quantity("2")))
                        .build();
        assertEquals(
                OptionalDouble.empty(),
                ResourceConfigUtils.computeLimitFactor(resources, CrdConstants.CPU));
    }

    /**
     * Mixed-unit handling: memory requests/limits and CPU requests/limits expressed in different
     * Kubernetes units must still resolve to the right config values and limit factors.
     */
    @Test
    public void testMixedUnitsMemoryAndCpu() {
        ResourceRequirements resources =
                new ResourceRequirementsBuilder()
                        .withRequests(
                                Map.of(
                                        CrdConstants.MEMORY, new Quantity("1Gi"),
                                        CrdConstants.CPU, new Quantity("500m")))
                        .withLimits(
                                Map.of(
                                        CrdConstants.MEMORY, new Quantity("2048Mi"),
                                        CrdConstants.CPU, new Quantity("1")))
                        .build();

        // 1Gi request memory resolves to the expected byte count.
        Quantity memory = ResourceConfigUtils.getRequestOrLimit(resources, CrdConstants.MEMORY);
        assertEquals(
                MemorySize.parse(String.valueOf(1024L * 1024 * 1024)),
                MemorySize.parse(ResourceConfigUtils.parseResourceMemoryString(memory.toString())));

        // 500m request CPU resolves to 0.5 cores.
        Quantity cpu = ResourceConfigUtils.getRequestOrLimit(resources, CrdConstants.CPU);
        assertEquals(0.5, cpu.getNumericalAmount().doubleValue());

        // 2048Mi / 1Gi == 2.0 memory limit factor.
        assertEquals(
                OptionalDouble.of(2.0),
                ResourceConfigUtils.computeLimitFactor(resources, CrdConstants.MEMORY));
        // 1 / 500m == 2.0 CPU limit factor.
        assertEquals(
                OptionalDouble.of(2.0),
                ResourceConfigUtils.computeLimitFactor(resources, CrdConstants.CPU));
    }

    @Test
    public void testIsMemoryDefined() {
        assertFalse(ResourceConfigUtils.isMemoryDefined(null));
        assertFalse(ResourceConfigUtils.isMemoryDefined(new ResourceRequirements()));
        assertTrue(
                ResourceConfigUtils.isMemoryDefined(
                        new ResourceRequirementsBuilder()
                                .withLimits(Map.of(CrdConstants.MEMORY, new Quantity("1Gi")))
                                .build()));
    }
}
