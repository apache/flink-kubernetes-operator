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

import javax.annotation.Nullable;

import java.util.Map;
import java.util.OptionalDouble;

/**
 * Utility methods for translating Kubernetes resource definitions ({@link ResourceRequirements} and
 * the deprecated {@code Resource} memory strings) into the values the operator feeds into the Flink
 * configuration. Kept in one place so the deploy path ({@code FlinkConfigBuilder}) and the
 * autoscaler memory tuning path ({@code KubernetesScalingRealizer}) parse and interpret resources
 * the same way, and so the parsing rules can be unit tested in isolation.
 */
public final class ResourceConfigUtils {

    private ResourceConfigUtils() {}

    /**
     * Parses a memory string into the canonical byte representation Flink expects. Accepts both
     * Flink {@link MemorySize} syntax (e.g. {@code "2048m"}) and Kubernetes quantity syntax (e.g.
     * {@code "2Gi"}), falling back to the latter when the former cannot parse the value.
     */
    public static String parseResourceMemoryString(String memory) {
        try {
            return MemorySize.parse(memory).toString();
        } catch (IllegalArgumentException e) {
            var memoryQuantity = formatMemoryStringForK8sSpec(memory);
            return Quantity.parse(memoryQuantity).getNumericalAmount() + "";
        }
    }

    private static String formatMemoryStringForK8sSpec(String memory) {
        var memoryQuantity = memory.trim().replaceAll("\\s", "").toUpperCase();
        if (memoryQuantity.endsWith("B")) {
            memoryQuantity = memoryQuantity.substring(0, memoryQuantity.length() - 1);
        }
        if (memoryQuantity.endsWith("I")) {
            memoryQuantity = memoryQuantity.substring(0, memoryQuantity.length() - 1) + "i";
        }
        return memoryQuantity;
    }

    /**
     * Returns true only when the given resource requirements actually carry usable requests or
     * limits. A non-null but empty {@link ResourceRequirements} (e.g. an empty {@code resources:
     * {}} block in the spec) must not suppress the deprecated {@code resource} field, so callers
     * gate on this rather than a plain null check.
     */
    public static boolean hasResourceRequirements(@Nullable ResourceRequirements resources) {
        if (resources == null) {
            return false;
        }
        Map<String, Quantity> requests = resources.getRequests();
        Map<String, Quantity> limits = resources.getLimits();
        return (requests != null && !requests.isEmpty()) || (limits != null && !limits.isEmpty());
    }

    /**
     * Returns the requested value for the given key, falling back to the limit when no request is
     * set (mirroring how Kubernetes defaults requests to limits). Returns null when neither is set
     * or the requirements are null.
     */
    @Nullable
    public static Quantity getRequestOrLimit(@Nullable ResourceRequirements resources, String key) {
        if (resources == null) {
            return null;
        }
        return getRequestOrLimit(resources.getRequests(), resources.getLimits(), key);
    }

    @Nullable
    public static Quantity getRequestOrLimit(
            @Nullable Map<String, Quantity> requests,
            @Nullable Map<String, Quantity> limits,
            String key) {
        if (requests != null && requests.get(key) != null) {
            return requests.get(key);
        }
        if (limits != null && limits.get(key) != null) {
            return limits.get(key);
        }
        return null;
    }

    /**
     * Computes the limit factor ({@code limits / requests}) for the given resource key. This is
     * only meaningful when both a request and a limit are explicitly set and the request is
     * positive, so the result is empty otherwise.
     */
    public static OptionalDouble computeLimitFactor(
            @Nullable Map<String, Quantity> requests,
            @Nullable Map<String, Quantity> limits,
            String key) {
        if (requests == null
                || requests.get(key) == null
                || limits == null
                || limits.get(key) == null) {
            return OptionalDouble.empty();
        }
        double request = requests.get(key).getNumericalAmount().doubleValue();
        double limit = limits.get(key).getNumericalAmount().doubleValue();
        if (request <= 0) {
            return OptionalDouble.empty();
        }
        return OptionalDouble.of(limit / request);
    }

    /**
     * Convenience overload of {@link #computeLimitFactor(Map, Map, String)} that reads the request
     * and limit maps directly from a {@link ResourceRequirements}.
     */
    public static OptionalDouble computeLimitFactor(
            @Nullable ResourceRequirements resources, String key) {
        if (resources == null) {
            return OptionalDouble.empty();
        }
        return computeLimitFactor(resources.getRequests(), resources.getLimits(), key);
    }

    /** Returns true when memory is defined via either {@code requests} or {@code limits}. */
    public static boolean isMemoryDefined(@Nullable ResourceRequirements resources) {
        return getRequestOrLimit(resources, CrdConstants.MEMORY) != null;
    }
}
