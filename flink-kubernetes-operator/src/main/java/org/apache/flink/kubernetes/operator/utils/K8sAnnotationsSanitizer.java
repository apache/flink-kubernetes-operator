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

import org.apache.flink.annotation.VisibleForTesting;

import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * Utility class for sanitizing Kubernetes annotations as per k8s rules. See <a
 * href="https://kubernetes.io/docs/concepts/overview/working-with-objects/annotations/#syntax-and-character-set">Kubernetes
 * Annotations Syntax and Character Set</a>
 */
public class K8sAnnotationsSanitizer {

    private static final int MAX_PREFIX_LENGTH = 253;
    private static final int MAX_NAME_LENGTH = 63;

    // Name: starts and ends with alphanumeric, allows alphanum, '-', '_', '.' in middle
    private static final Pattern NAME_PATTERN =
            Pattern.compile("^[a-zA-Z0-9]([a-zA-Z0-9_.-]*[a-zA-Z0-9])?$");

    // DNS label: alphanumeric, may have hyphens inside, length ≤ 63
    private static final Pattern DNS_LABEL_PATTERN =
            Pattern.compile("^[a-zA-Z0-9]([-a-zA-Z0-9]*[a-zA-Z0-9])?$");

    /**
     * Sanitizes the input annotations by validating keys and cleaning values. Only includes entries
     * with valid keys and non-null values.
     */
    public static Map<String, String> sanitizeAnnotations(Map<String, String> annotations) {
        Map<String, String> sanitized = new HashMap<>();
        if (annotations == null) {
            return sanitized;
        }

        for (Map.Entry<String, String> entry : annotations.entrySet()) {
            String key = entry.getKey();
            String value = entry.getValue();

            if (isValidAnnotationKey(key)) {
                String sanitizedValue = sanitizeAnnotationValue(value);
                if (sanitizedValue != null) {
                    sanitized.put(key, sanitizedValue);
                }
            }
        }
        return sanitized;
    }

    /** Validates the annotation key according to Kubernetes rules: Optional prefix + "/" + name. */
    @VisibleForTesting
    static boolean isValidAnnotationKey(String key) {
        if (key == null || key.isEmpty()) {
            return false;
        }

        String[] parts = key.split("/", 2);
        if (parts.length == 2) {
            return isValidPrefix(parts[0]) && isValidName(parts[1]);
        } else {
            return isValidName(parts[0]);
        }
    }

    /**
     * Validates the prefix as a DNS subdomain: series of DNS labels separated by dots, total length
     * ≤ 253.
     */
    private static boolean isValidPrefix(String prefix) {
        if (prefix.length() > MAX_PREFIX_LENGTH) {
            return false;
        }
        if (prefix.endsWith(".")) {
            return false; // no trailing dot allowed
        }
        String[] labels = prefix.split("\\.");
        for (String label : labels) {
            if (label.isEmpty() || label.length() > 63) {
                return false;
            }
            if (!DNS_LABEL_PATTERN.matcher(label).matches()) {
                return false;
            }
        }
        return true;
    }

    /** Validates the name part of the key. */
    private static boolean isValidName(String name) {
        return name != null
                && name.length() <= MAX_NAME_LENGTH
                && NAME_PATTERN.matcher(name).matches();
    }

    /**
     * Sanitizes the annotation value by trimming and removing control characters, replacing
     * newlines and tabs with spaces. No length limit.
     */
    @VisibleForTesting
    static String sanitizeAnnotationValue(String value) {
        if (value == null) {
            return null;
        }

        // Trim whitespace, remove control chars except \r \n \t, replace those with space
        String sanitized =
                value.trim()
                        .replaceAll("[\\p{Cntrl}&&[^\r\n\t]]", "")
                        .replaceAll("[\\r\\n\\t]+", " ");

        return sanitized;
    }
}
