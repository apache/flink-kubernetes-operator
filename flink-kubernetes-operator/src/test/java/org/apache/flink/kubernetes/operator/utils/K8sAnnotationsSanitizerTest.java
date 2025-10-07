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

import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link K8sAnnotationsSanitizer}. */
public class K8sAnnotationsSanitizerTest {

    @Test
    public void testValidKeysWithoutPrefix() {
        assertThat(K8sAnnotationsSanitizer.isValidAnnotationKey("foo")).isTrue();
        assertThat(K8sAnnotationsSanitizer.isValidAnnotationKey("foo-bar")).isTrue();
        assertThat(K8sAnnotationsSanitizer.isValidAnnotationKey("foo.bar")).isTrue();
        assertThat(K8sAnnotationsSanitizer.isValidAnnotationKey("foo_bar")).isTrue();
        assertThat(K8sAnnotationsSanitizer.isValidAnnotationKey("f")).isTrue();
        assertThat(K8sAnnotationsSanitizer.isValidAnnotationKey("f1")).isTrue();
        assertThat(K8sAnnotationsSanitizer.isValidAnnotationKey("1f")).isTrue();
    }

    @Test
    public void testInvalidKeysWithoutPrefix() {
        assertThat(K8sAnnotationsSanitizer.isValidAnnotationKey("")).isFalse();
        assertThat(K8sAnnotationsSanitizer.isValidAnnotationKey(null)).isFalse();
        assertThat(K8sAnnotationsSanitizer.isValidAnnotationKey("-foo")).isFalse();
        assertThat(K8sAnnotationsSanitizer.isValidAnnotationKey("foo-")).isFalse();
        assertThat(K8sAnnotationsSanitizer.isValidAnnotationKey(".foo")).isFalse();
        assertThat(K8sAnnotationsSanitizer.isValidAnnotationKey("foo.")).isFalse();
        assertThat(K8sAnnotationsSanitizer.isValidAnnotationKey("_foo")).isFalse();
        assertThat(K8sAnnotationsSanitizer.isValidAnnotationKey("foo_")).isFalse();
        assertThat(K8sAnnotationsSanitizer.isValidAnnotationKey("foo@bar")).isFalse();
        assertThat(K8sAnnotationsSanitizer.isValidAnnotationKey("foo/bar/baz"))
                .isFalse(); // multiple slashes invalid
    }

    @Test
    public void testValidKeysWithPrefix() {
        assertThat(K8sAnnotationsSanitizer.isValidAnnotationKey("example.com/foo")).isTrue();
        assertThat(K8sAnnotationsSanitizer.isValidAnnotationKey("a.b.c/foo-bar")).isTrue();
        assertThat(K8sAnnotationsSanitizer.isValidAnnotationKey("abc/foo_bar")).isTrue();
        assertThat(K8sAnnotationsSanitizer.isValidAnnotationKey("abc/foo.bar")).isTrue();
        assertThat(K8sAnnotationsSanitizer.isValidAnnotationKey("a/foo")).isTrue();
    }

    @Test
    public void testInvalidPrefix() {
        String longPrefix = "a".repeat(254);
        assertThat(K8sAnnotationsSanitizer.isValidAnnotationKey(longPrefix + "/foo")).isFalse();

        String longLabel = "a".repeat(64);
        assertThat(K8sAnnotationsSanitizer.isValidAnnotationKey(longLabel + ".com/foo")).isFalse();

        assertThat(K8sAnnotationsSanitizer.isValidAnnotationKey("ex_ample.com/foo")).isFalse();
        assertThat(K8sAnnotationsSanitizer.isValidAnnotationKey("-example.com/foo")).isFalse();
        assertThat(K8sAnnotationsSanitizer.isValidAnnotationKey("example-.com/foo")).isFalse();

        assertThat(K8sAnnotationsSanitizer.isValidAnnotationKey(".example.com/foo")).isFalse();
        assertThat(K8sAnnotationsSanitizer.isValidAnnotationKey("example..com/foo")).isFalse();
        assertThat(K8sAnnotationsSanitizer.isValidAnnotationKey("example.com./foo")).isFalse();
    }

    @Test
    public void testInvalidNameWithPrefix() {
        assertThat(K8sAnnotationsSanitizer.isValidAnnotationKey("example.com/-foo")).isFalse();
        assertThat(K8sAnnotationsSanitizer.isValidAnnotationKey("example.com/foo-")).isFalse();
        assertThat(K8sAnnotationsSanitizer.isValidAnnotationKey("example.com/.foo")).isFalse();
        assertThat(K8sAnnotationsSanitizer.isValidAnnotationKey("example.com/foo.")).isFalse();
        assertThat(K8sAnnotationsSanitizer.isValidAnnotationKey("example.com/foo@bar")).isFalse();
        assertThat(K8sAnnotationsSanitizer.isValidAnnotationKey("example.com/")).isFalse();
    }

    @Test
    public void testSanitizeAnnotationValue() {
        assertThat(K8sAnnotationsSanitizer.sanitizeAnnotationValue(null)).isNull();

        assertThat(K8sAnnotationsSanitizer.sanitizeAnnotationValue("  value  ")).isEqualTo("value");

        String rawValue = "line1\nline2\r\nline3\tend\u0007"; // \u0007 is bell (control char)
        String expected = "line1 line2 line3 end";
        assertThat(K8sAnnotationsSanitizer.sanitizeAnnotationValue(rawValue)).isEqualTo(expected);

        String unicode = "café résumé – test";
        assertThat(K8sAnnotationsSanitizer.sanitizeAnnotationValue(unicode)).isEqualTo(unicode);
    }

    @Test
    public void testSanitizeAnnotations() {
        Map<String, String> input =
                Map.of(
                        "valid-key", " some value ",
                        "example.com/valid-name", "value\nwith\nnewlines",
                        "invalid key", "value",
                        "prefix_with_underscore/foo", "value",
                        "validprefix/foo-", "value",
                        "example.com/invalid_name@", "value");

        Map<String, String> sanitized = K8sAnnotationsSanitizer.sanitizeAnnotations(input);

        assertThat(sanitized).hasSize(2).containsKeys("valid-key", "example.com/valid-name");

        assertThat(sanitized.get("valid-key")).isEqualTo("some value");
        assertThat(sanitized.get("example.com/valid-name")).isEqualTo("value with newlines");
    }
}
