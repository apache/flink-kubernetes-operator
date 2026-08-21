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

package org.apache.flink.kubernetes.operator.artifact;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.net.InetAddress;
import java.net.URL;

/**
 * Tests for {@link HttpArtifactFetcher}'s address-pinning helpers. These exercise the pinning logic
 * directly against constructed {@link InetAddress}es (no DNS lookups, no network), since a live
 * HTTP round trip through {@code localhost}/{@code 127.0.0.1} can't distinguish a pinned connection
 * from an unpinned one: both read identically.
 */
public class HttpArtifactFetcherTest {

    @Test
    public void testPinnedUrlUsesIpv4LiteralAsHost() throws Exception {
        var logicalUrl = new URL("http://example.com:8080/path/to/job.jar");
        var pinnedAddress = InetAddress.getByName("203.0.113.5");

        var pinned = HttpArtifactFetcher.pinnedUrl(logicalUrl, pinnedAddress);

        Assertions.assertEquals("http://203.0.113.5:8080/path/to/job.jar", pinned.toString());
    }

    @Test
    public void testPinnedUrlWrapsIpv6LiteralInBrackets() throws Exception {
        var logicalUrl = new URL("https://example.com/job.jar");
        var pinnedAddress = InetAddress.getByName("2001:db8::1");

        var pinned = HttpArtifactFetcher.pinnedUrl(logicalUrl, pinnedAddress);

        Assertions.assertEquals(
                "https://[" + pinnedAddress.getHostAddress() + "]/job.jar", pinned.toString());
        Assertions.assertTrue(pinned.getHost().startsWith("["));
        Assertions.assertTrue(pinned.getHost().endsWith("]"));
    }

    @Test
    public void testPinnedUrlPreservesQueryString() throws Exception {
        var logicalUrl = new URL("http://example.com/download/file.jar?some=params");
        var pinnedAddress = InetAddress.getByName("203.0.113.5");

        var pinned = HttpArtifactFetcher.pinnedUrl(logicalUrl, pinnedAddress);

        Assertions.assertEquals(
                "http://203.0.113.5/download/file.jar?some=params", pinned.toString());
    }

    @Test
    public void testHostHeaderValueIncludesExplicitPort() throws Exception {
        var logicalUrl = new URL("http://example.com:8080/job.jar");

        Assertions.assertEquals(
                "example.com:8080", HttpArtifactFetcher.hostHeaderValue(logicalUrl));
    }

    @Test
    public void testHostHeaderValueOmitsDefaultPort() throws Exception {
        var logicalUrl = new URL("https://example.com/job.jar");

        Assertions.assertEquals("example.com", HttpArtifactFetcher.hostHeaderValue(logicalUrl));
    }
}
