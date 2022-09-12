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

package org.apache.flink.kubernetes.operator.ssl;

import org.apache.flink.shaded.netty4.io.netty.buffer.ByteBufAllocator;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http2.Http2SecurityUtil;
import org.apache.flink.shaded.netty4.io.netty.handler.ssl.ApplicationProtocolNegotiator;
import org.apache.flink.shaded.netty4.io.netty.handler.ssl.SslContext;
import org.apache.flink.shaded.netty4.io.netty.handler.ssl.SslContextBuilder;
import org.apache.flink.shaded.netty4.io.netty.handler.ssl.SupportedCipherSuiteFilter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLSessionContext;

import java.io.File;
import java.io.InputStream;
import java.nio.file.Files;
import java.security.KeyStore;
import java.util.List;

/** SSL context which is able to reload keystore. */
public class ReloadableSslContext extends SslContext {
    private static final Logger LOG = LoggerFactory.getLogger(ReloadableSslContext.class);

    private final String keystorePath;
    private final String keystoreType;
    private final String keystorePassword;
    private volatile SslContext sslContext;

    public ReloadableSslContext(String keystorePath, String keystoreType, String keystorePassword)
            throws Exception {
        this.keystorePath = keystorePath;
        this.keystoreType = keystoreType;
        this.keystorePassword = keystorePassword;
        loadContext();
    }

    @Override
    public boolean isClient() {
        return sslContext.isClient();
    }

    @Override
    public List<String> cipherSuites() {
        return sslContext.cipherSuites();
    }

    @Override
    public ApplicationProtocolNegotiator applicationProtocolNegotiator() {
        return sslContext.applicationProtocolNegotiator();
    }

    @Override
    public SSLEngine newEngine(ByteBufAllocator byteBufAllocator) {
        return sslContext.newEngine(byteBufAllocator);
    }

    @Override
    public SSLEngine newEngine(ByteBufAllocator byteBufAllocator, String s, int i) {
        return sslContext.newEngine(byteBufAllocator, s, i);
    }

    @Override
    public SSLSessionContext sessionContext() {
        return sslContext.sessionContext();
    }

    public void reload() throws Exception {
        loadContext();
    }

    private void loadContext() throws Exception {
        LOG.info("Creating keystore with type: " + keystoreType);
        KeyStore keyStore = KeyStore.getInstance(keystoreType);
        LOG.info("Loading keystore from file: " + keystorePath);
        try (InputStream keyStoreFile = Files.newInputStream(new File(keystorePath).toPath())) {
            keyStore.load(keyStoreFile, keystorePassword.toCharArray());
        }
        final KeyManagerFactory kmf =
                KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
        LOG.info("Initializing key manager with keystore and password");
        kmf.init(keyStore, keystorePassword.toCharArray());

        sslContext =
                SslContextBuilder.forServer(kmf)
                        .ciphers(Http2SecurityUtil.CIPHERS, SupportedCipherSuiteFilter.INSTANCE)
                        .build();
    }
}
