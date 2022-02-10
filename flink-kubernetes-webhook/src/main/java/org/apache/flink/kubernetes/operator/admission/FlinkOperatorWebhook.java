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

package org.apache.flink.kubernetes.operator.admission;

import org.apache.flink.shaded.netty4.io.netty.bootstrap.ServerBootstrap;
import org.apache.flink.shaded.netty4.io.netty.channel.Channel;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelInitializer;
import org.apache.flink.shaded.netty4.io.netty.channel.nio.NioEventLoopGroup;
import org.apache.flink.shaded.netty4.io.netty.channel.socket.SocketChannel;
import org.apache.flink.shaded.netty4.io.netty.channel.socket.nio.NioServerSocketChannel;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpObjectAggregator;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpServerCodec;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http2.Http2SecurityUtil;
import org.apache.flink.shaded.netty4.io.netty.handler.ssl.SslContext;
import org.apache.flink.shaded.netty4.io.netty.handler.ssl.SslContextBuilder;
import org.apache.flink.shaded.netty4.io.netty.handler.ssl.SupportedCipherSuiteFilter;
import org.apache.flink.shaded.netty4.io.netty.handler.stream.ChunkedWriteHandler;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.KeyManagerFactory;

import java.io.File;
import java.io.InputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.security.KeyStore;

/** Main Class for Flink native k8s operator. */
public class FlinkOperatorWebhook {
    private static final Logger LOG = LoggerFactory.getLogger(FlinkOperatorWebhook.class);

    private static final int MAX_CONTEXT_LENGTH = 104_857_600;

    public static final String WEBHOOK_KEYSTORE_FILE = "WEBHOOK_KEYSTORE_FILE";
    public static final String WEBHOOK_KEYSTORE_PASSWORD = "WEBHOOK_KEYSTORE_PASSWORD";
    public static final String WEBHOOK_KEYSTORE_TYPE = "WEBHOOK_KEYSTORE_TYPE";
    public static final String WEBHOOK_SERVER_PORT = "WEBHOOK_SERVER_PORT";

    public static void main(String[] args) throws Exception {
        LOG.info("Starting Flink Kubernetes Webhook");
        AdmissionHandler endpoint = new AdmissionHandler(new FlinkDeploymentValidator());
        ChannelInitializer<SocketChannel> initializer = createChannelInitializer(endpoint);
        NioEventLoopGroup bossGroup = new NioEventLoopGroup(1);
        NioEventLoopGroup workerGroup = new NioEventLoopGroup();
        try {
            ServerBootstrap bootstrap = new ServerBootstrap();
            bootstrap
                    .group(bossGroup, workerGroup)
                    .channel(NioServerSocketChannel.class)
                    .childHandler(initializer);

            Channel serverChannel = bootstrap.bind(getPort()).sync().channel();

            InetSocketAddress bindAddress = (InetSocketAddress) serverChannel.localAddress();
            InetAddress inetAddress = bindAddress.getAddress();
            LOG.info(
                    "Webhook listening at {}" + ':' + "{}",
                    inetAddress.getHostAddress(),
                    bindAddress.getPort());

            serverChannel.closeFuture().sync();
        } finally {
            bossGroup.shutdownGracefully();
            workerGroup.shutdownGracefully();
        }
    }

    private static int getPort() {
        String portString = System.getenv(WEBHOOK_SERVER_PORT);
        if (StringUtils.isEmpty(portString)) {
            throw new RuntimeException(WEBHOOK_SERVER_PORT + " cannot be empty");
        }
        return Integer.parseInt(portString);
    }

    private static ChannelInitializer<SocketChannel> createChannelInitializer(
            AdmissionHandler admissionHandler) throws Exception {
        SslContext sslContext = createSslContext();

        return new ChannelInitializer<SocketChannel>() {

            @Override
            protected void initChannel(SocketChannel ch) {

                if (sslContext != null) {
                    ch.pipeline().addLast(sslContext.newHandler(ch.alloc()));
                }

                ch.pipeline()
                        .addLast(new HttpServerCodec())
                        .addLast(new HttpObjectAggregator(MAX_CONTEXT_LENGTH));

                ch.pipeline()
                        .addLast(new ChunkedWriteHandler())
                        .addLast(admissionHandler.getClass().getName(), admissionHandler);
            }
        };
    }

    private static SslContext createSslContext() throws Exception {
        String keystorePath = System.getenv(WEBHOOK_KEYSTORE_FILE);
        String keystorePassword = System.getenv(WEBHOOK_KEYSTORE_PASSWORD);
        String keystoreType = System.getenv(WEBHOOK_KEYSTORE_TYPE);

        if (StringUtils.isEmpty(keystorePath)) {
            LOG.info(
                    "No keystore path is defined in "
                            + WEBHOOK_KEYSTORE_FILE
                            + ", running without ssl");
            return null;
        }

        if (StringUtils.isAnyEmpty(keystorePassword, keystoreType)) {
            throw new RuntimeException(
                    WEBHOOK_KEYSTORE_PASSWORD
                            + " and "
                            + WEBHOOK_KEYSTORE_TYPE
                            + " must no be empty");
        }

        KeyStore keyStore = KeyStore.getInstance(keystoreType);
        try (InputStream keyStoreFile = Files.newInputStream(new File(keystorePath).toPath())) {
            keyStore.load(keyStoreFile, keystorePassword.toCharArray());
        }
        final KeyManagerFactory kmf =
                KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
        kmf.init(keyStore, keystorePassword.toCharArray());

        return SslContextBuilder.forServer(kmf)
                .ciphers(Http2SecurityUtil.CIPHERS, SupportedCipherSuiteFilter.INSTANCE)
                .build();
    }
}
