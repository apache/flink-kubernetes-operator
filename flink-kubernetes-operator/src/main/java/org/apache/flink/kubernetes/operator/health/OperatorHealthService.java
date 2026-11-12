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

package org.apache.flink.kubernetes.operator.health;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.kubernetes.operator.config.KubernetesOperatorConfigOptions;
import org.apache.flink.util.concurrent.ExecutorThreadFactory;

import org.apache.flink.shaded.netty4.io.netty.bootstrap.ServerBootstrap;
import org.apache.flink.shaded.netty4.io.netty.channel.Channel;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelInitializer;
import org.apache.flink.shaded.netty4.io.netty.channel.EventLoopGroup;
import org.apache.flink.shaded.netty4.io.netty.channel.nio.NioEventLoopGroup;
import org.apache.flink.shaded.netty4.io.netty.channel.socket.SocketChannel;
import org.apache.flink.shaded.netty4.io.netty.channel.socket.nio.NioServerSocketChannel;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpObjectAggregator;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpServerCodec;

import io.javaoperatorsdk.operator.RuntimeInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;

/**
 * Exposes the operator health to the Kubernetes liveness probe by serving the {@code
 * OperatorHealthProbe} result over HTTP on a single Netty thread. The service is started only once
 * the operator is running, since the probe takes note of the informers that are unhealthy at that
 * moment, and it is closed together with the operator.
 */
public class OperatorHealthService implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(OperatorHealthService.class);
    private static final int MAX_REQUEST_LENGTH = 65536;

    private final boolean enabled;
    private final int port;

    private EventLoopGroup eventLoopGroup;
    private Channel serverChannel;

    public OperatorHealthService(Configuration config) {
        this.enabled = config.get(KubernetesOperatorConfigOptions.OPERATOR_HEALTH_PROBE_ENABLED);
        this.port = config.get(KubernetesOperatorConfigOptions.OPERATOR_HEALTH_PROBE_PORT);
    }

    public void start(
            RuntimeInfo runtimeInfo, Collection<CanaryResourceManager<?>> canaryResourceManagers) {
        if (!enabled) {
            LOG.info("Health probe disabled");
            return;
        }

        LOG.info("Health probe HTTP endpoint starting...");
        var handler =
                new OperatorHealthHandler(
                        new OperatorHealthProbe(runtimeInfo, canaryResourceManagers));
        eventLoopGroup =
                new NioEventLoopGroup(1, new ExecutorThreadFactory("flink-operator-health-probe"));
        var bootstrap =
                new ServerBootstrap()
                        .group(eventLoopGroup)
                        .channel(NioServerSocketChannel.class)
                        .childHandler(
                                new ChannelInitializer<SocketChannel>() {
                                    @Override
                                    protected void initChannel(SocketChannel ch) {
                                        ch.pipeline()
                                                .addLast(new HttpServerCodec())
                                                .addLast(
                                                        new HttpObjectAggregator(
                                                                MAX_REQUEST_LENGTH))
                                                .addLast(handler);
                                    }
                                });
        try {
            serverChannel = bootstrap.bind(port).sync().channel();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        LOG.info("Health probe HTTP endpoint started on port {}", port);
    }

    @Override
    public void close() {
        if (serverChannel != null) {
            serverChannel.close().awaitUninterruptibly();
            serverChannel = null;
        }
        if (eventLoopGroup != null) {
            eventLoopGroup.shutdownGracefully();
            eventLoopGroup = null;
        }
    }
}
