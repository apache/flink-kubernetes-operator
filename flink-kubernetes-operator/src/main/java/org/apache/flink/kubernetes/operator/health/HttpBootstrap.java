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

import org.apache.flink.shaded.netty4.io.netty.bootstrap.ServerBootstrap;
import org.apache.flink.shaded.netty4.io.netty.channel.Channel;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelInitializer;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelPipeline;
import org.apache.flink.shaded.netty4.io.netty.channel.nio.NioEventLoopGroup;
import org.apache.flink.shaded.netty4.io.netty.channel.socket.SocketChannel;
import org.apache.flink.shaded.netty4.io.netty.channel.socket.nio.NioServerSocketChannel;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpObjectAggregator;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpServerCodec;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Class for serving HTTP requests for the health probe. */
public class HttpBootstrap {
    private static final Logger LOG = LoggerFactory.getLogger(OperatorHealthService.class);
    private static final int MAX_REQUEST_LENGTH = 65536;

    private final ServerBootstrap bootstrap;
    private final Channel serverChannel;

    public HttpBootstrap(HealthProbe probe, int port) throws InterruptedException {
        LOG.info("Health probe HTTP endpoint starting...");
        ChannelInitializer<SocketChannel> initializer =
                new ChannelInitializer<>() {
                    @Override
                    protected void initChannel(SocketChannel ch) {
                        ChannelPipeline pipeline = ch.pipeline();
                        pipeline.addLast(new HttpServerCodec());
                        pipeline.addLast(new HttpObjectAggregator(MAX_REQUEST_LENGTH));
                        pipeline.addLast(new OperatorHealthHandler());
                    }
                };

        NioEventLoopGroup bossGroup = new NioEventLoopGroup(1);
        NioEventLoopGroup workerGroup = new NioEventLoopGroup();

        this.bootstrap = new ServerBootstrap();

        bootstrap
                .group(bossGroup, workerGroup)
                .channel(NioServerSocketChannel.class)
                .childHandler(initializer);

        this.serverChannel = bootstrap.bind(port).sync().channel();
        LOG.info("Health probe HTTP endpoint started on port {}", port);
    }

    public void stop() {
        if (this.serverChannel != null) {
            this.serverChannel.close().awaitUninterruptibly();
        }
        if (bootstrap != null) {
            if (bootstrap.group() != null) {
                bootstrap.group().shutdownGracefully();
            }
            if (bootstrap.childGroup() != null) {
                bootstrap.childGroup().shutdownGracefully();
            }
        }
    }
}
