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

import org.apache.flink.shaded.netty4.io.netty.buffer.ByteBuf;
import org.apache.flink.shaded.netty4.io.netty.buffer.Unpooled;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelHandler;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelHandlerContext;
import org.apache.flink.shaded.netty4.io.netty.channel.SimpleChannelInboundHandler;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.DefaultFullHttpResponse;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpHeaderNames;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpObject;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpResponseStatus;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpVersion;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;

/**
 * Answers every HTTP request with the current probe result. A healthy operator yields HTTP 200 with
 * the body {@code OK}, an unhealthy one HTTP 500 with the body {@code ERROR}. When the probe itself
 * fails, the failure is logged and the caller receives the same {@code ERROR} response.
 */
@ChannelHandler.Sharable
class OperatorHealthHandler extends SimpleChannelInboundHandler<HttpObject> {

    private static final Logger LOG = LoggerFactory.getLogger(OperatorHealthHandler.class);

    private final OperatorHealthProbe probe;

    OperatorHealthHandler(OperatorHealthProbe probe) {
        this.probe = probe;
    }

    @Override
    public void channelRead0(ChannelHandlerContext ctx, HttpObject msg) {
        boolean healthy;
        try {
            healthy = probe.isHealthy();
        } catch (Throwable t) {
            LOG.error("Health probe failed", t);
            healthy = false;
        }
        ctx.writeAndFlush(
                healthy
                        ? createResponse("OK", HttpResponseStatus.OK)
                        : createResponse("ERROR", HttpResponseStatus.INTERNAL_SERVER_ERROR));
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        LOG.warn("Health probe request failed: {}", cause.toString());
        ctx.close();
    }

    private static DefaultFullHttpResponse createResponse(
            String content, HttpResponseStatus status) {
        ByteBuf buff = Unpooled.copiedBuffer(content, StandardCharsets.UTF_8);
        DefaultFullHttpResponse response =
                new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, status, buff);
        response.headers().set(HttpHeaderNames.CONTENT_TYPE, "text/plain;charset=UTF-8");
        response.headers().set(HttpHeaderNames.CONTENT_LENGTH, buff.readableBytes());
        return response;
    }
}
