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

import org.apache.flink.util.ExceptionUtils;

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

import java.nio.charset.StandardCharsets;

/**
 * Simple code which returns HTTP 200 messages if the service is live, and HTTP 500 messages if the
 * service is down. The response is based on the {@link HealthProbe#isHealthy()} result.
 */
@ChannelHandler.Sharable
public class OperatorHealthHandler extends SimpleChannelInboundHandler<HttpObject> {

    private final HealthProbe probe = HealthProbe.INSTANCE;

    @Override
    public void channelRead0(ChannelHandlerContext ctx, HttpObject msg) {
        try {
            if (probe.isHealthy()) {
                ctx.writeAndFlush(createResponse("OK", HttpResponseStatus.OK));
            } else {
                ctx.writeAndFlush(
                        createResponse("ERROR", HttpResponseStatus.INTERNAL_SERVER_ERROR));
            }
        } catch (Throwable t) {
            if (ctx.channel().isActive()) {
                ctx.writeAndFlush(
                        createResponse(
                                ExceptionUtils.stringifyException(t),
                                HttpResponseStatus.INTERNAL_SERVER_ERROR));
            }
        }
    }

    private DefaultFullHttpResponse createResponse(String content, HttpResponseStatus status) {
        ByteBuf buff = Unpooled.copiedBuffer(content, StandardCharsets.UTF_8);
        DefaultFullHttpResponse response =
                new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, status, buff);

        response.headers().set(HttpHeaderNames.CONTENT_TYPE, "text/plain;charset=UTF-8");
        response.headers().set(HttpHeaderNames.CONTENT_LENGTH, buff.readableBytes());
        return response;
    }
}
