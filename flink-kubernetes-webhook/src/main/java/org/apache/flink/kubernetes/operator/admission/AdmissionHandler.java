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

import org.apache.flink.kubernetes.operator.admission.mutator.DefaultRequestMutator;

import org.apache.flink.shaded.netty4.io.netty.buffer.ByteBuf;
import org.apache.flink.shaded.netty4.io.netty.buffer.ByteBufInputStream;
import org.apache.flink.shaded.netty4.io.netty.buffer.Unpooled;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelFuture;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelHandler;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelHandlerContext;
import org.apache.flink.shaded.netty4.io.netty.channel.SimpleChannelInboundHandler;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.DefaultFullHttpResponse;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.DefaultHttpResponse;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.FullHttpRequest;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpHeaderValues;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpRequest;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpResponse;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpResponseStatus;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpUtil;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpVersion;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.LastHttpContent;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.QueryStringDecoder;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.admission.v1.AdmissionReview;
import io.javaoperatorsdk.webhook.admission.AdmissionController;
import io.javaoperatorsdk.webhook.admission.mutation.Mutator;
import io.javaoperatorsdk.webhook.admission.validation.Validator;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;

import java.io.InputStream;
import java.nio.charset.Charset;
import java.util.concurrent.CompletableFuture;

import static org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpHeaderNames.CONTENT_LENGTH;
import static org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpHeaderNames.CONTENT_TYPE;
import static org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpHeaderValues.APPLICATION_JSON;
import static org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpVersion.HTTP_1_1;

/** Rest endpoint for validation requests. */
@ChannelHandler.Sharable
public class AdmissionHandler extends SimpleChannelInboundHandler<HttpRequest> {
    private static final Logger LOG = LoggerFactory.getLogger(AdmissionHandler.class);
    private static final ObjectMapper objectMapper = new ObjectMapper();
    protected static final String VALIDATE_REQUEST_PATH = "/validate";
    protected static final String MUTATOR_REQUEST_PATH = "/mutate";

    private final AdmissionController<HasMetadata> validatingController;
    private final AdmissionController<HasMetadata> mutatorController;

    public AdmissionHandler(Validator<HasMetadata> validator, Mutator<HasMetadata> mutator) {
        this.validatingController = new AdmissionController<>(validator);
        this.mutatorController = new AdmissionController<>(new DefaultRequestMutator<>(mutator));
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, HttpRequest httpRequest) {
        QueryStringDecoder decoder = new QueryStringDecoder(httpRequest.uri());
        String path = decoder.path();
        if (VALIDATE_REQUEST_PATH.equals(path)) {
            final ByteBuf msgContent = ((FullHttpRequest) httpRequest).content();
            AdmissionReview review;
            try {
                InputStream in = new ByteBufInputStream(msgContent);
                review = objectMapper.readValue(in, AdmissionReview.class);
                AdmissionReview response = validatingController.handle(review);
                sendResponse(ctx, objectMapper.writeValueAsString(response));
            } catch (Exception e) {
                LOG.error("Failed to validate", e);
                sendError(ctx, ExceptionUtils.getStackTrace(e));
            }
        } else if (MUTATOR_REQUEST_PATH.equals(path)) {
            final ByteBuf msgContent = ((FullHttpRequest) httpRequest).content();
            AdmissionReview review;
            try {
                InputStream in = new ByteBufInputStream(msgContent);
                review = objectMapper.readValue(in, AdmissionReview.class);
                AdmissionReview response = mutatorController.handle(review);
                sendResponse(ctx, objectMapper.writeValueAsString(response));
            } catch (Exception e) {
                LOG.error("Failed to mutate", e);
                sendError(ctx, ExceptionUtils.getStackTrace(e));
            }
        } else {
            String error =
                    String.format(
                            "Illegal path requested: %s. Only %s or %s is accepted.",
                            path, VALIDATE_REQUEST_PATH, MUTATOR_REQUEST_PATH);
            LOG.error(error);
            sendError(ctx, error);
        }
    }

    private void sendError(ChannelHandlerContext ctx, String error) {
        if (ctx.channel().isActive()) {
            DefaultFullHttpResponse response =
                    new DefaultFullHttpResponse(
                            HttpVersion.HTTP_1_1,
                            HttpResponseStatus.INTERNAL_SERVER_ERROR,
                            Unpooled.wrappedBuffer(error.getBytes(Charset.defaultCharset())));

            response.headers().set(CONTENT_TYPE, HttpHeaderValues.TEXT_PLAIN);
            response.headers().set(CONTENT_LENGTH, response.content().readableBytes());

            ctx.writeAndFlush(response);
        }
    }

    public static void sendResponse(
            @Nonnull ChannelHandlerContext channelHandlerContext, @Nonnull String json) {
        HttpResponse response = new DefaultHttpResponse(HTTP_1_1, HttpResponseStatus.OK);

        response.headers().set(CONTENT_TYPE, APPLICATION_JSON);

        byte[] buf = json.getBytes(Charset.defaultCharset());
        ByteBuf b = Unpooled.copiedBuffer(buf);
        HttpUtil.setContentLength(response, buf.length);

        // write the initial line and the header.
        channelHandlerContext.write(response);
        channelHandlerContext.write(b);
        ChannelFuture channelFuture =
                channelHandlerContext.writeAndFlush(LastHttpContent.EMPTY_LAST_CONTENT);

        final CompletableFuture<Void> completableFuture = new CompletableFuture<>();
        channelFuture.addListener(
                future -> {
                    if (future.isSuccess()) {
                        completableFuture.complete(null);
                    } else {
                        completableFuture.completeExceptionally(future.cause());
                    }
                });
    }
}
