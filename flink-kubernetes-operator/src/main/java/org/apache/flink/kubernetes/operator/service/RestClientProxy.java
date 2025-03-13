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

package org.apache.flink.kubernetes.operator.service;

import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.rest.RestClient;
import org.apache.flink.util.ConfigurationException;
import org.apache.flink.util.Preconditions;

import org.apache.flink.shaded.netty4.io.netty.bootstrap.AbstractBootstrap;
import org.apache.flink.shaded.netty4.io.netty.bootstrap.Bootstrap;
import org.apache.flink.shaded.netty4.io.netty.channel.EventLoopGroup;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Field;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;

/** Proxy for the {@link RestClient}. */
public class RestClientProxy extends RestClient {
    private static final Logger LOG = LoggerFactory.getLogger(RestClientProxy.class);

    private final boolean useSharedEventLoopGroup;
    private Field groupField;
    private Bootstrap bootstrap;
    private CompletableFuture<Void> terminationFuture;

    public RestClientProxy(
            Configuration configuration,
            ExecutorService executor,
            String host,
            int port,
            EventLoopGroup sharedGroup)
            throws ConfigurationException, NoSuchFieldException, IllegalAccessException {
        super(configuration, executor, host, port);

        if (sharedGroup != null) {
            Preconditions.checkArgument(
                    !sharedGroup.isShuttingDown() && !sharedGroup.isShutdown(),
                    "provided eventLoopGroup is shut/shutting down");

            // get private field
            Field bootstrapField = RestClient.class.getDeclaredField("bootstrap");
            Field terminationFutureField = RestClient.class.getDeclaredField("terminationFuture");
            Field groupField = AbstractBootstrap.class.getDeclaredField("group");

            bootstrapField.setAccessible(true);
            terminationFutureField.setAccessible(true);
            groupField.setAccessible(true);

            this.terminationFuture = (CompletableFuture<Void>) terminationFutureField.get(this);
            this.bootstrap = (Bootstrap) bootstrapField.get(this);
            this.groupField = groupField;

            // close previous group
            bootstrap.config().group().shutdown();
            // setup share group
            groupField.set(bootstrap, sharedGroup);

            useSharedEventLoopGroup = true;
        } else {
            useSharedEventLoopGroup = false;
        }
    }

    @Override
    public CompletableFuture<Void> closeAsync() {
        if (useSharedEventLoopGroup) {
            this.shutdownInternal();
        }

        return super.closeAsync();
    }

    @Override
    public void shutdown(Time timeout) {
        if (useSharedEventLoopGroup) {
            this.shutdownInternal();
        }
        super.shutdown(timeout);
    }

    private void shutdownInternal() {
        try {
            // replace bootstrap's group to null to avoid shutdown shared group
            groupField.set(bootstrap, null);
            terminationFuture.complete(null);
        } catch (IllegalAccessException e) {
            LOG.error("Failed to setup rest client event group .", e);
        }
    }
}
