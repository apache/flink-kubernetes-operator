/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.kubernetes.operator;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.rest.FileUpload;
import org.apache.flink.runtime.rest.RestClient;
import org.apache.flink.runtime.rest.messages.EmptyResponseBody;
import org.apache.flink.runtime.rest.messages.MessageHeaders;
import org.apache.flink.runtime.rest.messages.MessageParameters;
import org.apache.flink.runtime.rest.messages.RequestBody;
import org.apache.flink.runtime.rest.messages.ResponseBody;
import org.apache.flink.runtime.rest.versioning.RestAPIVersion;
import org.apache.flink.util.ConfigurationException;
import org.apache.flink.util.concurrent.Executors;
import org.apache.flink.util.function.TriFunction;

import lombok.Setter;

import java.util.Collection;
import java.util.concurrent.CompletableFuture;

/** Testing RestClient implementation. */
public class TestingRestClient extends RestClient {

    @Setter
    private TriFunction<
                    MessageHeaders<?, ?, ?>,
                    MessageParameters,
                    RequestBody,
                    CompletableFuture<ResponseBody>>
            requestProcessor =
                    (ignore1, ignore2, ignore) ->
                            CompletableFuture.completedFuture(EmptyResponseBody.getInstance());

    public TestingRestClient(Configuration conf) throws ConfigurationException {
        super(conf, Executors.directExecutor());
    }

    @Override
    public <
                    M extends MessageHeaders<R, P, U>,
                    U extends MessageParameters,
                    R extends RequestBody,
                    P extends ResponseBody>
            CompletableFuture<P> sendRequest(
                    String targetAddress,
                    int targetPort,
                    M messageHeaders,
                    U messageParameters,
                    R request,
                    Collection<FileUpload> fileUploads,
                    RestAPIVersion<? extends RestAPIVersion<?>> apiVersion) {
        return (CompletableFuture<P>)
                requestProcessor.apply(messageHeaders, messageParameters, request);
    }
}
