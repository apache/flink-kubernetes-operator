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

import org.apache.flink.kubernetes.operator.admission.informer.InformerManager;
import org.apache.flink.kubernetes.operator.admission.mutator.FlinkMutator;
import org.apache.flink.kubernetes.operator.api.CrdConstants;
import org.apache.flink.kubernetes.operator.api.FlinkDeployment;
import org.apache.flink.kubernetes.operator.api.FlinkSessionJob;
import org.apache.flink.kubernetes.operator.api.spec.FlinkDeploymentSpec;
import org.apache.flink.kubernetes.operator.api.spec.FlinkSessionJobSpec;
import org.apache.flink.kubernetes.operator.api.spec.JobSpec;
import org.apache.flink.kubernetes.operator.config.FlinkConfigManager;
import org.apache.flink.kubernetes.operator.utils.MutatorUtils;
import org.apache.flink.kubernetes.operator.utils.ValidatorUtils;

import org.apache.flink.shaded.netty4.io.netty.buffer.Unpooled;
import org.apache.flink.shaded.netty4.io.netty.buffer.UnpooledHeapByteBuf;
import org.apache.flink.shaded.netty4.io.netty.channel.embedded.EmbeddedChannel;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.DefaultFullHttpRequest;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.DefaultFullHttpResponse;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.DefaultHttpResponse;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.exc.MismatchedInputException;
import io.fabric8.kubernetes.api.model.GroupVersionKind;
import io.fabric8.kubernetes.api.model.ObjectMeta;
import io.fabric8.kubernetes.api.model.admission.v1.AdmissionRequest;
import io.fabric8.kubernetes.api.model.admission.v1.AdmissionReview;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.server.mock.EnableKubernetesMockClient;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Base64;

import static io.javaoperatorsdk.operator.api.reconciler.Constants.DEFAULT_NAMESPACES_SET;
import static io.javaoperatorsdk.webhook.admission.Operation.CREATE;
import static org.apache.flink.kubernetes.operator.admission.AdmissionHandler.MUTATOR_REQUEST_PATH;
import static org.apache.flink.kubernetes.operator.admission.AdmissionHandler.VALIDATE_REQUEST_PATH;
import static org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpMethod.GET;
import static org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpResponseStatus.INTERNAL_SERVER_ERROR;
import static org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpResponseStatus.OK;
import static org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpVersion.HTTP_1_1;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * @link AdmissionHandler unit tests
 */
@EnableKubernetesMockClient(crud = true)
public class AdmissionHandlerTest {

    private KubernetesClient kubernetesClient;
    private AdmissionHandler admissionHandler =
            new AdmissionHandler(
                    new FlinkValidator(
                            ValidatorUtils.discoverValidators(
                                    new FlinkConfigManager(ns -> {}, true)),
                            new InformerManager(null)),
                    new FlinkMutator(
                            MutatorUtils.discoverMutators(new FlinkConfigManager(ns -> {}, true)),
                            new InformerManager(kubernetesClient)));

    @Test
    public void testHandleIllegalRequest() {
        final EmbeddedChannel embeddedChannel = new EmbeddedChannel(admissionHandler);
        final String illegalRequest = "/test";
        embeddedChannel.writeInbound(new DefaultFullHttpRequest(HTTP_1_1, GET, illegalRequest));
        embeddedChannel.writeOutbound(new DefaultFullHttpResponse(HTTP_1_1, OK));
        final DefaultFullHttpResponse response = embeddedChannel.readOutbound();
        assertEquals(INTERNAL_SERVER_ERROR, response.status());
        assertEquals(
                String.format(
                        "Illegal path requested: %s. Only %s or %s is accepted.",
                        illegalRequest, VALIDATE_REQUEST_PATH, MUTATOR_REQUEST_PATH),
                new String(response.content().array()));
        assertTrue(embeddedChannel.finish());
    }

    @Test
    public void testHandleValidateRequestWithoutContent() {
        final EmbeddedChannel embeddedChannel = new EmbeddedChannel(admissionHandler);
        embeddedChannel.writeInbound(
                new DefaultFullHttpRequest(HTTP_1_1, GET, VALIDATE_REQUEST_PATH));
        embeddedChannel.writeOutbound(new DefaultFullHttpResponse(HTTP_1_1, OK));
        final DefaultFullHttpResponse response = embeddedChannel.readOutbound();
        assertEquals(INTERNAL_SERVER_ERROR, response.status());
        assertTrue(
                new String(response.content().array())
                        .contains(MismatchedInputException.class.getName()));
        assertTrue(embeddedChannel.finish());
    }

    @Test
    public void testHandleValidateRequestWithAdmissionReview() throws IOException {
        final EmbeddedChannel embeddedChannel = new EmbeddedChannel(admissionHandler);
        final FlinkDeployment flinkDeployment = new FlinkDeployment();
        ObjectMeta objectMeta = new ObjectMeta();
        objectMeta.setName("basic-session-cluster");
        flinkDeployment.setMetadata(objectMeta);
        flinkDeployment.setSpec(new FlinkDeploymentSpec());
        final AdmissionRequest admissionRequest = new AdmissionRequest();
        admissionRequest.setOperation(CREATE.name());
        admissionRequest.setObject(flinkDeployment);
        admissionRequest.setKind(
                new GroupVersionKind(
                        flinkDeployment.getGroup(),
                        flinkDeployment.getVersion(),
                        flinkDeployment.getKind()));
        final AdmissionReview admissionReview = new AdmissionReview();
        admissionReview.setRequest(admissionRequest);
        embeddedChannel.writeInbound(
                new DefaultFullHttpRequest(
                        HTTP_1_1,
                        GET,
                        VALIDATE_REQUEST_PATH,
                        Unpooled.wrappedBuffer(
                                new ObjectMapper()
                                        .writeValueAsString(admissionReview)
                                        .getBytes())));
        embeddedChannel.writeOutbound(new DefaultFullHttpResponse(HTTP_1_1, OK));
        final DefaultHttpResponse response = embeddedChannel.readOutbound();
        assertEquals(OK, response.status());
        assertTrue(embeddedChannel.finish());
    }

    @Test
    public void testMutateHandler() throws Exception {

        var informerManager = new InformerManager(kubernetesClient);
        informerManager.setNamespaces(DEFAULT_NAMESPACES_SET);
        admissionHandler =
                new AdmissionHandler(
                        new FlinkValidator(
                                ValidatorUtils.discoverValidators(
                                        new FlinkConfigManager(ns -> {}, true)),
                                new InformerManager(null)),
                        new FlinkMutator(
                                MutatorUtils.discoverMutators(
                                        new FlinkConfigManager(ns -> {}, true)),
                                informerManager));

        final EmbeddedChannel embeddedChannel = new EmbeddedChannel(admissionHandler);
        var sessionJob = new FlinkSessionJob();
        ObjectMeta objectMeta = new ObjectMeta();
        objectMeta.setName("basic-session-cluster");
        objectMeta.setNamespace("testNamespace");
        sessionJob.setMetadata(objectMeta);
        sessionJob.setSpec(
                FlinkSessionJobSpec.builder()
                        .job(JobSpec.builder().jarURI("http://myjob.jar").build())
                        .deploymentName("test-session")
                        .build());

        final AdmissionRequest admissionRequest = new AdmissionRequest();
        admissionRequest.setOperation(CREATE.name());
        admissionRequest.setObject(sessionJob);
        admissionRequest.setKind(
                new GroupVersionKind(
                        sessionJob.getGroup(), sessionJob.getVersion(), sessionJob.getKind()));
        final AdmissionReview admissionReview = new AdmissionReview();
        admissionReview.setRequest(admissionRequest);
        embeddedChannel.writeInbound(
                new DefaultFullHttpRequest(
                        HTTP_1_1,
                        GET,
                        MUTATOR_REQUEST_PATH,
                        Unpooled.wrappedBuffer(
                                new ObjectMapper()
                                        .writeValueAsString(admissionReview)
                                        .getBytes())));
        embeddedChannel.writeOutbound(new DefaultFullHttpResponse(HTTP_1_1, OK));
        final DefaultHttpResponse response = embeddedChannel.readOutbound();
        assertEquals(OK, response.status());
        Assertions.assertFalse(embeddedChannel.outboundMessages().isEmpty());
        var body = embeddedChannel.readOutbound();
        Assertions.assertNotNull(body);
        var str = new String(((UnpooledHeapByteBuf) body).array());
        var review = new ObjectMapper().readValue(str, AdmissionReview.class);
        var patch = new String(Base64.getDecoder().decode(review.getResponse().getPatch()));
        Assertions.assertTrue(patch.contains(CrdConstants.LABEL_TARGET_SESSION));
        assertTrue(embeddedChannel.finish());
    }

    @Test
    public void testmutateHandlerFlinkDeployment() throws Exception {

        final EmbeddedChannel embeddedChannel = new EmbeddedChannel(admissionHandler);
        final FlinkDeployment flinkDeployment = new FlinkDeployment();
        ObjectMeta objectMeta = new ObjectMeta();
        objectMeta.setName("basic-session-cluster");
        flinkDeployment.setMetadata(objectMeta);
        flinkDeployment.setSpec(new FlinkDeploymentSpec());

        final AdmissionRequest admissionRequest = new AdmissionRequest();
        admissionRequest.setOperation(CREATE.name());
        admissionRequest.setObject(flinkDeployment);
        admissionRequest.setKind(
                new GroupVersionKind(
                        flinkDeployment.getGroup(),
                        flinkDeployment.getVersion(),
                        flinkDeployment.getKind()));
        final AdmissionReview admissionReview = new AdmissionReview();
        admissionReview.setRequest(admissionRequest);
        embeddedChannel.writeInbound(
                new DefaultFullHttpRequest(
                        HTTP_1_1,
                        GET,
                        MUTATOR_REQUEST_PATH,
                        Unpooled.wrappedBuffer(
                                new ObjectMapper()
                                        .writeValueAsString(admissionReview)
                                        .getBytes())));
        embeddedChannel.writeOutbound(new DefaultFullHttpResponse(HTTP_1_1, OK));
        final DefaultHttpResponse response = embeddedChannel.readOutbound();
        assertEquals(OK, response.status());
        Assertions.assertFalse(embeddedChannel.outboundMessages().isEmpty());
        var body = embeddedChannel.readOutbound();
        Assertions.assertNotNull(body);
        assertTrue(embeddedChannel.finish());
    }
}
