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

package org.apache.flink.kubernetes.operator.utils;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.fabric8.kubernetes.api.model.Container;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.PodSpec;
import org.junit.Assert;
import org.junit.jupiter.api.Test;

import java.util.Arrays;

/** FlinkUtilsTest. */
public class FlinkUtilsTest {

    @Test
    public void testMergePods() throws Exception {

        Container container1 = new Container();
        container1.setName("container1");
        Container container2 = new Container();
        container2.setName("container2");

        PodSpec podSpec1 = new PodSpec();
        podSpec1.setHostname("pod1 hostname");
        podSpec1.setContainers(Arrays.asList(container2));
        Pod pod1 = new Pod();
        pod1.setApiVersion("pod1 api version");
        pod1.setSpec(podSpec1);

        PodSpec podSpec2 = new PodSpec();
        podSpec2.setHostname("pod2 hostname");
        podSpec2.setContainers(Arrays.asList(container1, container2));
        Pod pod2 = new Pod();
        pod2.setApiVersion("pod2 api version");
        pod2.setSpec(podSpec2);

        ObjectMapper mapper = new ObjectMapper();
        JsonNode node1 = mapper.valueToTree(pod1);
        JsonNode node2 = mapper.valueToTree(pod2);

        Pod mergedPod = FlinkUtils.mergePodTemplates(pod1, pod2);

        Assert.assertEquals(pod2.getApiVersion(), mergedPod.getApiVersion());
        Assert.assertEquals(pod2.getSpec().getContainers(), mergedPod.getSpec().getContainers());
    }
}
