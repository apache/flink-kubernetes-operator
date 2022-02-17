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

import org.apache.flink.kubernetes.operator.TestUtils;

import io.fabric8.kubernetes.api.model.Container;
import io.fabric8.kubernetes.api.model.Pod;
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

        Pod pod1 =
                TestUtils.getTestPod(
                        "pod1 hostname", "pod1 api version", Arrays.asList(container2));

        Pod pod2 =
                TestUtils.getTestPod(
                        "pod2 hostname", "pod2 api version", Arrays.asList(container1, container2));

        Pod mergedPod = FlinkUtils.mergePodTemplates(pod1, pod2);

        Assert.assertEquals(pod2.getApiVersion(), mergedPod.getApiVersion());
        Assert.assertEquals(pod2.getSpec().getContainers(), mergedPod.getSpec().getContainers());
    }
}
