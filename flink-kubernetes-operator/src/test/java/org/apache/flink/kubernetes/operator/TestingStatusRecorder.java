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
import org.apache.flink.kubernetes.operator.crd.AbstractFlinkResource;
import org.apache.flink.kubernetes.operator.crd.status.CommonStatus;
import org.apache.flink.kubernetes.operator.utils.StatusRecorder;

import com.fasterxml.jackson.databind.node.ObjectNode;

/** Testing statusRecorder. */
public class TestingStatusRecorder<STATUS extends CommonStatus<?>> extends StatusRecorder<STATUS> {

    public TestingStatusRecorder() {
        super(null, TestUtils.createTestMetricManager(new Configuration()), (r, s) -> {});
    }

    @Override
    public <T extends AbstractFlinkResource<?, STATUS>> void patchAndCacheStatus(T resource) {
        statusCache.put(
                getKey(resource),
                objectMapper.convertValue(resource.getStatus(), ObjectNode.class));
    }
}
