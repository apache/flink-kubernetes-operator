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

package org.apache.flink.autoscaler;

import org.apache.flink.api.common.JobID;
import org.apache.flink.autoscaler.state.AutoScalerStateStore;
import org.apache.flink.autoscaler.state.AutoScalerStateStoreFactory;
import org.apache.flink.client.program.rest.RestClusterClient;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.util.function.SupplierWithException;

import lombok.AllArgsConstructor;
import lombok.Getter;

import javax.annotation.Nullable;

import java.time.Duration;
import java.util.Optional;

/**
 * The job autoscaler context.
 *
 * @param <KEY>
 * @param <INFO>
 */
@AllArgsConstructor
public class JobAutoScalerContext<KEY, INFO> {

    // The identifier of each flink job.
    @Getter private final KEY jobKey;

    @Getter private final JobID jobID;

    // Whether the job is really running, the STARTING or CANCELING aren't running.
    @Getter private final boolean isRunning;

    @Getter private final Configuration conf;

    @Getter private final MetricGroup metricGroup;

    private final SupplierWithException<RestClusterClient<String>, Exception> restClientSupplier;

    @Getter private final Duration flinkClientTimeout;

    private final AutoScalerStateStoreFactory stateStoreFactory;

    /**
     * The flink-autoscaler doesn't use the extraJobInfo, it is only used in some implements. This
     * whole context will be passed to these implements when the autoscaler callbacks them.
     */
    @Getter @Nullable private final INFO extraJobInfo;

    public RestClusterClient<String> getRestClusterClient() throws Exception {
        return restClientSupplier.get();
    }

    public Optional<AutoScalerStateStore> getStateStore() {
        return stateStoreFactory.get();
    }

    public AutoScalerStateStore getOrCreateStateStore() {
        return stateStoreFactory.getOrCreate();
    }
}
