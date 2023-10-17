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

import org.apache.flink.annotation.Experimental;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.client.program.rest.RestClusterClient;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.util.function.SupplierWithException;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.ToString;

import javax.annotation.Nullable;

/**
 * The job autoscaler context, it includes all details related to the current job.
 *
 * @param <KEY> The job key.
 */
@Experimental
@AllArgsConstructor
@ToString
public class JobAutoScalerContext<KEY> {

    /** The identifier of each flink job. */
    @Getter private final KEY jobKey;

    /** The jobId and jobStatus can be null when the job isn't started. */
    @Nullable @Getter private final JobID jobID;

    @Nullable @Getter private final JobStatus jobStatus;

    @Getter private final Configuration configuration;

    @Getter private final MetricGroup metricGroup;

    @ToString.Exclude
    private final SupplierWithException<RestClusterClient<String>, Exception> restClientSupplier;

    public RestClusterClient<String> getRestClusterClient() throws Exception {
        return restClientSupplier.get();
    }
}
