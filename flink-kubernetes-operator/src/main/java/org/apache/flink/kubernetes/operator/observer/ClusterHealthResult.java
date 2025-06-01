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

package org.apache.flink.kubernetes.operator.observer;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Value;

import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/** Cluster Health Result. */
@Value
public class ClusterHealthResult {
    boolean healthy;
    String error;

    @JsonCreator
    public ClusterHealthResult(
            @JsonProperty("healthy") boolean healthy, @JsonProperty("error") String error) {
        this.healthy = healthy;
        this.error = error;
    }

    public static ClusterHealthResult error(String error) {
        return new ClusterHealthResult(false, error);
    }

    public static ClusterHealthResult healthy() {
        return new ClusterHealthResult(true, null);
    }

    public ClusterHealthResult join(ClusterHealthResult clusterHealthResult) {
        boolean isHealthy = this.healthy && clusterHealthResult.healthy;
        String error =
                Stream.of(this.error, clusterHealthResult.getError())
                        .filter(Objects::nonNull)
                        .collect(Collectors.joining("|"));

        return new ClusterHealthResult(isHealthy, error);
    }
}
