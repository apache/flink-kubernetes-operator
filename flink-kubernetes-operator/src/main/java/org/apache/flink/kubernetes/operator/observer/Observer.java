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

import org.apache.flink.configuration.Configuration;
import org.apache.flink.kubernetes.operator.crd.FlinkDeployment;

import io.javaoperatorsdk.operator.api.reconciler.Context;

/** The Observer of {@link FlinkDeployment}. */
public interface Observer {

    /**
     * Observe the flinkApp status, It will reflect the changed status on the flinkApp resource.
     *
     * @param flinkApp the target flinkDeployment resource
     * @param context the context with which the operation is executed
     * @param effectiveConfig the effective config of the flinkApp
     */
    void observe(FlinkDeployment flinkApp, Context context, Configuration effectiveConfig);
}
