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

package org.apache.flink.kubernetes.operator.hooks;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.plugin.Plugin;
import org.apache.flink.kubernetes.operator.api.FlinkSessionJob;
import org.apache.flink.kubernetes.operator.hooks.flink.FlinkCluster;

import io.fabric8.kubernetes.client.KubernetesClient;

/** Hook for executing custom logic during flink resource reconciliation. */
public interface FlinkResourceHook extends Plugin {

    /**
     * Get the name of the hook.
     *
     * @return Name of the hook.
     */
    String getName();

    /**
     * Get the status of the hook or execute the hook.
     *
     * @return Status of the hook.
     */
    FlinkResourceHookStatus execute(FlinkResourceHookContext context);

    /**
     * Gets the hook type for which the hook is applicable.
     *
     * @return The hook type.
     */
    FlinkResourceHookType getHookType();

    /** Context for the {@link FlinkResourceHook}. */
    interface FlinkResourceHookContext {
        FlinkSessionJob getFlinkSessionJob();

        FlinkCluster getFlinkSessionCluster();

        Configuration getDeployConfig();

        Configuration getCurrentDeployedConfig();

        KubernetesClient getKubernetesClient();
    }
}
