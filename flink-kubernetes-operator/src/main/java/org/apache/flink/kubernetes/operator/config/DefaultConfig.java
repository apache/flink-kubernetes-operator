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

package org.apache.flink.kubernetes.operator.config;

import org.apache.flink.configuration.Configuration;

/** The container for the operator component default config. */
public class DefaultConfig {

    private final Configuration operatorConfig;
    private final Configuration flinkConfig;

    public DefaultConfig(Configuration operatorConfig, Configuration flinkConfig) {
        this.operatorConfig = operatorConfig;
        this.flinkConfig = flinkConfig;
    }

    public Configuration getOperatorConfig() {
        return operatorConfig;
    }

    public Configuration getFlinkConfig() {
        return flinkConfig;
    }
}
