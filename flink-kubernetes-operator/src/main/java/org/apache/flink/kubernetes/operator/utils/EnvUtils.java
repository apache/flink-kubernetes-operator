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

import org.apache.commons.lang3.StringUtils;

/** Util to get value from environments. */
public class EnvUtils {

    public static final String ENV_FLINK_OPERATOR_CONF_DIR = "FLINK_OPERATOR_CONF_DIR";
    public static final String ENV_FLINK_CONF_DIR = "FLINK_CONF_DIR";
    public static final String ENV_WEBHOOK_KEYSTORE_FILE = "WEBHOOK_KEYSTORE_FILE";
    public static final String ENV_WEBHOOK_KEYSTORE_PASSWORD = "WEBHOOK_KEYSTORE_PASSWORD";
    public static final String ENV_WEBHOOK_KEYSTORE_TYPE = "WEBHOOK_KEYSTORE_TYPE";
    public static final String ENV_WEBHOOK_SERVER_PORT = "WEBHOOK_SERVER_PORT";
    public static final String ENV_WATCHED_NAMESPACES = "FLINK_OPERATOR_WATCH_NAMESPACES";
    public static final String ENV_HOSTNAME = "HOSTNAME";
    public static final String ENV_OPERATOR_NAME = "OPERATOR_NAME";
    public static final String ENV_OPERATOR_NAMESPACE = "OPERATOR_NAMESPACE";

    /**
     * Get the value provided by environments.
     *
     * @param key the target key
     * @return the value value provided by environments.
     */
    public static String get(String key) {
        return System.getenv().get(key);
    }

    /**
     * Get the value or default value provided by environments.
     *
     * @param key the target key
     * @param defaultValue the default value if key not exists.
     * @return the value or default value provided by environments.
     */
    public static String getOrDefault(String key, String defaultValue) {
        return System.getenv().getOrDefault(key, defaultValue);
    }

    /**
     * Get the value provided by environments.
     *
     * @param key the target key
     * @return the value provided by environments.
     */
    public static String getRequired(String key) {
        String value = System.getenv().get(key);
        if (StringUtils.isEmpty(value)) {
            throw new RuntimeException("Environments: " + key + " cannot be empty");
        }
        return value;
    }
}
