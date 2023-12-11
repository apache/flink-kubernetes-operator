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

package org.apache.flink.autoscaler.config;

import org.apache.flink.configuration.Configuration;

import java.util.HashMap;
import java.util.Map;

/** A config to consolidate configuration keys with multiple prefixes. */
public class KeyMigratingConfiguration extends Configuration {

    public KeyMigratingConfiguration(String prefixToRemove, Configuration config) {
        super(config);
        migrateOldConfigKeys(prefixToRemove);
    }

    private void migrateOldConfigKeys(String prefixToRemove) {
        var toBeMigrated = new HashMap<String, Object>();
        for (Map.Entry<String, Object> entry : confData.entrySet()) {
            if (entry.getKey().startsWith(prefixToRemove)) {
                toBeMigrated.put(entry.getKey(), entry.getValue());
            }
        }
        for (Map.Entry<String, Object> entry : toBeMigrated.entrySet()) {
            String migratedKey = entry.getKey().substring(prefixToRemove.length());
            confData.putIfAbsent(migratedKey, entry.getValue());
            confData.remove(entry.getKey());
        }
    }
}
