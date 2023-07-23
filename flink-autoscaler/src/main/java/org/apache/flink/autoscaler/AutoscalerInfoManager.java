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

import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/** Class responsible to managing the creation and retrieval of {@link AutoScalerInfo} objects. */
public class AutoscalerInfoManager<KEY> {

    private final ConcurrentHashMap<KEY, Optional<AutoScalerInfo>> cache =
            new ConcurrentHashMap<>();

    public AutoscalerInfoManager() {}

    public AutoScalerInfo getOrCreateInfo(JobAutoScalerContext<KEY, ?> context) {
        return cache.compute(
                        context.getJobKey(),
                        (id, infOpt) -> {
                            // If in the cache and valid simply return
                            if (infOpt != null
                                    && infOpt.map(AutoScalerInfo::isValid).orElse(false)) {
                                return infOpt;
                            }
                            // Otherwise get or create
                            return Optional.of(new AutoScalerInfo(context.getOrCreateStateStore()));
                        })
                .get();
    }

    public Optional<AutoScalerInfo> getInfo(JobAutoScalerContext<KEY, ?> context) {
        return cache.compute(
                context.getJobKey(),
                (id, infOpt) -> {
                    // If in the cache and empty or valid simply return
                    if (infOpt != null && (infOpt.isEmpty() || infOpt.get().isValid())) {
                        return infOpt;
                    }

                    // Otherwise get
                    return context.getStateStore().map(AutoScalerInfo::new);
                });
    }

    public void removeInfoFromCache(JobAutoScalerContext<KEY, ?> context) {
        // We don't need to remove from Kubernetes, that is handled through the owner reference
        cache.remove(context.getJobKey());
    }
}
