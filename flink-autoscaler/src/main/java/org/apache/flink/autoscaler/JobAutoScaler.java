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

import java.util.Map;

/** The general Autoscaler instance. */
public interface JobAutoScaler<KEY, INFO> {

    /** Called as part of the reconciliation loop. Returns true if this call led to scaling. */
    boolean scale(JobAutoScalerContext<KEY, INFO> context);

    /** Called when the job is deleted. */
    void cleanup(JobAutoScalerContext<KEY, INFO> context);

    /** Get the current parallelism overrides for the job. */
    Map<String, String> getParallelismOverrides(JobAutoScalerContext<KEY, INFO> context);
}
