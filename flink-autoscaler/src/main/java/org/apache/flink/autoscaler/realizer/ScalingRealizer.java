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

package org.apache.flink.autoscaler.realizer;

import org.apache.flink.annotation.Experimental;
import org.apache.flink.autoscaler.JobAutoScalerContext;
import org.apache.flink.autoscaler.tuning.ConfigChanges;

import java.util.Map;

/**
 * The Scaling Realizer is responsible for applying scaling actions, i.e. actually rescaling the
 * jobs.
 *
 * @param <KEY> The job key.
 * @param <Context> Instance of JobAutoScalerContext.
 */
@Experimental
public interface ScalingRealizer<KEY, Context extends JobAutoScalerContext<KEY>> {

    /**
     * Update job's parallelism to parallelismOverrides.
     *
     * @throws Exception Error during realize parallelism overrides.
     */
    void realizeParallelismOverrides(Context context, Map<String, String> parallelismOverrides)
            throws Exception;

    /**
     * Updates the TaskManager memory configuration.
     *
     * @throws Exception Error during realize config overrides.
     */
    void realizeConfigOverrides(Context context, ConfigChanges configChanges) throws Exception;
}
