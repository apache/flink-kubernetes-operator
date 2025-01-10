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

import org.apache.flink.annotation.Internal;

/**
 * Flink Job AutoScaler.
 *
 * @param <KEY> The job key.
 * @param <Context> Instance of {@link JobAutoScalerContext}.
 */
@Internal
public interface JobAutoScaler<KEY, Context extends JobAutoScalerContext<KEY>> {

    /**
     * Compute and apply new parallelism overrides for the provided job context.
     *
     * @param context Job context.
     * @throws Exception
     */
    void scale(Context context) throws Exception;

    /**
     * Called when the job is deleted.
     *
     * @param context Job context.
     */
    void cleanup(Context context);
}
