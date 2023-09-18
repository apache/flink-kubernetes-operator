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

package org.apache.flink.autoscaler.event;

import org.apache.flink.annotation.Experimental;
import org.apache.flink.autoscaler.JobAutoScalerContext;

import javax.annotation.Nullable;

import java.time.Duration;

/**
 * Handler all loggable events during scaling.
 *
 * @param <KEY> The job key.
 * @param <Context> Instance of JobAutoScalerContext.
 */
@Experimental
public interface AutoScalerEventHandler<KEY, Context extends JobAutoScalerContext<KEY>> {

    /**
     * Handle the event.
     *
     * @param interval When interval is great than 0, events that repeat within the interval will be
     *     ignored.
     */
    void handleEvent(
            Context context,
            Type type,
            String reason,
            String message,
            @Nullable String messageKey,
            @Nullable Duration interval);

    /** The type of the events. */
    enum Type {
        Normal,
        Warning
    }
}
