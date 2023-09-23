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

import org.apache.flink.autoscaler.JobAutoScalerContext;

import java.util.LinkedList;
import java.util.Map;

/** Testing {@link ScalingRealizer} implementation. */
public class TestingScalingRealizer<KEY, Context extends JobAutoScalerContext<KEY>>
        implements ScalingRealizer<KEY, Context> {

    public final LinkedList<Event<KEY, Context>> events = new LinkedList<>();

    @Override
    public void realize(Context context, Map<String, String> parallelismOverrides) {
        events.add(new Event<>(context, parallelismOverrides));
    }

    /** The collected event. */
    public static class Event<KEY, Context extends JobAutoScalerContext<KEY>> {

        private final Context context;

        private final Map<String, String> parallelismOverrides;

        public Event(Context context, Map<String, String> parallelismOverrides) {
            this.context = context;
            this.parallelismOverrides = parallelismOverrides;
        }

        public Context getContext() {
            return context;
        }

        public Map<String, String> getParallelismOverrides() {
            return parallelismOverrides;
        }
    }
}
