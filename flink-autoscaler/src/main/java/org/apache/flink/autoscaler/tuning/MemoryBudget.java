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

package org.apache.flink.autoscaler.tuning;

import org.apache.flink.util.Preconditions;

/** Accounting for distributing memory over the available pools. */
public class MemoryBudget {

    private long remaining;

    public MemoryBudget(long remaining) {
        Preconditions.checkArgument(remaining >= 0);
        this.remaining = remaining;
    }

    public long budget(long amount) {
        Preconditions.checkArgument(amount >= 0);
        var budgeted = Math.min(amount, remaining);
        remaining -= budgeted;
        return budgeted;
    }

    public long getRemaining() {
        return remaining;
    }
}
