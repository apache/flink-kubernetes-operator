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

import lombok.Getter;

import java.util.Collection;
import java.util.stream.Collectors;

/**
 * Why the autoscaler changed the parallelism of a vertex.
 *
 * <p>The {@code autoscaler.scalings} counter reports that a change happened but not what drove it.
 * These constants tag the counter so that an operator can tell a backlog driven scale up from a
 * sustained load one, and can alert on only the kind that matters.
 *
 * <p>A vertex can report more than one reason in the same cycle. The target capacity formula adds
 * the lag catch up rate to the rate at the target utilization, so a backlog and a high load can
 * both contribute to one decision.
 *
 * <p>The constants are declared in the order that they appear in a tag, and the tags are lower case
 * and stable, because they are part of the metric surface.
 */
@Internal
public enum ScaleReason {

    /** A consumer lag drives the scale up, through the catch up data rate. */
    BACKLOG("backlog"),

    /** The sustained input rate needs more capacity than the vertex has. */
    HIGH_LOAD("high_load"),

    /** The current input rate is well above its own average, so the load is a spike. */
    INPUT_SPIKE("input_spike"),

    /** The vertex has more capacity than it uses, so the parallelism drops. */
    LOW_UTIL("low_util");

    /** Separates the reasons inside one tag value. */
    public static final String TAG_DELIMITER = "|";

    @Getter private final String tag;

    ScaleReason(String tag) {
        this.tag = tag;
    }

    /**
     * Joins the reasons into one tag value, in declaration order, for example {@code
     * backlog|high_load}. Four reasons give at most 15 distinct values.
     *
     * @param reasons the reasons observed across all scaled vertices, never empty
     * @return the tag value for the {@code autoscaler.scalings} counter
     */
    public static String toTag(Collection<ScaleReason> reasons) {
        return reasons.stream()
                .sorted()
                .map(ScaleReason::getTag)
                .collect(Collectors.joining(TAG_DELIMITER));
    }
}
