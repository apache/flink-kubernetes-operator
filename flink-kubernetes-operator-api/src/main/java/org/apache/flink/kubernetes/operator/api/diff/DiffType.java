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

package org.apache.flink.kubernetes.operator.api.diff;

import org.apache.flink.annotation.Experimental;

import java.util.Collection;
import java.util.Comparator;

/** Spec change type. */
@Experimental
public enum DiffType {

    /** Ignorable spec change. */
    IGNORE,
    /** Scalable spec change. */
    SCALE,
    /** Upgradable spec change. */
    UPGRADE,
    /** Full redeploy from new state. */
    SAVEPOINT_REDEPLOY;

    /**
     * Aggregate a collection of {@link DiffType}s into the type that minimally subsumes all the
     * diffs. We rely on the fact that the enum values are sorted this way.
     *
     * @param diffs Collection of diffs.
     * @return Aggregated {@link DiffType}
     */
    public static DiffType from(Collection<DiffType> diffs) {
        return diffs.stream().max(Comparator.comparing(DiffType::ordinal)).orElse(DiffType.IGNORE);
    }
}
