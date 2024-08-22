/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.kubernetes.operator.api.spec;

import org.apache.flink.annotation.Experimental;

/** Enumeration for supported Flink versions. */
@Experimental
public enum FlinkVersion {
    /** No longer supported since 1.7 operator release. */
    @Deprecated
    v1_13,
    /** No longer supported since 1.7 operator release. */
    @Deprecated
    v1_14,
    /** Deprecated since 1.10 operator release. */
    @Deprecated
    v1_15,
    v1_16,
    v1_17,
    v1_18,
    v1_19,
    v1_20;

    public boolean isEqualOrNewer(FlinkVersion otherVersion) {
        return this.ordinal() >= otherVersion.ordinal();
    }

    /**
     * Returns the current version.
     *
     * @return The current version.
     */
    public static FlinkVersion current() {
        return values()[values().length - 1];
    }

    public static boolean isSupported(FlinkVersion version) {
        return version != null && version.isEqualOrNewer(FlinkVersion.v1_15);
    }
}
