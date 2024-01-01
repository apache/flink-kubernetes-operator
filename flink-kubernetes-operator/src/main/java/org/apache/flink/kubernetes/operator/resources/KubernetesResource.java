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

package org.apache.flink.kubernetes.operator.resources;

import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.Setter;

/** A Kubernetes resource and its current allocation. */
@RequiredArgsConstructor
@Getter
public class KubernetesResource {

    /** Allocatable as per Kubernetes cluster API. */
    private final double allocatable;

    /** Used as per Kubernetes cluster API. */
    private final double used;

    /** Reserved via the corresponding Setter. */
    @Getter private double reserved;

    /** Pending reservation which is not yet committed. */
    @Getter @Setter private double pending;

    public void commitPending() {
        this.reserved += pending;
        this.pending = 0;
    }

    public void release(double amount) {
        this.pending -= Math.min(amount, getUsed());
    }

    public double getFree() {
        return allocatable - getUsed();
    }

    public double getUsed() {
        return used + reserved + pending;
    }

    @Override
    public String toString() {
        return "KubernetesResource{"
                + "allocatable="
                + allocatable
                + ", used="
                + used
                + ", reserved="
                + reserved
                + ", pending="
                + pending
                + '}';
    }
}
