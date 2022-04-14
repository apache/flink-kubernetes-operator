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

package org.apache.flink.kubernetes.operator.crd.status;

import org.apache.flink.annotation.Experimental;
import org.apache.flink.kubernetes.operator.crd.spec.FlinkSessionJobSpec;
import org.apache.flink.kubernetes.operator.reconciler.ReconciliationUtils;

import com.fasterxml.jackson.annotation.JsonIgnore;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/** Status of the last reconcile step for the session job. */
@Experimental
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class FlinkSessionJobReconciliationStatus {

    /**
     * Last reconciled job spec. Used to decide whether further reconciliation steps are necessary.
     */
    private String lastReconciledSpec;

    @JsonIgnore
    public FlinkSessionJobSpec deserializeLastReconciledSpec() {
        return ReconciliationUtils.deserializedSpecWithVersion(
                lastReconciledSpec, FlinkSessionJobSpec.class);
    }

    @JsonIgnore
    public void serializeAndSetLastReconciledSpec(FlinkSessionJobSpec spec) {
        setLastReconciledSpec(ReconciliationUtils.writeSpecWithCurrentVersion(spec));
    }
}
