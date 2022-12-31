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

package org.apache.flink.kubernetes.operator.reconciler;

import org.apache.flink.kubernetes.operator.OperatorTestBase;
import org.apache.flink.kubernetes.operator.api.AbstractFlinkResource;
import org.apache.flink.kubernetes.operator.api.spec.AbstractFlinkSpec;
import org.apache.flink.kubernetes.operator.api.status.CommonStatus;
import org.apache.flink.kubernetes.operator.reconciler.deployment.AbstractFlinkResourceReconciler;

import io.javaoperatorsdk.operator.api.reconciler.Context;
import io.javaoperatorsdk.operator.api.reconciler.DeleteControl;

/** Test adapter for old reconciler interface. */
public class TestReconcilerAdapter<
        CR extends AbstractFlinkResource<SPEC, STATUS>,
        SPEC extends AbstractFlinkSpec,
        STATUS extends CommonStatus<SPEC>> {

    private final OperatorTestBase operatorTestBase;

    private final AbstractFlinkResourceReconciler<CR, SPEC, STATUS> reconciler;

    public TestReconcilerAdapter(
            OperatorTestBase operatorTestBase,
            AbstractFlinkResourceReconciler<CR, SPEC, STATUS> reconciler) {
        this.operatorTestBase = operatorTestBase;
        this.reconciler = reconciler;
    }

    public void reconcile(CR cr, Context<?> context) throws Exception {
        var crClone = ReconciliationUtils.clone(cr);
        crClone.setStatus(cr.getStatus());
        reconciler.reconcile(operatorTestBase.getResourceContext(crClone, context));
    }

    public DeleteControl cleanup(CR cr, Context<?> context) {
        var crClone = ReconciliationUtils.clone(cr);
        crClone.setStatus(cr.getStatus());
        return reconciler.cleanup(operatorTestBase.getResourceContext(crClone, context));
    }

    public AbstractFlinkResourceReconciler<CR, SPEC, STATUS> getReconciler() {
        return reconciler;
    }
}
