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

package org.apache.flink.kubernetes.operator.reconciler;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.kubernetes.operator.crd.FlinkDeployment;

import io.javaoperatorsdk.operator.api.reconciler.Context;
import io.javaoperatorsdk.operator.api.reconciler.DeleteControl;
import io.javaoperatorsdk.operator.api.reconciler.UpdateControl;

/** The interface of reconciler. */
public interface Reconciler {

    /**
     * This is called when receiving the create or update event of the FlinkDeployment resource.
     *
     * @param operatorNamespace The namespace of the operator
     * @param flinkApp the FlinkDeployment resource that has been created or updated
     * @param context the context with which the operation is executed
     * @param effectiveConfig the effective config of the flinkApp
     * @return UpdateControl to manage updates on the custom resource (usually the status) after
     *     reconciliation.
     */
    UpdateControl<FlinkDeployment> reconcile(
            String operatorNamespace,
            FlinkDeployment flinkApp,
            Context context,
            Configuration effectiveConfig)
            throws Exception;

    /**
     * This is called when receiving the delete event of FlinkDeployment resource. This method is
     * meant to cleanup the associated components like the Flink job components.
     *
     * @param operatorNamespace The namespace of the operator
     * @param flinkApp the FlinkDeployment resource that has been deleted
     * @param effectiveConfig the effective config of the flinkApp
     * @return DeleteControl to manage the delete behavior
     */
    DeleteControl cleanup(
            String operatorNamespace, FlinkDeployment flinkApp, Configuration effectiveConfig);
}
