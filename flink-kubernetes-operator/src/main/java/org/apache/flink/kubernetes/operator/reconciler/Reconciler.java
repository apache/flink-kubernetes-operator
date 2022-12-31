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

import org.apache.flink.kubernetes.operator.api.AbstractFlinkResource;
import org.apache.flink.kubernetes.operator.controller.FlinkResourceContext;

import io.javaoperatorsdk.operator.api.reconciler.DeleteControl;

/**
 * The interface of reconciler.
 *
 * @param <CR> The custom resource to be reconciled.
 */
public interface Reconciler<CR extends AbstractFlinkResource<?, ?>> {

    /**
     * This is called when receiving the create or update event of the custom resource.
     *
     * @param context the context with which the operation is executed
     * @throws Exception Error during reconciliation.
     */
    void reconcile(FlinkResourceContext<CR> context) throws Exception;

    /**
     * This is called when receiving the delete event of custom resource. This method is meant to
     * cleanup the associated components like the Flink job components.
     *
     * @param context the context with which the operation is executed
     * @return DeleteControl to manage the deletion behavior
     */
    DeleteControl cleanup(FlinkResourceContext<CR> context);
}
