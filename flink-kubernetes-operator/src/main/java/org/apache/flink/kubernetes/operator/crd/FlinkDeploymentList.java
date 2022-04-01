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

package org.apache.flink.kubernetes.operator.crd;

import org.apache.flink.annotation.Experimental;

import io.fabric8.kubernetes.client.CustomResourceList;

/**
 * Multiple Flink deployments. This class is needed by upstream projects, if they have to use this
 * flink operator CRD via fabric8 java client. do not delete
 */
@Experimental
public class FlinkDeploymentList extends CustomResourceList<FlinkDeployment> {
    public FlinkDeploymentList() {}
}
