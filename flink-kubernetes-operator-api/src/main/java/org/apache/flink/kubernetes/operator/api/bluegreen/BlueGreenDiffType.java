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

package org.apache.flink.kubernetes.operator.api.bluegreen;

/**
 * Enum representing different types of differences found in Blue/Green deployment specifications.
 */
public enum BlueGreenDiffType {
    /** No differences found between specifications. */
    IGNORE,

    /** Only top-level properties (metadata, configuration) have differences. */
    PATCH_TOP_LEVEL,

    /** Only the nested FlinkDeploymentSpec has differences. */
    PATCH_CHILD,

    /** Both top-level and nested specifications have differences. */
    PATCH_BOTH,

    /** The nested spec has SCALE or UPGRADE differences, requiring a blue/green transition. */
    TRANSITION
}
