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
import org.apache.flink.kubernetes.operator.api.spec.KubernetesDeploymentMode;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/** Spec diff annotation. */
@Experimental
@Target(ElementType.FIELD)
@Retention(RetentionPolicy.RUNTIME)
public @interface SpecDiff {
    DiffType value() default DiffType.UPGRADE;

    KubernetesDeploymentMode[] mode() default {};

    boolean onNullIgnore() default false;

    /** Spec diff config annotation. */
    @Target(ElementType.FIELD)
    @Retention(RetentionPolicy.RUNTIME)
    @interface Config {
        Entry[] value();
    }

    /** Spec diff config annotation entry. */
    @Target(ElementType.FIELD)
    @Retention(RetentionPolicy.RUNTIME)
    @interface Entry {
        String prefix();

        DiffType type();

        KubernetesDeploymentMode[] mode() default {};
    }
}
