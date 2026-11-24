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

package org.apache.flink.kubernetes.operator.utils;

import org.apache.flink.kubernetes.operator.api.FlinkStateSnapshot;
import org.apache.flink.kubernetes.operator.api.spec.FlinkStateSnapshotSpec;
import org.apache.flink.kubernetes.operator.api.spec.JobKind;
import org.apache.flink.kubernetes.operator.api.spec.JobReference;

import io.fabric8.kubernetes.api.model.ObjectMetaBuilder;
import io.javaoperatorsdk.operator.processing.event.ResourceID;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link EventSourceUtils}. */
class EventSourceUtilsTest {

    private static final String NAMESPACE = "ns";
    private static final String NAME = "job";

    @ParameterizedTest
    @EnumSource(JobKind.class)
    void snapshotMapsToResourceOfSameKind(JobKind kind) {
        assertThat(EventSourceUtils.getSnapshotJobResourceIds(snapshot(kind, NAME), kind))
                .containsExactly(new ResourceID(NAME, NAMESPACE));
    }

    @ParameterizedTest
    @EnumSource(JobKind.class)
    void snapshotDoesNotMapToSameNamedResourceOfOtherKind(JobKind kind) {
        for (var otherKind : JobKind.values()) {
            if (otherKind != kind) {
                assertThat(
                                EventSourceUtils.getSnapshotJobResourceIds(
                                        snapshot(otherKind, NAME), kind))
                        .as("%s snapshot mapped to %s", otherKind, kind)
                        .isEmpty();
            }
        }
    }

    @Test
    void snapshotWithoutJobReferenceKindMapsToNothing() {
        assertThat(
                        EventSourceUtils.getSnapshotJobResourceIds(
                                snapshot(null, NAME), JobKind.FLINK_DEPLOYMENT))
                .isEmpty();
    }

    @Test
    void snapshotWithoutJobReferenceNameMapsToNothing() {
        assertThat(
                        EventSourceUtils.getSnapshotJobResourceIds(
                                snapshot(JobKind.FLINK_DEPLOYMENT, null), JobKind.FLINK_DEPLOYMENT))
                .isEmpty();
    }

    @Test
    void snapshotWithoutJobReferenceMapsToNothing() {
        var snapshot = snapshot(JobKind.FLINK_DEPLOYMENT, NAME);
        snapshot.getSpec().setJobReference(null);
        assertThat(EventSourceUtils.getSnapshotJobResourceIds(snapshot, JobKind.FLINK_DEPLOYMENT))
                .isEmpty();
    }

    private static FlinkStateSnapshot snapshot(JobKind kind, String name) {
        var snapshot = new FlinkStateSnapshot();
        snapshot.setMetadata(
                new ObjectMetaBuilder().withName("snapshot").withNamespace(NAMESPACE).build());
        var spec = new FlinkStateSnapshotSpec();
        spec.setJobReference(JobReference.builder().kind(kind).name(name).build());
        snapshot.setSpec(spec);
        return snapshot;
    }
}
