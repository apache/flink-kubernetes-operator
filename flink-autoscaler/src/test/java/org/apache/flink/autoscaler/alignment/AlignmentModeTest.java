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

package org.apache.flink.autoscaler.alignment;

import org.apache.flink.api.common.JobID;
import org.apache.flink.autoscaler.config.AutoScalerOptions;
import org.apache.flink.autoscaler.topology.ShipStrategy;
import org.apache.flink.configuration.Configuration;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

import static org.apache.flink.autoscaler.TestingAutoscalerUtils.createDefaultJobAutoScalerContext;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.params.provider.Arguments.arguments;

/**
 * Tests for the built-in and legacy {@link ParallelismAlignmentMode}s and {@link
 * ParallelismAligner}.
 */
class AlignmentModeTest {

    private final AtomicInteger emitted = new AtomicInteger();

    private final ParallelismAligner.ScalingLimitedEmitter emitter =
            (expected, actual) -> emitted.incrementAndGet();

    private ParallelismAlignmentMode.Context<JobID> ctx(
            int current, int newParallelism, int numSourcePartitions, int maxParallelism) {
        return ctx(
                current,
                newParallelism,
                numSourcePartitions,
                maxParallelism,
                1,
                Integer.MAX_VALUE,
                List.of(ShipStrategy.HASH));
    }

    private ParallelismAlignmentMode.Context<JobID> ctx(
            int current,
            int newParallelism,
            int numSourcePartitions,
            int maxParallelism,
            int parallelismLowerLimit,
            int parallelismUpperLimit,
            Collection<ShipStrategy> inputShipStrategies) {
        return new ParallelismAlignmentMode.Context<>(
                createDefaultJobAutoScalerContext(),
                new Configuration(),
                null,
                current,
                newParallelism,
                numSourcePartitions,
                maxParallelism,
                parallelismLowerLimit,
                parallelismUpperLimit,
                inputShipStrategies,
                Map.of());
    }

    @Test
    void balancedReducesLoadAndNeverEmits() {
        // N=128, target 24: BALANCED accepts 26 (128/26=4 < 128/24=5), EVENLY_SPREAD snaps to 32.
        assertThat(BuiltInAlignmentMode.BALANCED.alignParallelism(ctx(16, 24, 0, 128)))
                .isEqualTo(26);
        assertThat(BuiltInAlignmentMode.EVENLY_SPREAD.alignParallelism(ctx(16, 24, 0, 128)))
                .isEqualTo(32);
        assertThat(emitted).hasValue(0);
    }

    @Test
    void offReturnsComputedTarget() {
        assertThat(BuiltInAlignmentMode.OFF.alignParallelism(ctx(16, 24, 0, 128))).isEqualTo(24);
    }

    @Test
    void builtInModesFallBackToTargetInsteadOfBlocking() {
        // current=22, target=25, N=35 source partitions, upper bound 30: no aligned value preserves
        // the scale-up. Built-in modes return the raw target (OFF fallback), and emit no event.
        var blocked = ctx(22, 25, 35, 128, 20, 30, List.of(ShipStrategy.HASH));
        assertThat(BuiltInAlignmentMode.EVENLY_SPREAD.alignParallelism(blocked)).isEqualTo(25);
        assertThat(BuiltInAlignmentMode.BALANCED.alignParallelism(blocked)).isEqualTo(25);
        assertThat(emitted).hasValue(0);
    }

    @Test
    @SuppressWarnings("deprecation")
    void builtInModesSearchOneRegionAndDoNotRelaxBelowTarget() {
        // N=12, scale-up to target 7 with the region capped at 8: no divisor in [7, 8]. The new
        // built-in modes keep the target (7), whereas the legacy modes relax below the target to
        // the nearest divisor 6.
        var c = ctx(5, 7, 12, 8);
        assertThat(BuiltInAlignmentMode.EVENLY_SPREAD.alignParallelism(c)).isEqualTo(7);
        assertThat(KeyGroupOrPartitionsAdjustMode.EVENLY_SPREAD.alignParallelism(c)).isEqualTo(6);
    }

    @Test
    @SuppressWarnings("deprecation")
    void legacyModesBlockAndEmit() {
        // The same scenario under the deprecated legacy mode blocks (returns current) and emits.
        var blocked = ctx(22, 25, 35, 128, 20, 30, List.of(ShipStrategy.HASH));
        assertThat(KeyGroupOrPartitionsAdjustMode.EVENLY_SPREAD.alignParallelism(blocked, emitter))
                .isEqualTo(22);
        assertThat(emitted).hasValue(1);
    }

    @SuppressWarnings("deprecation")
    static Stream<ParallelismAlignmentMode> aligningModes() {
        return Stream.of(
                BuiltInAlignmentMode.BALANCED,
                BuiltInAlignmentMode.EVENLY_SPREAD,
                KeyGroupOrPartitionsAdjustMode.EVENLY_SPREAD,
                KeyGroupOrPartitionsAdjustMode.MAXIMIZE_UTILISATION);
    }

    @SuppressWarnings("deprecation")
    static Stream<KeyGroupOrPartitionsAdjustMode> legacyModes() {
        return Stream.of(
                KeyGroupOrPartitionsAdjustMode.EVENLY_SPREAD,
                KeyGroupOrPartitionsAdjustMode.MAXIMIZE_UTILISATION);
    }

    /** Every aligning mode against a target above the cap, on both sides of the current value. */
    static Stream<Arguments> cappedTargets() {
        return aligningModes()
                .flatMap(mode -> Stream.of(180, 50, 16, 15).map(target -> arguments(mode, target)));
    }

    @ParameterizedTest(name = "{0}, target {1}")
    @MethodSource("cappedTargets")
    void targetAbovePartitionCountIsCappedInEitherDirection(
            ParallelismAlignmentMode mode, int target) {
        // 15 partitions, running at 100: 15 is the ceiling whether the target is above it (180),
        // below it (50, 16), or already on it (15).
        assertThat(mode.alignParallelism(ctx(100, target, 15, 180))).isEqualTo(15);
    }

    @Test
    void offKeepsTheTargetAbovePartitionCount() {
        // OFF opts out of alignment altogether, so the over-provisioning is the user's choice.
        assertThat(BuiltInAlignmentMode.OFF.alignParallelism(ctx(100, 180, 15, 180)))
                .isEqualTo(180);
        assertThat(BuiltInAlignmentMode.OFF.alignParallelism(ctx(100, 50, 15, 180))).isEqualTo(50);
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("aligningModes")
    void keyedVertexIsCappedAtItsKeyGroupCount(ParallelismAlignmentMode mode) {
        // Without source partitions the cap is the key group count. scale() already clamps the
        // target to maxParallelism, so only sources reach this in practice.
        assertThat(mode.alignParallelism(ctx(20, 18, 0, 12))).isEqualTo(12);
    }

    /** Legacy modes only, and only targets that deviate from the cap, so an event is expected. */
    static Stream<Arguments> legacyCappedTargets() {
        return legacyModes()
                .flatMap(mode -> Stream.of(180, 50, 16).map(target -> arguments(mode, target)));
    }

    @ParameterizedTest(name = "{0}, target {1}")
    @MethodSource("legacyCappedTargets")
    @SuppressWarnings("deprecation")
    void legacyEmitsWheneverCappedAtPartitionCount(
            KeyGroupOrPartitionsAdjustMode mode, int target) {
        // Capping is not an inversion, but it still deviates from the target, so the event stays.
        assertThat(mode.alignParallelism(ctx(100, target, 15, 180), emitter)).isEqualTo(15);
        assertThat(emitted).hasValue(1);
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("legacyModes")
    @SuppressWarnings("deprecation")
    void alreadyAtThePartitionCountBlocksAScaleUp(KeyGroupOrPartitionsAdjustMode mode) {
        // Already on the cap: the candidate is not below current, so the scale-up stays blocked.
        assertThat(mode.alignParallelism(ctx(15, 20, 15, 180), emitter)).isEqualTo(15);
        assertThat(emitted).hasValue(1);
    }

    @Test
    @SuppressWarnings("deprecation")
    void movingUpToTheCapOnAScaleDownIsStillAnInversion() {
        // The lower limit clamps the fallback up onto the cap (12), above the current 10, so
        // taking it would invert the scale-down.
        var c = ctx(10, 8, 12, 128, 12, 12, List.of(ShipStrategy.HASH));
        assertThat(KeyGroupOrPartitionsAdjustMode.EVENLY_SPREAD.alignParallelism(c, emitter))
                .isEqualTo(10);
        assertThat(emitted).hasValue(1);
    }

    @Test
    @SuppressWarnings("deprecation")
    void lowerLimitClampAboveCurrentStillBlocksAScaleDown() {
        // The same inversion, but below the cap (candidate 11, cap 30): the exemption must not
        // reach it.
        var c = ctx(10, 8, 35, 128, 11, 30, List.of(ShipStrategy.HASH));
        assertThat(KeyGroupOrPartitionsAdjustMode.EVENLY_SPREAD.alignParallelism(c, emitter))
                .isEqualTo(10);
        assertThat(emitted).hasValue(1);
    }

    @Test
    void modesDoNotApplyToNonSourceNonHashVertices() {
        var rebalance = ctx(16, 24, 0, 128, 1, Integer.MAX_VALUE, List.of(ShipStrategy.REBALANCE));
        assertThat(BuiltInAlignmentMode.BALANCED.isApplicable(rebalance)).isFalse();
        assertThat(BuiltInAlignmentMode.EVENLY_SPREAD.isApplicable(rebalance)).isFalse();
        assertThat(BuiltInAlignmentMode.OFF.isApplicable(rebalance)).isFalse();
        // A source / keyBy vertex is in scope.
        var hash = ctx(16, 24, 0, 128);
        assertThat(BuiltInAlignmentMode.BALANCED.isApplicable(hash)).isTrue();
    }

    @Test
    void resolveSelectsBuiltInByName() {
        Configuration conf = new Configuration();
        conf.set(AutoScalerOptions.ALIGNMENT_MODE, "EVENLY_SPREAD");
        assertThat(new ParallelismAligner(List.of()).resolve(conf))
                .isEqualTo(BuiltInAlignmentMode.EVENLY_SPREAD);
    }

    @Test
    void resolveDefaultsToBalanced() {
        assertThat(new ParallelismAligner(List.of()).resolve(new Configuration()))
                .isEqualTo(BuiltInAlignmentMode.BALANCED);
    }

    @Test
    @SuppressWarnings("deprecation")
    void resolveHonorsDeprecatedKey() {
        Configuration conf = new Configuration();
        conf.set(
                AutoScalerOptions.SCALING_KEY_GROUP_PARTITIONS_ADJUST_MODE,
                KeyGroupOrPartitionsAdjustMode.MAXIMIZE_UTILISATION);
        assertThat(new ParallelismAligner(List.of()).resolve(conf))
                .isEqualTo(KeyGroupOrPartitionsAdjustMode.MAXIMIZE_UTILISATION);
    }

    @Test
    @SuppressWarnings("deprecation")
    void newKeyTakesPrecedenceOverDeprecatedKey() {
        Configuration conf = new Configuration();
        conf.set(AutoScalerOptions.ALIGNMENT_MODE, "OFF");
        conf.set(
                AutoScalerOptions.SCALING_KEY_GROUP_PARTITIONS_ADJUST_MODE,
                KeyGroupOrPartitionsAdjustMode.EVENLY_SPREAD);
        assertThat(new ParallelismAligner(List.of()).resolve(conf))
                .isEqualTo(BuiltInAlignmentMode.OFF);
    }

    @Test
    void resolveSelectsCustomModeByClass() {
        Configuration conf = new Configuration();
        conf.set(AutoScalerOptions.ALIGNMENT_MODE, "custom");
        conf.set(
                AutoScalerOptions.customAlignmentModeClassOption("custom"),
                TestAlignmentMode.class.getName());
        ParallelismAlignmentMode custom = new TestAlignmentMode();
        assertThat(new ParallelismAligner(List.of(custom)).resolve(conf)).isSameAs(custom);
    }

    @Test
    void resolveFallsBackWhenCustomModeMissing() {
        Configuration conf = new Configuration();
        conf.set(AutoScalerOptions.ALIGNMENT_MODE, "custom");
        conf.set(AutoScalerOptions.customAlignmentModeClassOption("custom"), "com.example.Missing");
        assertThat(new ParallelismAligner(List.of()).resolve(conf))
                .isEqualTo(BuiltInAlignmentMode.BALANCED);
    }
}
