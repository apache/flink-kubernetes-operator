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

import org.apache.flink.autoscaler.config.AutoScalerOptions;
import org.apache.flink.autoscaler.topology.ShipStrategy;
import org.apache.flink.configuration.Configuration;

import org.junit.jupiter.api.Test;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for the built-in and legacy {@link AlignmentMode}s and {@link ParallelismAligner}. */
class AlignmentModeTest {

    private final AtomicInteger emitted = new AtomicInteger();

    private AlignmentContext ctx(
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

    private AlignmentContext ctx(
            int current,
            int newParallelism,
            int numSourcePartitions,
            int maxParallelism,
            int parallelismLowerLimit,
            int parallelismUpperLimit,
            Collection<ShipStrategy> inputShipStrategies) {
        return new AlignmentContext(
                null,
                current,
                newParallelism,
                numSourcePartitions,
                maxParallelism,
                parallelismLowerLimit,
                parallelismUpperLimit,
                inputShipStrategies,
                null,
                Map.of(),
                null,
                new Configuration(),
                (expected, actual) -> emitted.incrementAndGet());
    }

    @Test
    void balancedReducesLoadAndNeverEmits() {
        // N=128, target 24: BALANCED accepts 26 (128/26=4 < 128/24=5), EVENLY_SPREAD snaps to 32.
        assertThat(BuiltInAlignmentMode.BALANCED.align(ctx(16, 24, 0, 128))).isEqualTo(26);
        assertThat(BuiltInAlignmentMode.EVENLY_SPREAD.align(ctx(16, 24, 0, 128))).isEqualTo(32);
        assertThat(emitted).hasValue(0);
    }

    @Test
    void offReturnsComputedTarget() {
        assertThat(BuiltInAlignmentMode.OFF.align(ctx(16, 24, 0, 128))).isEqualTo(24);
    }

    @Test
    void builtInModesFallBackToTargetInsteadOfBlocking() {
        // current=22, target=25, N=35 source partitions, upper bound 30: no aligned value preserves
        // the scale-up. Built-in modes return the raw target (OFF fallback), and emit no event.
        AlignmentContext blocked = ctx(22, 25, 35, 128, 20, 30, List.of(ShipStrategy.HASH));
        assertThat(BuiltInAlignmentMode.EVENLY_SPREAD.align(blocked)).isEqualTo(25);
        assertThat(BuiltInAlignmentMode.BALANCED.align(blocked)).isEqualTo(25);
        assertThat(emitted).hasValue(0);
    }

    @Test
    @SuppressWarnings("deprecation")
    void builtInModesSearchOneRegionAndDoNotRelaxBelowTarget() {
        // N=12, scale-up to target 7 with the region capped at 8: no divisor in [7, 8]. The new
        // built-in modes keep the target (7), whereas the legacy modes relax below the target to
        // the nearest divisor 6.
        AlignmentContext c = ctx(5, 7, 12, 8);
        assertThat(BuiltInAlignmentMode.EVENLY_SPREAD.align(c)).isEqualTo(7);
        assertThat(KeyGroupOrPartitionsAdjustMode.EVENLY_SPREAD.align(c)).isEqualTo(6);
    }

    @Test
    @SuppressWarnings("deprecation")
    void legacyModesBlockAndEmit() {
        // The same scenario under the deprecated legacy mode blocks (returns current) and emits.
        AlignmentContext blocked = ctx(22, 25, 35, 128, 20, 30, List.of(ShipStrategy.HASH));
        assertThat(KeyGroupOrPartitionsAdjustMode.EVENLY_SPREAD.align(blocked)).isEqualTo(22);
        assertThat(emitted).hasValue(1);
    }

    @Test
    void modesDoNotApplyToNonSourceNonHashVertices() {
        AlignmentContext rebalance =
                ctx(16, 24, 0, 128, 1, Integer.MAX_VALUE, List.of(ShipStrategy.REBALANCE));
        assertThat(BuiltInAlignmentMode.BALANCED.isApplicable(rebalance)).isFalse();
        assertThat(BuiltInAlignmentMode.EVENLY_SPREAD.isApplicable(rebalance)).isFalse();
        assertThat(BuiltInAlignmentMode.OFF.isApplicable(rebalance)).isFalse();
        // A source / keyBy vertex is in scope.
        AlignmentContext hash = ctx(16, 24, 0, 128);
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
                AutoScalerOptions.alignmentModeClassOption("custom"),
                TestAlignmentMode.class.getName());
        AlignmentMode custom = new TestAlignmentMode();
        assertThat(new ParallelismAligner(List.of(custom)).resolve(conf)).isSameAs(custom);
    }

    @Test
    void resolveFallsBackWhenCustomModeMissing() {
        Configuration conf = new Configuration();
        conf.set(AutoScalerOptions.ALIGNMENT_MODE, "custom");
        conf.set(AutoScalerOptions.alignmentModeClassOption("custom"), "com.example.Missing");
        assertThat(new ParallelismAligner(List.of()).resolve(conf))
                .isEqualTo(BuiltInAlignmentMode.BALANCED);
    }
}
