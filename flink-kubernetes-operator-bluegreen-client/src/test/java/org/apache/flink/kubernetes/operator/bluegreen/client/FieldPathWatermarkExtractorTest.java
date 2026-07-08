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

package org.apache.flink.kubernetes.operator.bluegreen.client;

import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.util.concurrent.ConcurrentHashMap;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Tests for {@link FieldPathWatermarkExtractor}. */
class FieldPathWatermarkExtractorTest {

    // ---- Constructor validation ----

    @Test
    void constructor_rejectsNullPath() {
        assertThrows(IllegalArgumentException.class, () -> new FieldPathWatermarkExtractor(null));
    }

    @Test
    void constructor_rejectsBlankPath() {
        assertThrows(IllegalArgumentException.class, () -> new FieldPathWatermarkExtractor(""));
        assertThrows(IllegalArgumentException.class, () -> new FieldPathWatermarkExtractor("   "));
    }

    // ---- Single-segment resolution ----

    @Test
    void apply_resolvesPublicField() {
        var extractor = new FieldPathWatermarkExtractor("publicWm");
        assertEquals(123L, extractor.apply(new RecordWithPublicField(123L)));
    }

    @Test
    void apply_resolvesPrivateField() {
        // Probe's case: Event has a private final long eventWatermark.
        var extractor = new FieldPathWatermarkExtractor("privateWm");
        assertEquals(456L, extractor.apply(new RecordWithPrivateField(456L)));
    }

    @Test
    void apply_resolvesInheritedPrivateField() {
        var extractor = new FieldPathWatermarkExtractor("privateWm");
        assertEquals(789L, extractor.apply(new SubclassOfPrivateRecord(789L)));
    }

    @Test
    void apply_resolvesGetter() {
        var extractor = new FieldPathWatermarkExtractor("watermark");
        assertEquals(321L, extractor.apply(new RecordWithGetter(321L)));
    }

    // ---- Multi-segment resolution ----

    @Test
    void apply_resolvesNestedPathViaFields() {
        var record = new RecordWithNestedField(new InnerWithPublicField(42L));
        var extractor = new FieldPathWatermarkExtractor("inner.publicWm");
        assertEquals(42L, extractor.apply(record));
    }

    @Test
    void apply_resolvesDeeplyNestedPath() {
        var record = new Outer(new Mid(new InnerWithPublicField(99L)));
        var extractor = new FieldPathWatermarkExtractor("mid.inner.publicWm");
        assertEquals(99L, extractor.apply(record));
    }

    @Test
    void apply_resolvesNestedPathViaGetters() {
        var record = new RecordWithNestedGetter(new InnerWithGetter(777L));
        var extractor = new FieldPathWatermarkExtractor("nested.watermark");
        assertEquals(777L, extractor.apply(record));
    }

    // ---- Null handling ----

    @Test
    void apply_returnsMinValueOnNullRecord() {
        var extractor = new FieldPathWatermarkExtractor("publicWm");
        assertEquals(Long.MIN_VALUE, extractor.apply(null));
    }

    @Test
    void apply_returnsMinValueOnNullIntermediateSegment() {
        var record = new RecordWithNestedField(null);
        var extractor = new FieldPathWatermarkExtractor("inner.publicWm");
        assertEquals(Long.MIN_VALUE, extractor.apply(record));
    }

    // ---- Type / resolution failures ----

    @Test
    void apply_throwsWhenFinalSegmentIsNotLong() {
        var extractor = new FieldPathWatermarkExtractor("name");
        assertThrows(
                IllegalStateException.class,
                () -> extractor.apply(new RecordWithStringField("hello")));
    }

    @Test
    void apply_throwsWhenSegmentNotFound() {
        var extractor = new FieldPathWatermarkExtractor("doesNotExist");
        assertThrows(
                IllegalArgumentException.class,
                () -> extractor.apply(new RecordWithPublicField(1L)));
    }

    // ---- Cache behavior (correctness, not perf) ----

    @Test
    void apply_returnsConsistentValuesAcrossManyInvocations() {
        // Sanity check that the cache doesn't accidentally pin a stale value
        // to a record class — values must come fresh from each record.
        var extractor = new FieldPathWatermarkExtractor("privateWm");
        for (long v = 0; v < 1000; v++) {
            assertEquals(v, extractor.apply(new RecordWithPrivateField(v)));
        }
    }

    @Test
    void apply_handlesPolymorphicRecordClasses() {
        // Two distinct concrete classes, each with their own privateWm field,
        // must both resolve correctly via the overflow cache.
        var extractor = new FieldPathWatermarkExtractor("privateWm");
        assertEquals(10L, extractor.apply(new RecordWithPrivateField(10L)));
        assertEquals(20L, extractor.apply(new SubclassOfPrivateRecord(20L)));
        assertEquals(11L, extractor.apply(new RecordWithPrivateField(11L)));
        assertEquals(21L, extractor.apply(new SubclassOfPrivateRecord(21L)));
    }

    @Test
    void apply_publicFieldExtractorUnaffectedByUnrelatedRecordType() {
        // The hot-class inline cache must not bleed across record types with
        // a same-named field of different visibility.
        var extractor = new FieldPathWatermarkExtractor("publicWm");
        assertEquals(1L, extractor.apply(new RecordWithPublicField(1L)));
        // Different concrete class with same path semantics.
        assertEquals(2L, extractor.apply(new AnotherRecordWithPublicField(2L)));
        assertEquals(3L, extractor.apply(new RecordWithPublicField(3L)));
    }

    // ---- Cache structural correctness ----
    //
    // These tests verify that the cache prevents re-resolution on subsequent
    // calls for the same record class. They inspect the extractor's private
    // overflow map directly — a deterministic check that doesn't depend on
    // wall-clock timing.

    @Test
    void cache_singleClassPopulatesExactlyOneEntry() throws Exception {
        var extractor = new FieldPathWatermarkExtractor("privateWm");
        // Cache should be empty before any apply()
        assertEquals(0, overflowSize(extractor));

        for (int i = 0; i < 1000; i++) {
            extractor.apply(new RecordWithPrivateField(i));
        }
        // 1000 calls, same record class → exactly one cache entry
        assertEquals(1, overflowSize(extractor));
    }

    @Test
    void cache_polymorphicStreamPopulatesOneEntryPerConcreteClass() throws Exception {
        var extractor = new FieldPathWatermarkExtractor("privateWm");

        // 100 calls each across two concrete classes
        for (int i = 0; i < 100; i++) {
            extractor.apply(new RecordWithPrivateField(i));
            extractor.apply(new SubclassOfPrivateRecord(i + 1000L));
        }
        assertEquals(2, overflowSize(extractor));
    }

    @Test
    void cache_returnsSameAccessorChainForRepeatedCalls() throws Exception {
        // The cached Accessor[] reference must be reused across calls — not
        // rebuilt. We can't compare values directly (Accessor is private) but
        // we can verify the map's value is the same Object reference between
        // a first call and the 100th call.
        var extractor = new FieldPathWatermarkExtractor("privateWm");

        extractor.apply(new RecordWithPrivateField(0L));
        Object firstCallChain = overflowMap(extractor).get(RecordWithPrivateField.class);
        assertNotNull(firstCallChain);

        for (int i = 0; i < 100; i++) {
            extractor.apply(new RecordWithPrivateField(i));
        }
        Object hundredthCallChain = overflowMap(extractor).get(RecordWithPrivateField.class);
        // Reference equality — proves the cache returned the same array, not a fresh resolution
        assertSame(firstCallChain, hundredthCallChain);
    }

    @Test
    void cache_multiSegmentPathPopulatesOneEntryPerOuterClass() throws Exception {
        // The cache key is the OUTER record class, even for nested paths —
        // the chain of accessors is stored as the value, not as separate entries.
        var extractor = new FieldPathWatermarkExtractor("inner.publicWm");

        for (int i = 0; i < 500; i++) {
            extractor.apply(new RecordWithNestedField(new InnerWithPublicField(i)));
        }
        assertEquals(1, overflowSize(extractor));
    }

    @Test
    void cache_loosenedTimingCheck_warmCallsFasterThanCold() {
        // Loose wall-clock sanity check. Not a precise benchmark — JMH would be the
        // proper tool for nanosecond-level measurements — but sufficient as a
        // regression guard against accidentally disabling the cache. Threshold is
        // intentionally generous (warm ≥ 2× faster than the cold first call) so
        // CI noise doesn't cause flakes.
        var extractor = new FieldPathWatermarkExtractor("privateWm");
        var record = new RecordWithPrivateField(42L);

        // Cold: first call includes resolution
        long coldStart = System.nanoTime();
        extractor.apply(record);
        long coldNs = System.nanoTime() - coldStart;

        // Warm-up the JIT so the timing of the warm phase isn't dominated by
        // interpreter / C1 startup.
        for (int i = 0; i < 10_000; i++) {
            extractor.apply(record);
        }

        // Measure warm path
        int warmIterations = 100_000;
        long warmStart = System.nanoTime();
        for (int i = 0; i < warmIterations; i++) {
            extractor.apply(record);
        }
        long warmAvgNs = (System.nanoTime() - warmStart) / warmIterations;

        // Cold call should be at least 2× the average warm call. This is a very
        // loose check; with the cache in place the actual ratio is typically
        // 10×–50×, but JIT noise on a contested CI host can compress that.
        assertTrue(
                coldNs > warmAvgNs * 2,
                () ->
                        String.format(
                                "Expected cold call (%d ns) to be ≥2× the warm average (%d ns).",
                                coldNs, warmAvgNs));
    }

    // ---- Reflective access to the internal cache for the structural tests ----
    //
    // The overflow ConcurrentHashMap is a private transient field. Reflecting
    // into it from the test (same package) keeps the production class free of
    // testing-only accessors.

    @SuppressWarnings("unchecked")
    private static ConcurrentHashMap<Class<?>, ?> overflowMap(FieldPathWatermarkExtractor extractor)
            throws Exception {
        Field f = FieldPathWatermarkExtractor.class.getDeclaredField("overflow");
        f.setAccessible(true);
        var map = (ConcurrentHashMap<Class<?>, ?>) f.get(extractor);
        // Lazily initialized inside apply(); for a brand-new extractor before
        // any apply() it's null. Treat that as "empty" for these tests.
        return map == null ? new ConcurrentHashMap<>() : map;
    }

    private static int overflowSize(FieldPathWatermarkExtractor extractor) throws Exception {
        return overflowMap(extractor).size();
    }

    // ---- Test record types ----

    static class RecordWithPublicField {
        public long publicWm;

        RecordWithPublicField(long v) {
            this.publicWm = v;
        }
    }

    static class AnotherRecordWithPublicField {
        public long publicWm;

        AnotherRecordWithPublicField(long v) {
            this.publicWm = v;
        }
    }

    static class RecordWithPrivateField {
        private final long privateWm;

        RecordWithPrivateField(long v) {
            this.privateWm = v;
        }
    }

    static class SubclassOfPrivateRecord extends RecordWithPrivateField {
        SubclassOfPrivateRecord(long v) {
            super(v);
        }
    }

    static class RecordWithGetter {
        private final long wm;

        RecordWithGetter(long v) {
            this.wm = v;
        }

        public long getWatermark() {
            return wm;
        }
    }

    static class InnerWithPublicField {
        public long publicWm;

        InnerWithPublicField(long v) {
            this.publicWm = v;
        }
    }

    static class RecordWithNestedField {
        public InnerWithPublicField inner;

        RecordWithNestedField(InnerWithPublicField inner) {
            this.inner = inner;
        }
    }

    static class Mid {
        public InnerWithPublicField inner;

        Mid(InnerWithPublicField inner) {
            this.inner = inner;
        }
    }

    static class Outer {
        public Mid mid;

        Outer(Mid mid) {
            this.mid = mid;
        }
    }

    static class InnerWithGetter {
        private final long wm;

        InnerWithGetter(long v) {
            this.wm = v;
        }

        public long getWatermark() {
            return wm;
        }
    }

    static class RecordWithNestedGetter {
        private final InnerWithGetter nested;

        RecordWithNestedGetter(InnerWithGetter nested) {
            this.nested = nested;
        }

        public InnerWithGetter getNested() {
            return nested;
        }
    }

    static class RecordWithStringField {
        public String name;

        RecordWithStringField(String name) {
            this.name = name;
        }
    }
}
