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

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Built-in {@link WatermarkExtractor} that reads an epoch-ms {@code long} from a POJO using a
 * dot-notation field path (e.g. {@code "eventTime"} or {@code "metadata.timestamp"}).
 *
 * <p>At each path segment the extractor first attempts a public field access, then walks the class
 * hierarchy for non-public fields, then falls back to a no-arg getter following the {@code getXxx}
 * convention. The value at the final segment must be assignable to {@link Long}.
 *
 * <p>Configure via {@code bluegreen.gate.watermark.field-path} — no application code required. Use
 * {@code bluegreen.gate.watermark.extractor-class} only when the watermark cannot be expressed as a
 * simple field path.
 *
 * <p>Resolution metadata (the {@link Field} / {@link Method} chain to invoke) is cached per record
 * class on first call. Subsequent calls reuse the cached chain — no per-record reflective lookup,
 * no string parsing, no exception-based control flow. Multi-segment paths resolve nested segments
 * against the <em>declared</em> type of the prior segment, which is correct for monomorphic nested
 * types (the typical case, including all protobuf-generated classes). For polymorphic intermediate
 * types where a single field may hold instances of different runtime classes, prefer {@code
 * extractor-class}.
 */
public class FieldPathWatermarkExtractor implements WatermarkExtractor<Object> {

    private static final long serialVersionUID = 2L;

    private final String fieldPath;
    private final String[] segments;

    // Inline cache for the common monomorphic case (single record class flowing through).
    // volatile because the operator may be touched from threads other than the per-record
    // worker during init/teardown, even though processElement itself is single-threaded.
    // All cache fields are transient — reconstructed lazily on TaskManager after deserialization.
    private transient volatile Class<?> hotClass;
    private transient volatile Accessor[] hotAccessors;

    // Overflow cache for polymorphic streams (rare in practice).
    private transient volatile ConcurrentHashMap<Class<?>, Accessor[]> overflow;

    public FieldPathWatermarkExtractor(String fieldPath) {
        if (fieldPath == null || fieldPath.isBlank()) {
            throw new IllegalArgumentException(
                    "bluegreen.gate.watermark.field-path must not be blank");
        }
        this.fieldPath = fieldPath;
        this.segments = fieldPath.split("\\.");
    }

    @Override
    public Long apply(Object record) {
        if (record == null) {
            return Long.MIN_VALUE;
        }

        Class<?> cls = record.getClass();
        Accessor[] accessors;
        if (cls == hotClass) {
            accessors = hotAccessors;
        } else {
            accessors = lookupOrResolve(cls);
            // Update inline cache. Benign race under the single-threaded
            // processElement contract; if multiple init threads were to
            // race here, each would write a (cls, accessors) pair that is
            // a valid resolution for cls, so the cache is always consistent.
            hotClass = cls;
            hotAccessors = accessors;
        }

        Object current = record;
        for (Accessor a : accessors) {
            current = a.get(current);
            if (current == null) {
                return Long.MIN_VALUE;
            }
        }
        if (!(current instanceof Long)) {
            throw new IllegalStateException(
                    "Field path '"
                            + fieldPath
                            + "' resolved to "
                            + current.getClass().getName()
                            + ", expected Long");
        }
        return (Long) current;
    }

    private Accessor[] lookupOrResolve(Class<?> cls) {
        ConcurrentHashMap<Class<?>, Accessor[]> map = overflow;
        if (map == null) {
            // Lazy init — only allocated the first time we resolve.
            // Benign double-init under contention; both maps are functionally
            // equivalent and the loser's cached entries are simply discarded.
            map = new ConcurrentHashMap<>();
            overflow = map;
        }
        return map.computeIfAbsent(cls, c -> resolveChain(c, segments, fieldPath));
    }

    /**
     * Resolves each path segment against the declared type produced by the previous segment. Done
     * once per record class; result is cached and reused for every subsequent record of that class.
     */
    private static Accessor[] resolveChain(Class<?> rootCls, String[] segments, String fieldPath) {
        Accessor[] chain = new Accessor[segments.length];
        Class<?> currentCls = rootCls;
        for (int i = 0; i < segments.length; i++) {
            Accessor a = resolveSegment(currentCls, segments[i]);
            chain[i] = a;
            currentCls = a.declaredType();
        }
        return chain;
    }

    private static Accessor resolveSegment(Class<?> cls, String name) {
        // Public field
        try {
            return new FieldAccessor(cls.getField(name));
        } catch (NoSuchFieldException ignored) {
        }

        // Non-public field — walk hierarchy
        for (Class<?> c = cls; c != null; c = c.getSuperclass()) {
            try {
                Field f = c.getDeclaredField(name);
                f.setAccessible(true);
                return new FieldAccessor(f);
            } catch (NoSuchFieldException ignored) {
            }
        }

        // getXxx() getter
        String getter = "get" + Character.toUpperCase(name.charAt(0)) + name.substring(1);
        try {
            return new MethodAccessor(cls.getMethod(getter));
        } catch (NoSuchMethodException ignored) {
        }

        throw new IllegalArgumentException(
                "No accessible field or getter '" + name + "' found on " + cls.getName());
    }

    /** Resolved per-segment accessor used on the per-record hot path. */
    private interface Accessor {
        Object get(Object target);

        Class<?> declaredType();
    }

    private static final class FieldAccessor implements Accessor {
        private final Field field;

        FieldAccessor(Field field) {
            this.field = field;
        }

        @Override
        public Object get(Object target) {
            try {
                return field.get(target);
            } catch (IllegalAccessException e) {
                throw new IllegalStateException("Cannot access " + field, e);
            }
        }

        @Override
        public Class<?> declaredType() {
            return field.getType();
        }
    }

    private static final class MethodAccessor implements Accessor {
        private final Method method;

        MethodAccessor(Method method) {
            this.method = method;
        }

        @Override
        public Object get(Object target) {
            try {
                return method.invoke(target);
            } catch (IllegalAccessException e) {
                throw new IllegalStateException("Cannot access " + method, e);
            } catch (InvocationTargetException e) {
                Throwable cause = e.getCause();
                if (cause instanceof RuntimeException) {
                    throw (RuntimeException) cause;
                }
                throw new IllegalStateException("Failed to invoke " + method, cause);
            }
        }

        @Override
        public Class<?> declaredType() {
            return method.getReturnType();
        }
    }
}
