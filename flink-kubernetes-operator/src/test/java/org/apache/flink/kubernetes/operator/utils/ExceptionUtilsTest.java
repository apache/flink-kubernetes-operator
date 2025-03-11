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

import org.apache.flink.util.SerializedThrowable;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link ExceptionUtils}. */
public class ExceptionUtilsTest {

    @Test
    void testGetExceptionMessage_nullThrowable() {
        assertThat(ExceptionUtils.getExceptionMessage(null)).isNull();
    }

    @Test
    void testGetExceptionMessage_simpleException() {
        var ex = new RuntimeException("My Exception");
        assertThat(ExceptionUtils.getExceptionMessage(ex)).isEqualTo("My Exception");
    }

    @Test
    void testGetExceptionMessage_simpleExceptionNullCause() {
        var ex = new OutOfMemoryError(null);
        assertThat(ExceptionUtils.getExceptionMessage(ex)).isEqualTo("OutOfMemoryError");
    }

    @Test
    void testGetExceptionMessage_exceptionWithCause() {
        var ex = new RuntimeException("Root Cause");
        var exWrapped = new RuntimeException("Wrapped Exception", ex);
        assertThat(ExceptionUtils.getExceptionMessage(exWrapped))
                .isEqualTo("Wrapped Exception -> Root Cause");
    }

    @Test
    void testGetExceptionMessage_exactlyThree() {
        var ex3 = new IllegalAccessException(null);
        var ex2 = new RuntimeException("Cause 2", ex3);
        var ex = new RuntimeException("Cause 1", ex2);
        assertThat(ExceptionUtils.getExceptionMessage(ex))
                .isEqualTo("Cause 1 -> Cause 2 -> IllegalAccessException");
    }

    @Test
    void testGetExceptionMessage_onlyTwo() {
        var ex2 = new RuntimeException("Cause 2");
        var ex = new RuntimeException("Cause 1", ex2);
        assertThat(ExceptionUtils.getExceptionMessage(ex)).isEqualTo("Cause 1 -> Cause 2");
    }

    @Test
    void testGetExceptionMessage_moreThanThree() {
        var ex4 = new RuntimeException("Cause 4");
        var ex3 = new RuntimeException("Cause 3", ex4);
        var ex2 = new IllegalStateException(null, ex3);
        var ex = new RuntimeException("Cause 1", ex2);
        assertThat(ExceptionUtils.getExceptionMessage(ex))
                .isEqualTo("Cause 1 -> IllegalStateException -> Cause 3");
    }

    @Test
    void testGetExceptionMessage_serializedThrowable() {
        var serializedException = new SerializedThrowable(new Exception("Serialized Exception"));
        assertThat(ExceptionUtils.getExceptionMessage(serializedException))
                .isEqualTo("Serialized Exception");
    }

    @Test
    void testGetExceptionMessage_serializedThrowableWithDoubleSerializedException() {
        var firstSerialized = new SerializedThrowable(new IndexOutOfBoundsException("4>3"));
        var serializedException = new SerializedThrowable(firstSerialized);
        assertThat(ExceptionUtils.getExceptionMessage(serializedException)).isEqualTo("4>3");
    }

    @Test
    void testGetExceptionMessage_serializedThrowableWithRegularException() {
        var serializedException = new SerializedThrowable(new Exception("Serialized Exception"));
        var ex = new RuntimeException("Cause 1", serializedException);
        assertThat(ExceptionUtils.getExceptionMessage(ex))
                .isEqualTo("Cause 1 -> Serialized Exception");
    }

    @Test
    void testGetExceptionMessage_serializedThrowableAndAnotherCause() {
        var ex = new RuntimeException("Cause 1");
        var serializedException =
                new SerializedThrowable(new RuntimeException("Serialized Exception", ex));
        assertThat(ExceptionUtils.getExceptionMessage(serializedException))
                .isEqualTo("Serialized Exception -> Cause 1");
    }

    @Test
    void testGetExceptionMessage_moreThanThreeWithSerializedAnnRegularOnes() {
        var ex4 = new RuntimeException("Cause 4");
        var ex3 = new RuntimeException("Cause 3", ex4);
        var ex2 = new RuntimeException("Cause 2", new SerializedThrowable(ex3));
        var ex = new RuntimeException("Cause 1", ex2);
        assertThat(ExceptionUtils.getExceptionMessage(ex))
                .isEqualTo("Cause 1 -> Cause 2 -> Cause 3");
    }

    @Test
    void testGetExceptionMessage_moreThanThreeWithSerializedAndNotWithDifferentOrdering() {
        var ex4 = new RuntimeException("Cause 4");
        var ex3 = new RuntimeException("Cause 3", ex4);
        var ex2 = new RuntimeException("Cause 2", new SerializedThrowable(ex3));
        var ex = new RuntimeException("Cause 1", new SerializedThrowable(ex2));
        assertThat(ExceptionUtils.getExceptionMessage(ex))
                .isEqualTo("Cause 1 -> Cause 2 -> Cause 3");
    }
}
