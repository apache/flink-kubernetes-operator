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

import java.io.IOException;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link ExceptionUtils}. */
public class ExceptionUtilsTest {

    @Test
    void testGetExceptionMessage_nullThrowable() {
        assertThat(ExceptionUtils.getExceptionMessage(null)).isNull();
    }

    @Test
    void testGetExceptionMessage_serializedThrowable() {
        var serializedException = new SerializedThrowable(new Exception("Serialized Exception"));
        assertThat(ExceptionUtils.getExceptionMessage(serializedException))
                .isEqualTo("Serialized Exception");
    }

    @Test
    void testGetExceptionMessage_differentKindsOfExceptions() {
        var ex4 = new RuntimeException("Cause 4");
        var ex3 = new RuntimeException("Cause 3", ex4);
        var ex2 = new RuntimeException("Cause 2", new SerializedThrowable(ex3));
        var ex = new RuntimeException("Cause 1", ex2);
        assertThat(ExceptionUtils.getExceptionMessage(ex))
                .isEqualTo("Cause 1 -> Cause 2 -> Cause 3");
    }

    @Test
    void testSerializedThrowableError() {
        assertThat(
                        ExceptionUtils.getExceptionMessage(
                                new SerializedThrowable(new NonSerializableException("Message"))))
                .isEqualTo(String.format("%s: Message", NonSerializableException.class.getName()));

        assertThat(
                        ExceptionUtils.getExceptionMessage(
                                new SerializedThrowable(new NonSerializableException())))
                .isEqualTo(NonSerializableException.class.getName());
    }

    private static class NonSerializableException extends Exception {

        public NonSerializableException(String message) {
            super(message);
        }

        public NonSerializableException() {}

        private void writeObject(java.io.ObjectOutputStream stream) throws IOException {
            throw new IOException();
        }
    }
}
