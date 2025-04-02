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

import org.apache.flink.types.DeserializationException;
import org.apache.flink.util.SerializedThrowable;

import java.util.Optional;

/** Exception utils. * */
public class ExceptionUtils {

    private static final int EXCEPTION_LIMIT_FOR_EVENT_MESSAGE = 3;

    /**
     * Based on the flink ExceptionUtils#findThrowableSerializedAware but fixes an infinite loop bug
     * resulting from SerializedThrowable deserialization errors.
     *
     * @param throwable the throwable to be processed
     * @param searchType the type of the exception to search for
     * @param classLoader the classloader to use for deserialization
     * @param <T> the exception type
     * @return the found exception, or empty if it is not found.
     */
    public static <T extends Throwable> Optional<T> findThrowableSerializedAware(
            Throwable throwable, Class<T> searchType, ClassLoader classLoader) {

        if (throwable == null || searchType == null) {
            return Optional.empty();
        }

        Throwable t = throwable;
        while (t != null) {
            if (searchType.isAssignableFrom(t.getClass())) {
                return Optional.of(searchType.cast(t));
            } else if (t instanceof SerializedThrowable) {
                var deserialized = ((SerializedThrowable) t).deserializeError(classLoader);
                // This is the key part of the fix:
                // The deserializeError method returns the same SerializedThrowable if it cannot
                // deserialize it. Previously this is what caused the infinite loop.
                t =
                        deserialized == t
                                ? new DeserializationException(
                                        "Could not deserialize SerializedThrowable")
                                : deserialized;
            } else {
                t = t.getCause();
            }
        }

        return Optional.empty();
    }

    /**
     * traverse the throwable and extract useful information for up to the first 3 possible
     * exceptions in the hierarchy.
     *
     * @param throwable the throwable to be processed
     * @return the exception message, which will have a format similar to "cause1 &rarr; cause2
     *     &rarr; cause3"
     */
    public static String getExceptionMessage(Throwable throwable) {
        return getExceptionMessage(throwable, 1);
    }

    /**
     * Helper for recursion for `getExceptionMessage`.
     *
     * @param throwable the throwable to be processed
     * @param level the level we are in. The caller will set this value to 0, and we will be
     *     incrementing it with each recursive call
     * @return the exception message, which will have a format similar to "cause1 -> cause2 ->
     *     cause3"
     */
    private static String getExceptionMessage(Throwable throwable, int level) {
        if (throwable == null) {
            return null;
        }

        if (throwable instanceof SerializedThrowable) {
            var serialized = ((SerializedThrowable) throwable);
            var deserialized =
                    serialized.deserializeError(Thread.currentThread().getContextClassLoader());
            if (deserialized == throwable) {
                var msg = serialized.getMessage();
                return msg != null ? msg : serialized.getOriginalErrorClassName();
            } else {
                return getExceptionMessage(deserialized, level);
            }
        }

        var msg =
                Optional.ofNullable(throwable.getMessage())
                        .orElse(throwable.getClass().getSimpleName());

        if (level >= EXCEPTION_LIMIT_FOR_EVENT_MESSAGE) {
            return msg;
        }

        if (throwable.getCause() == null) {
            return msg;
        } else {
            return msg + " -> " + getExceptionMessage(throwable.getCause(), level + 1);
        }
    }
}
