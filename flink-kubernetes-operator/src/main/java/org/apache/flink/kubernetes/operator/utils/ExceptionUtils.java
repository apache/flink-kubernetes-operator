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

    /**
     * Based on the flink ExceptionUtils#findThrowableSerializedAware but fixes an infinite loop bug
     * resulting from SerializedThrowable deserialization errors.
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
}
