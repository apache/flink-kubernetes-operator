/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.kubernetes.operator.api.utils;

import org.apache.flink.kubernetes.operator.api.AbstractFlinkResource;
import org.apache.flink.kubernetes.operator.api.reconciler.ReconciliationMetadata;
import org.apache.flink.kubernetes.operator.api.spec.AbstractFlinkSpec;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import javax.annotation.Nullable;

/** Spec utilities. */
public class SpecUtils {
    public static final String INTERNAL_METADATA_JSON_KEY = "resource_metadata";
    private static final ObjectMapper objectMapper = new ObjectMapper();

    /**
     * Deserializes the spec and custom metadata object from JSON.
     *
     * @param specWithMetaString JSON string.
     * @param specClass Spec class for deserialization.
     * @param <T> Spec type.
     * @return SpecWithMeta of spec and meta.
     */
    public static <T extends AbstractFlinkSpec> SpecWithMeta<T> deserializeSpecWithMeta(
            @Nullable String specWithMetaString, Class<T> specClass) {
        if (specWithMetaString == null) {
            return null;
        }

        try {
            ObjectNode wrapper = (ObjectNode) objectMapper.readTree(specWithMetaString);
            ObjectNode internalMeta = (ObjectNode) wrapper.remove(INTERNAL_METADATA_JSON_KEY);
            if (internalMeta == null) {
                // migrating from old format
                wrapper.remove("apiVersion");
                return new SpecWithMeta<>(objectMapper.treeToValue(wrapper, specClass), null);
            } else {
                internalMeta.remove("metadata");
                return new SpecWithMeta<>(
                        objectMapper.treeToValue(wrapper.get("spec"), specClass),
                        objectMapper.convertValue(internalMeta, ReconciliationMetadata.class));
            }
        } catch (JsonProcessingException e) {
            throw new RuntimeException("Could not deserialize spec, this indicates a bug...", e);
        }
    }

    /**
     * Serializes the spec and custom meta information into a JSON string.
     *
     * @param spec Flink resource spec.
     * @param relatedResource Related Flink resource for creating the meta object.
     * @return Serialized json.
     */
    public static String writeSpecWithMeta(
            AbstractFlinkSpec spec, AbstractFlinkResource<?, ?> relatedResource) {
        return writeSpecWithMeta(spec, ReconciliationMetadata.from(relatedResource));
    }

    /**
     * Serializes the spec and custom meta information into a JSON string.
     *
     * @param spec Flink resource spec.
     * @param metadata Reconciliation meta object.
     * @return Serialized json.
     */
    public static String writeSpecWithMeta(
            AbstractFlinkSpec spec, ReconciliationMetadata metadata) {

        ObjectNode wrapper = objectMapper.createObjectNode();

        wrapper.set("spec", objectMapper.valueToTree(checkNotNull(spec)));
        wrapper.set(INTERNAL_METADATA_JSON_KEY, objectMapper.valueToTree(checkNotNull(metadata)));

        try {
            return objectMapper.writeValueAsString(wrapper);
        } catch (JsonProcessingException e) {
            throw new RuntimeException("Could not serialize spec, this indicates a bug...", e);
        }
    }

    public static String writeSpecAsJSON(Object object, String wrapperKey) {
        ObjectNode wrapper = objectMapper.createObjectNode();
        wrapper.set(wrapperKey, objectMapper.valueToTree(checkNotNull(object)));

        try {
            return objectMapper.writeValueAsString(wrapper);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(
                    "Could not serialize " + wrapperKey + ", this indicates a bug...", e);
        }
    }

    public static <T> T readSpecFromJSON(String serialized, String wrapperKey, Class<T> valueType) {
        try {
            ObjectNode wrapper = (ObjectNode) objectMapper.readTree(serialized);
            return objectMapper.treeToValue(wrapper.get(wrapperKey), valueType);
        } catch (JsonProcessingException e) {
            throw new RuntimeException("Could not deserialize spec, this indicates a bug...", e);
        }
    }

    // We do not have access to  Flink's Preconditions from here
    private static <T> T checkNotNull(T object) {
        if (object == null) {
            throw new NullPointerException();
        } else {
            return object;
        }
    }

    public static <T> T clone(T object) {
        if (object == null) {
            return null;
        }
        try {
            return (T)
                    objectMapper.readValue(
                            objectMapper.writeValueAsString(object), object.getClass());
        } catch (JsonProcessingException e) {
            throw new IllegalStateException(e);
        }
    }
}
