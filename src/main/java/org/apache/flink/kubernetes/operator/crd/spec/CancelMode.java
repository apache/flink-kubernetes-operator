package org.apache.flink.kubernetes.operator.crd.spec;

import com.fasterxml.jackson.annotation.JsonProperty;

/** Enum to control Flink job cancel behavior. */
public enum CancelMode {
    @JsonProperty("savepoint")
    SAVEPOINT,
    @JsonProperty("none")
    NONE
}
