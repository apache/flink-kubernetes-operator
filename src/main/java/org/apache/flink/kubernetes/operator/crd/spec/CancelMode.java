package org.apache.flink.kubernetes.operator.crd.spec;

import com.fasterxml.jackson.annotation.JsonProperty;

public enum CancelMode {
    @JsonProperty("savepoint")
    SAVEPOINT,
    @JsonProperty("none")
    NONE
}
