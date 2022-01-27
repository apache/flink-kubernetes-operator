package org.apache.flink.kubernetes.operator.crd.spec;

import com.fasterxml.jackson.annotation.JsonProperty;

public enum RestoreMode {
    @JsonProperty("savepoint")
    SAVEPOINT,
    @JsonProperty("last-state")
    LAST_STATE,
    @JsonProperty("none")
    NONE
}
