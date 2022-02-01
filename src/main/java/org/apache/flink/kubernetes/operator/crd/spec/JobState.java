package org.apache.flink.kubernetes.operator.crd.spec;

import com.fasterxml.jackson.annotation.JsonProperty;

/** Enum describing the desired job state. */
public enum JobState {
    @JsonProperty("running")
    RUNNING,
    @JsonProperty("suspended")
    SUSPENDED
}
