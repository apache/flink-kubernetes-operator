package org.apache.flink.kubernetes.operator.crd.spec;

import com.fasterxml.jackson.annotation.JsonProperty;

/** Enum to control Flink job restore behavior on upgrade/start. */
public enum UpgradeMode {
    @JsonProperty("savepoint")
    SAVEPOINT,
    @JsonProperty("last-state")
    LAST_STATE,
    @JsonProperty("stateless")
    STATELESS
}
