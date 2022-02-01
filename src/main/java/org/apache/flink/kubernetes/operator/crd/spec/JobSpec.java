package org.apache.flink.kubernetes.operator.crd.spec;

import lombok.Data;
import lombok.NoArgsConstructor;

/** Flink job spec. */
@Data
@NoArgsConstructor
public class JobSpec {
    private String jarURI;
    private int parallelism;
    private String entryClass;
    private String[] args = new String[0];
    private UpgradeMode upgradeMode = UpgradeMode.STATELESS;
    private JobState state = JobState.RUNNING;
}
