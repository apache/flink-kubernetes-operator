package org.apache.flink.kubernetes.operator.crd.spec;

import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
public class JobSpec {
    private String jarURI;
    private int parallelism;
    private String entryClass;
    private String[] args = new String[0];
    private RestoreMode restoreMode;
    private CancelMode cancelMode;
}

