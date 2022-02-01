package org.apache.flink.kubernetes.operator.crd.spec;

import lombok.Data;
import lombok.NoArgsConstructor;

/** Resource spec. */
@Data
@NoArgsConstructor
public class Resource {
    private double cpu;
    // 1024m, 1g
    private String memory;
}
