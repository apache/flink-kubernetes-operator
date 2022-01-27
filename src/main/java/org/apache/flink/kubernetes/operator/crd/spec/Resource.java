package org.apache.flink.kubernetes.operator.crd.spec;

import io.fabric8.kubernetes.api.model.KubernetesResource;
import lombok.*;

@Data
@NoArgsConstructor
public class Resource {
    private double cpu;
    // 1024m, 1g
    private String memory;
}
