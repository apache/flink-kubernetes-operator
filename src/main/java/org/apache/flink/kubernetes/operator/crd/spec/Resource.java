package org.apache.flink.kubernetes.operator.crd.spec;

import io.fabric8.kubernetes.api.model.KubernetesResource;
import lombok.*;

@Data
@NoArgsConstructor
public class Resource implements KubernetesResource {
    private double cpu;
    // 1024m, 1g
    private String mem;
}
