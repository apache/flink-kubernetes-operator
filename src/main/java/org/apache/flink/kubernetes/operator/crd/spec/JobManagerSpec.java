package org.apache.flink.kubernetes.operator.crd.spec;

import io.fabric8.kubernetes.api.model.Pod;
import lombok.Data;
import lombok.NoArgsConstructor;

/** JobManager spec. */
@Data
@NoArgsConstructor
public class JobManagerSpec {
    private Resource resource;
    private int replicas;
    private Pod podTemplate;
}
