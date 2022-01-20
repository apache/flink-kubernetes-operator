package org.apache.flink.kubernetes.operator.crd.status;

import io.fabric8.kubernetes.api.model.KubernetesResource;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
public class JobStatus implements KubernetesResource {
    private String jobName;
    private String jobId;
    private String state;
    private String updateTime;
    private String savepointLocation;
}
