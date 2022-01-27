package org.apache.flink.kubernetes.operator.crd.status;

import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
public class FlinkDeploymentStatus {
    private JobStatus[] jobStatuses;
}
