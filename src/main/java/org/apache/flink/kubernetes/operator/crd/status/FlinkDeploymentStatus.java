package org.apache.flink.kubernetes.operator.crd.status;

import org.apache.flink.kubernetes.operator.crd.spec.FlinkDeploymentSpec;

import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

/** Current status of the Flink deployment. */
@Data
@NoArgsConstructor
public class FlinkDeploymentStatus {
    private List<JobStatus> jobStatuses;
    private FlinkDeploymentSpec spec;
}
