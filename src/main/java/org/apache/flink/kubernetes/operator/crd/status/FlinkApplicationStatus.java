package org.apache.flink.kubernetes.operator.crd.status;

import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
public class FlinkApplicationStatus  {
    private JobStatus[] jobStatuses;
}
