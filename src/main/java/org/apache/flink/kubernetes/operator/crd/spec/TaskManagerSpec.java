package org.apache.flink.kubernetes.operator.crd.spec;

import io.fabric8.kubernetes.api.model.Pod;
import lombok.Data;
import lombok.NoArgsConstructor;

/** TaskManager spec. */
@Data
@NoArgsConstructor
public class TaskManagerSpec {
    private int taskSlots;
    private Resource resource;
    private Pod podTemplate;
}
