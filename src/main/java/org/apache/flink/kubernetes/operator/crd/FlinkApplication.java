package org.apache.flink.kubernetes.operator.crd;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import io.fabric8.kubernetes.api.model.Namespaced;
import io.fabric8.kubernetes.client.CustomResource;
import io.fabric8.kubernetes.model.annotation.Group;
import io.fabric8.kubernetes.model.annotation.Version;
import org.apache.flink.kubernetes.operator.crd.spec.FlinkApplicationSpec;
import org.apache.flink.kubernetes.operator.crd.status.FlinkApplicationStatus;

@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonDeserialize()
@Group("flink.io")
@Version("v1alpha1")
public class FlinkApplication extends CustomResource<FlinkApplicationSpec, FlinkApplicationStatus> implements Namespaced {
}
