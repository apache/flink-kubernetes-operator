package org.apache.flink.kubernetes.operator.crd;

import org.apache.flink.kubernetes.operator.crd.spec.FlinkDeploymentSpec;
import org.apache.flink.kubernetes.operator.crd.status.FlinkDeploymentStatus;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import io.fabric8.kubernetes.api.model.Namespaced;
import io.fabric8.kubernetes.client.CustomResource;
import io.fabric8.kubernetes.model.annotation.Group;
import io.fabric8.kubernetes.model.annotation.Version;

/** Flink deployment object (spec + status). */
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonDeserialize()
@Group("flink.io")
@Version("v1alpha1")
public class FlinkDeployment extends CustomResource<FlinkDeploymentSpec, FlinkDeploymentStatus>
        implements Namespaced {}
