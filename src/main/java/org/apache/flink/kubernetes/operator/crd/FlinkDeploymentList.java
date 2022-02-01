package org.apache.flink.kubernetes.operator.crd;

import io.fabric8.kubernetes.client.CustomResourceList;

/** Multiple Flink deployments. */
public class FlinkDeploymentList extends CustomResourceList<FlinkDeployment> {}
