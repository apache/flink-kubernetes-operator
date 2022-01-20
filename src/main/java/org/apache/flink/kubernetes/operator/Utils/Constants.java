package org.apache.flink.kubernetes.operator.Utils;

public class Constants {
	public static final String FLINK_NATIVE_K8S_OPERATOR_NAME = "flink-operator";
	public static final String KUBERNETES_APP_TARGET = "kubernetes-application";

	public static final String REST_SVC_NAME_SUFFIX = "-rest";

	public static final String INGRESS_API_VERSION = "networking.k8s.io/v1";
	public static final String INGRESS_SUFFIX = ".flink.io";
}
