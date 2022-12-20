#!/usr/bin/env bash
################################################################################
#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
# limitations under the License.
################################################################################
#
# Generates OLM bundle using existing CRDs
#
#
set -euox pipefail

. env.sh
BUNDLE_VERSION=${BUNDLE_VERSION:-1.2.0}
PREVIOUS_BUNDLE_VERSION=${PREVIOUS_BUNDLE_VERSION:-1.1.0}
BASEDIR="$(dirname "$(realpath "$0")")"
TARGET="${BASEDIR}/target"
BUNDLE="${TARGET}/${BUNDLE_VERSION}"
OPERATOR_IMG=${OPERATOR_IMG:-"docker.io/apache/flink-kubernetes-operator:1.2.0"}

CHANNELS=alpha
PACKAGE_NAME=flink-kubernetes-operator
CSV_TEMPLATE_DIR="$(dirname "$(realpath "$0")")/csv-template"
# Base ClusterServiceVersion resource containing static information
CSV_TEMPLATE=${CSV_TEMPLATE_DIR}/bases/${PACKAGE_NAME}.clusterserviceversion.yaml
INIT_CONT=${CSV_TEMPLATE_DIR}/bases/initContainer.yaml
CRD_DIR="${BASEDIR}/helm/${PACKAGE_NAME}/crds"
MANIFESTS=${BUNDLE}/manifests
ROLE_BINDING=${MANIFESTS}/flink-role-binding_rbac.authorization.k8s.io_v1_rolebinding.yaml
CRDDIR="${TARGET}/crds"

# CSV name may contain versioned or constant
# CSV_FILE="${MANIFESTS}/${PACKAGE_NAME}.v${BUNDLE_VERSION}.clusterserviceversion.yaml"
CSV_FILE="${MANIFESTS}/${PACKAGE_NAME}.clusterserviceversion.yaml"

# Generates bundle from existing CRDs and Resources
generate_olm_bundle() {
  rm -rf "${BUNDLE}"
  rm -rf "${CRDDIR}"
  mkdir -p "${CRDDIR}"
  cp -R "${CRD_DIR}" "${CRDDIR}"

  helm template flink-kubernetes-operator helm/flink-kubernetes-operator > "$CRDDIR/template.yaml"

  operator-sdk generate bundle \
    --input-dir="$CRDDIR" \
	  --output-dir="$BUNDLE" \
	  --kustomize-dir="$CSV_TEMPLATE_DIR" \
	  --package="$PACKAGE_NAME" \
	  --version="$BUNDLE_VERSION" \
	  --channels="$CHANNELS"

  # Remove Dockerfile generated by operator-sdk
  rm ./bundle.Dockerfile
  cp "${CSV_TEMPLATE_DIR}/bundle.Dockerfile" "${BUNDLE}"

  # Remove extra files added by operator-sdk
  rm "${MANIFESTS}"/*webhook-service*.yaml

  # Update CSV filename to name traditionally used for OperatorHub.
  # Is this step necessary ?
  # mv "${MANIFESTS}"/*.clusterserviceversion.yaml "${CSV_FILE}"

  yq ea -i 'select(fi==0).metadata.annotations = select(fi==1).metadata.annotations | select(fi==0)' "${CSV_FILE}" "${CSV_TEMPLATE}"
  yq ea -i ".spec.install.spec.deployments[0].spec.template.spec.securityContext = {}" "${CSV_FILE}"
  yq ea -i '.spec.install.spec.deployments[0].spec.template.spec.containers[0].resources = {"requests": {"cpu": "10m", "memory": "100Mi"}}' "${CSV_FILE}"
  yq ea -i '.spec.install.spec.deployments[0].spec.template.spec.containers[1].resources = {"requests": {"cpu": "10m", "memory": "100Mi"}}' "${CSV_FILE}"

  # The OLM lets operator to watch specified namespace with i.e.: spec.targetNamespaces:[bar, foo] in the OperatorGroup crd.
  # The OLM then automatically injects template.metadata.annotations.olm.targetNamespaces:[bar, foo] into deployment.apps/flink-kubernetes-operator
  # See PR https://github.com/apache/flink-kubernetes-operator/pull/420 see how the watchedNamespaces is assigned to targetNamespaces.
  yq ea -i '(... | select(has("env"))).env += {"name": "WATCH_NAMESPACES", "valueFrom": {"fieldRef": {"fieldPath": "metadata.annotations['\''olm.targetNamespaces'\'']"}}}' "${CSV_FILE}"

  yq ea -i ".spec.install.spec.deployments[0].spec.template.spec.initContainers = load(\"${INIT_CONT}\").initContainers" "${CSV_FILE}"

  yq ea -i '.spec.install.spec.deployments[0].spec.template.spec.volumes[1] = {"name": "keystore","emptyDir": {}}' "${CSV_FILE}"

  yq ea -i "(... | select(has(\"image\"))).image =\"${OPERATOR_IMG}\"" "${CSV_FILE}"

  yq ea -i ".metadata.annotations.createdAt = \"$(date +'%Y-%m-%d %H:%M:%S')\"" "${CSV_FILE}"

  yq ea -i ".metadata.annotations.containerImage = \"${OPERATOR_IMG}\"" "${CSV_FILE}"

  yq ea -i ".spec.replaces = \"${PACKAGE_NAME}.v${PREVIOUS_BUNDLE_VERSION}\" | .spec.replaces style=\"\"" "${CSV_FILE}"

  yq ea -i "del(.subjects[0].namespace)" "${ROLE_BINDING}"

  # Needed to replace description with new bundle values
  sed -i "s/RELEASE_VERSION/${BUNDLE_VERSION}/" "${CSV_FILE}"
}

validate_olm_bundle() {
  operator-sdk bundle validate "${BUNDLE}" --select-optional name=operatorhub
  operator-sdk bundle validate "${BUNDLE}" --select-optional suite=operatorframework
}

generate_olm_bundle
validate_olm_bundle

