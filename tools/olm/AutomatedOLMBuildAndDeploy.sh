#!/bin/bash
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
set +x
# Script that Automates the OLM Creation and test of Flink Operator

# Run the generate-olm-bundle.sh script
./generate-olm-bundle.sh

# Reset Cluster
kind delete cluster
kind create cluster
kubectl cluster-info --context kind-kind
# operator-sdk olm install
curl -sL https://github.com/operator-framework/operator-lifecycle-manager/releases/download/v0.22.0/install.sh | bash -s v0.22.0

# Deploy the catalog src
cat <<EOF | kubectl apply -f -
apiVersion: operators.coreos.com/v1alpha1
kind: CatalogSource
metadata:
  name: olm-flink-operator-catalog
  namespace: default
spec:
  sourceType: grpc
  image: "${DOCKER_REGISTRY}/${DOCKER_ORG}/flink-op-catalog:${BUNDLE_VERSION}"
EOF

# sleep 40 seconds wait for the catalog serving pod to start
echo "Sleeping 40 seconds"
sleep 40
# Check that the image is up:
kubectl get pods -n default |grep flink

# Deploy the subscription in the default namespace:
cat <<EOF | kubectl apply -f -
apiVersion: operators.coreos.com/v1alpha2
kind: OperatorGroup
metadata:
  name: default-og
  namespace: default
spec:
  # if not set, default to watch all namespaces
  targetNamespaces:
  - default
---
apiVersion: operators.coreos.com/v1alpha1
kind: Subscription
metadata:
  name: flink-kubernetes-operator
  namespace: default
spec:
  channel: alpha
  name: flink-kubernetes-operator
  source: olm-flink-operator-catalog
  sourceNamespace: default
  # For testing upgrade from previous version
  #installPlanApproval: Automatic # Manual
  #startingCSV: "flink-kubernetes-operator.v${PREVIOUS_BUNDLE_VERSION}"
EOF

# sleep 40 seconds
echo "Sleeping 40 seconds"
sleep 40

# Check the image is running
kubectl get pods,csv,ip,sub

# Deploy the sample:
echo "Run a Flink job, use: kubectl create -f https://raw.githubusercontent.com/apache/flink-kubernetes-operator/release-1.2/examples/basic.yaml"

