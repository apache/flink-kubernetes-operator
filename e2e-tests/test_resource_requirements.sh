#!/usr/bin/env bash
################################################################################
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
################################################################################

# This script verifies that spec.jobManager/taskManager.resources (standard
# Kubernetes ResourceRequirements) are honored end-to-end: the requested
# CPU/memory and the CPU/memory limit factors (limits/requests) are applied to
# the actual JM and TM main containers. Runs in both native and standalone
# deployment modes via the CI matrix.

SCRIPT_DIR=$(dirname "$(readlink -f "$0")")
source "${SCRIPT_DIR}/utils.sh"

CLUSTER_ID="flink-example-resource-requirements"
APPLICATION_YAML="${SCRIPT_DIR}/data/resource-requirements-cr.yaml"
APPLICATION_IDENTIFIER="flinkdep/$CLUSTER_ID"
TIMEOUT=300

on_exit cleanup_and_exit "$APPLICATION_YAML" $TIMEOUT $CLUSTER_ID

retry_times 5 30 "kubectl apply -f $APPLICATION_YAML" || exit 1

wait_for_jobmanager_running $CLUSTER_ID $TIMEOUT
wait_for_status $APPLICATION_IDENTIFIER '.status.jobManagerDeploymentStatus' READY ${TIMEOUT} || exit 1
wait_for_status $APPLICATION_IDENTIFIER '.status.jobStatus.state' RUNNING ${TIMEOUT} || exit 1

# Asserts the main container of a JM/TM pod carries the expected resources.
# The spec requests cpu=500m/mem=1Gi with limits cpu=1/mem=2Gi, i.e. a limit
# factor of 2.0 for both, which must be reflected on the running container.
function assert_container_resources() {
  local component=$1
  local pod
  pod=$(kubectl get pods --selector="app=${CLUSTER_ID},component=${component}" -o jsonpath='{.items[0].metadata.name}')
  if [ -z "$pod" ]; then
    echo "No ${component} pod found for ${CLUSTER_ID}"
    exit 1
  fi

  local res
  res=$(kubectl get pod "$pod" -o yaml | yq '.spec.containers[] | select(.name == "flink-main-container") | .resources')
  echo "${component} (${pod}) resources:"
  echo "$res"

  local rcpu lcpu rmem lmem
  rcpu=$(echo "$res" | yq '.requests.cpu')
  lcpu=$(echo "$res" | yq '.limits.cpu')
  rmem=$(echo "$res" | yq '.requests.memory')
  lmem=$(echo "$res" | yq '.limits.memory')

  if [ "$rcpu" != "500m" ]; then
    echo "FAIL: ${component} requests.cpu=${rcpu}, expected 500m"; exit 1
  fi
  if [ "$lcpu" != "1" ]; then
    echo "FAIL: ${component} limits.cpu=${lcpu}, expected 1 (cpu limit factor 2.0)"; exit 1
  fi
  if [ -z "$rmem" ] || [ "$rmem" == "null" ]; then
    echo "FAIL: ${component} requests.memory is empty"; exit 1
  fi
  if [ -z "$lmem" ] || [ "$lmem" == "null" ]; then
    echo "FAIL: ${component} limits.memory is empty"; exit 1
  fi
  if [ "$lmem" == "$rmem" ]; then
    echo "FAIL: ${component} memory limit (${lmem}) not inflated over request (${rmem}) by the memory limit factor"; exit 1
  fi
  echo "OK: ${component} cpu ${rcpu}->${lcpu}, memory ${rmem}->${lmem}"
}

assert_container_resources jobmanager
assert_container_resources taskmanager

echo "Successfully verified ResourceRequirements are applied to the JM/TM pods"
