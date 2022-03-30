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

source "$(dirname "$0")"/utils.sh

CLUSTER_ID="flink-example-statemachine"
TIMEOUT=300

function cleanup_and_exit() {
    if [ $TRAPPED_EXIT_CODE != 0 ];then
      debug_and_show_logs
    fi

    kubectl delete -f e2e-tests/data/cr.yaml
    kubectl wait --for=delete pod --timeout=${TIMEOUT}s --selector="app=${CLUSTER_ID}"
    kubectl delete cm --selector="app=${CLUSTER_ID},configmap-type=high-availability"
}

function wait_for_jobmanager_running() {
    retry_times 30 3 "kubectl get deploy/${CLUSTER_ID}" || exit 1

    kubectl wait --for=condition=Available --timeout=${TIMEOUT}s deploy/${CLUSTER_ID} || exit 1
    jm_pod_name=$(kubectl get pods --selector="app=${CLUSTER_ID},component=jobmanager" -o jsonpath='{..metadata.name}')

    echo "Waiting for jobmanager pod ${jm_pod_name} ready."
    kubectl wait --for=condition=Ready --timeout=${TIMEOUT}s pod/$jm_pod_name || exit 1

    wait_for_logs $jm_pod_name "Rest endpoint listening at" ${TIMEOUT} || exit 1
}

function assert_available_slots() {
  expected=$1
  ip=$(minikube ip)
  actual=$(curl http://$ip/default/${CLUSTER_ID}/overview 2>/dev/null | grep -E -o '"slots-available":[0-9]+' | awk -F':' '{print $2}')
  if [[ expected -ne actual ]]; then
    echo "Expected available slots: $expected, actual: $actual"
    exit 1
  fi
  echo "Successfully assert available slots"
}

on_exit cleanup_and_exit

retry_times 5 30 "kubectl apply -f e2e-tests/data/cr.yaml" || exit 1

wait_for_jobmanager_running

wait_for_logs $jm_pod_name "Completed checkpoint [0-9]+ for job" ${TIMEOUT} || exit 1
wait_for_status flinkdep/flink-example-statemachine '.status.jobManagerDeploymentStatus' READY ${TIMEOUT} || exit 1
wait_for_status flinkdep/flink-example-statemachine '.status.jobStatus.state' RUNNING ${TIMEOUT} || exit 1
assert_available_slots 0

job_id=$(kubectl logs $jm_pod_name | grep -E -o 'Job [a-z0-9]+ is submitted' | awk '{print $2}')

# Update the FlinkDeployment and trigger the last state upgrade
kubectl patch flinkdep ${CLUSTER_ID} --type merge --patch '{"spec":{"job": {"parallelism": 1 } } }'

kubectl wait --for=delete pod --timeout=${TIMEOUT}s --selector="app=${CLUSTER_ID}"
wait_for_jobmanager_running

# Check the new JobManager recovering from latest successful checkpoint
wait_for_logs $jm_pod_name "Restoring job $job_id from Checkpoint" ${TIMEOUT} || exit 1
wait_for_logs $jm_pod_name "Completed checkpoint [0-9]+ for job" ${TIMEOUT} || exit 1
wait_for_status flinkdep/flink-example-statemachine '.status.jobManagerDeploymentStatus' READY ${TIMEOUT} || exit 1
wait_for_status flinkdep/flink-example-statemachine '.status.jobStatus.state' RUNNING ${TIMEOUT} || exit 1
assert_available_slots 1

echo "Successfully run the last-state upgrade test"

