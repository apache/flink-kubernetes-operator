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

# This script tests the application job operations:
# 1. Trigger savepoint
# 2. last state mode upgrade
SCRIPT_DIR=$(dirname "$(readlink -f "$0")")
source "${SCRIPT_DIR}/utils.sh"

CLUSTER_ID="flink-example-statemachine"
APPLICATION_YAML="${SCRIPT_DIR}/data/flinkdep-cr.yaml"
APPLICATION_IDENTIFIER="flinkdep/$CLUSTER_ID"
TIMEOUT=300

on_exit cleanup_and_exit "$APPLICATION_YAML" $TIMEOUT $CLUSTER_ID

retry_times 5 30 "kubectl apply -f $APPLICATION_YAML" || exit 1

wait_for_jobmanager_running $CLUSTER_ID $TIMEOUT
jm_pod_name=$(get_jm_pod_name $CLUSTER_ID)

wait_for_logs $jm_pod_name "Completed checkpoint [0-9]+ for job" ${TIMEOUT} || exit 1
wait_for_status $APPLICATION_IDENTIFIER '.status.jobManagerDeploymentStatus' READY ${TIMEOUT} || exit 1
wait_for_status $APPLICATION_IDENTIFIER '.status.jobStatus.state' RUNNING ${TIMEOUT} || exit 1
assert_available_slots 0 $CLUSTER_ID

job_id=$(kubectl logs $jm_pod_name -c flink-main-container | grep -E -o 'Job [a-z0-9]+ is submitted' | awk '{print $2}')

# Testing trigger savepoint
kubectl patch $APPLICATION_IDENTIFIER --type merge --patch '{"spec":{"job": {"savepointTriggerNonce": 123456 } } }'
wait_for_logs $jm_pod_name "Triggering savepoint for job" ${TIMEOUT} || exit 1
wait_for_status $APPLICATION_IDENTIFIER '.status.jobStatus.savepointInfo.triggerId' null $TIMEOUT || exit 1
wait_for_status $APPLICATION_IDENTIFIER '.status.jobStatus.savepointInfo.triggerTimestamp' null $TIMEOUT || exit 1
location=$(kubectl get $APPLICATION_IDENTIFIER -o yaml | yq '.status.jobStatus.savepointInfo.lastSavepoint.location')
if [ "$location" == "" ];then
  echo "lost savepoint location"
  exit 1
fi

# Testing last-state mode upgrade
# Update the FlinkDeployment and trigger the last state upgrade
kubectl patch flinkdep ${CLUSTER_ID} --type merge --patch '{"spec":{"job": {"parallelism": 1 } } }'
kubectl wait --for=delete deployment --timeout=${TIMEOUT}s --selector="app=${CLUSTER_ID}"
wait_for_jobmanager_running $CLUSTER_ID $TIMEOUT
jm_pod_name=$(get_jm_pod_name $CLUSTER_ID)

# Check the new JobManager recovering from latest successful checkpoint
wait_for_logs $jm_pod_name "Restoring job $job_id from Checkpoint" ${TIMEOUT} || exit 1
wait_for_logs $jm_pod_name "Completed checkpoint [0-9]+ for job" ${TIMEOUT} || exit 1
wait_for_status flinkdep/flink-example-statemachine '.status.jobManagerDeploymentStatus' READY ${TIMEOUT} || exit 1
wait_for_status flinkdep/flink-example-statemachine '.status.jobStatus.state' RUNNING ${TIMEOUT} || exit 1
assert_available_slots 1 $CLUSTER_ID

echo "Successfully run the last-state upgrade test"
