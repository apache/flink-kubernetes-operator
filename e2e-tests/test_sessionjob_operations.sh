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

# This script tests the session job operations:
# 1. Trigger savepoint
# 2. savepoint mode upgrade
SCRIPT_DIR=$(dirname "$(readlink -f "$0")")
source "${SCRIPT_DIR}/utils.sh"

CLUSTER_ID="session-cluster-1"
APPLICATION_YAML="${SCRIPT_DIR}/data/sessionjob-cr.yaml"
TIMEOUT=300
SESSION_CLUSTER_IDENTIFIER="flinkdep/$CLUSTER_ID"
SESSION_JOB_NAME="flink-example-statemachine"
SESSION_JOB_IDENTIFIER="sessionjob/$SESSION_JOB_NAME"
OPERATOR_POD_LABEL="app.kubernetes.io/name=flink-kubernetes-operator"

on_exit cleanup_and_exit "$APPLICATION_YAML" $TIMEOUT $CLUSTER_ID

retry_times 5 30 "kubectl apply -f $APPLICATION_YAML" || exit 1

wait_for_jobmanager_running $CLUSTER_ID $TIMEOUT
jm_pod_name=$(get_jm_pod_name $CLUSTER_ID)

wait_for_logs $jm_pod_name "Completed checkpoint [0-9]+ for job" ${TIMEOUT} || exit 1
wait_for_status $SESSION_CLUSTER_IDENTIFIER '.status.jobManagerDeploymentStatus' READY ${TIMEOUT} || exit 1
wait_for_status $SESSION_JOB_IDENTIFIER '.status.jobStatus.state' RUNNING ${TIMEOUT} || exit 1
assert_available_slots 0 $CLUSTER_ID

# Testing trigger savepoint
kubectl patch sessionjob ${SESSION_JOB_NAME} --type merge --patch '{"spec":{"job": {"savepointTriggerNonce": 123456 } } }'
wait_for_logs $jm_pod_name "Triggering savepoint for job" ${TIMEOUT} || exit 1
wait_for_status $SESSION_JOB_IDENTIFIER '.status.jobStatus.savepointInfo.triggerId' null $TIMEOUT || exit 1
wait_for_status $SESSION_JOB_IDENTIFIER '.status.jobStatus.savepointInfo.triggerTimestamp' null $TIMEOUT || exit 1
location=$(kubectl get $SESSION_JOB_IDENTIFIER -o yaml | yq '.status.jobStatus.savepointInfo.lastSavepoint.location')
if [ "$location" == "" ];then
  echo "lost savepoint location"
  exit 1
fi

echo "Starting sessionjob savepoint upgrade test"
# Testing savepoint mode upgrade
# Update the FlinkSessionJob and trigger the savepoint upgrade
kubectl patch sessionjob ${SESSION_JOB_NAME} --type merge --patch '{"spec":{"job": {"parallelism": 1 } } }'

# Check the new JobManager recovering from savepoint
wait_for_logs $jm_pod_name "Restoring job .* from Savepoint" ${TIMEOUT} || exit 1
wait_for_logs $jm_pod_name "Completed checkpoint [0-9]+ for job" ${TIMEOUT} || exit 1
wait_for_status $SESSION_CLUSTER_IDENTIFIER '.status.jobManagerDeploymentStatus' READY ${TIMEOUT} || exit 1
wait_for_status $SESSION_JOB_IDENTIFIER '.status.jobStatus.state' RUNNING ${TIMEOUT} || exit 1
assert_available_slots 1 $CLUSTER_ID

echo "Successfully run the sessionjob savepoint upgrade test"

flink_version=$(kubectl get $SESSION_CLUSTER_IDENTIFIER -o yaml | yq '.spec.flinkVersion')

if [ "$flink_version" != "v1_16" ]; then
  echo "Starting sessionjob last-state upgrade test"
  # Testing last-state mode upgrade
  # Update the FlinkSessionJob and trigger the last-state upgrade
  kubectl patch sessionjob ${SESSION_JOB_NAME} --type merge --patch '{"spec":{"job": {"parallelism": 2, "upgradeMode": "last-state" } } }'

  # Check the job was restarted with the new parallelism
  wait_for_status $SESSION_JOB_IDENTIFIER '.status.jobStatus.state' CANCELLING ${TIMEOUT} || exit 1
  wait_for_status $SESSION_JOB_IDENTIFIER '.status.jobStatus.state' RUNNING ${TIMEOUT} || exit 1
  assert_available_slots 0 $CLUSTER_ID

  echo "Successfully run the sessionjob last-state upgrade test"
else
  echo "Skipping last-state test for flink version 1.16"
fi

# Test Operator restart
echo "Delete session job " + $SESSION_JOB_NAME
kubectl delete flinksessionjob $SESSION_JOB_NAME
echo "Killing the operator pod"
kubectl delete pod -l $OPERATOR_POD_LABEL
echo "Submitting the session job again"
retry_times 5 30 "kubectl apply -f $APPLICATION_YAML" || exit 1

wait_for_jobmanager_running $CLUSTER_ID $TIMEOUT
wait_for_logs $jm_pod_name "Completed checkpoint [0-9]+ for job" ${TIMEOUT} || exit 1
wait_for_status $SESSION_CLUSTER_IDENTIFIER '.status.jobManagerDeploymentStatus' READY ${TIMEOUT} || exit 1
wait_for_status $SESSION_JOB_IDENTIFIER '.status.jobStatus.state' RUNNING ${TIMEOUT} || exit 1
