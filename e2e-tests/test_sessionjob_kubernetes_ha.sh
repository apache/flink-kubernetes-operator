#!/usr/bin/env bash
#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

SCRIPT_DIR=$(dirname "$(readlink -f "$0")")
source "${SCRIPT_DIR}/utils.sh"

CLUSTER_ID="session-cluster-1"
APPLICATION_YAML="${SCRIPT_DIR}/data/sessionjob-cr.yaml"
TIMEOUT=300
SESSION_CLUSTER_IDENTIFIER="flinkdep/session-cluster-1"
SESSION_JOB_IDENTIFIER="sessionjob/flink-example-statemachine"

on_exit cleanup_and_exit "$APPLICATION_YAML" $TIMEOUT $CLUSTER_ID

retry_times 5 30 "kubectl apply -f $APPLICATION_YAML" || exit 1

wait_for_jobmanager_running $CLUSTER_ID $TIMEOUT
jm_pod_name=$(get_jm_pod_name $CLUSTER_ID)

wait_for_logs $jm_pod_name "Completed checkpoint [0-9]+ for job" ${TIMEOUT} || exit 1
wait_for_status $SESSION_CLUSTER_IDENTIFIER '.status.jobManagerDeploymentStatus' READY ${TIMEOUT} || exit 1
wait_for_status $SESSION_JOB_IDENTIFIER '.status.jobStatus.state' RUNNING ${TIMEOUT} || exit 1

job_id=$(kubectl logs $jm_pod_name -c flink-main-container | grep -E -o 'Job [a-z0-9]+ is submitted' | awk '{print $2}')

# Kill the JobManager
echo "Kill the $jm_pod_name"
kubectl exec $jm_pod_name -c flink-main-container -- /bin/sh -c "kill 1"

# Check the new JobManager recovering from latest successful checkpoint
wait_for_logs $jm_pod_name "Restoring job $job_id from Checkpoint" ${TIMEOUT} || exit 1
wait_for_logs $jm_pod_name "Completed checkpoint [0-9]+ for job" ${TIMEOUT} || exit 1
wait_for_status $SESSION_CLUSTER_IDENTIFIER '.status.jobManagerDeploymentStatus' READY ${TIMEOUT} || exit 1
wait_for_status $SESSION_JOB_IDENTIFIER '.status.jobStatus.state' RUNNING ${TIMEOUT} || exit 1

echo "Successfully run the Flink Session Job HA test"

