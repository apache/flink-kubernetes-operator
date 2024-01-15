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
#######################

# This script tests the operator HA:
# 1. Deploy a new flink deployment and wait for job manager to come up
# 2. Verify the operator log on existing leader
# 3. Delete the leader operator pod
# 4. Verify the new leader is different with the old one
# 5. Check operator log for the flink deployment in the new leader

SCRIPT_DIR=$(dirname "$(readlink -f "$0")")
source "${SCRIPT_DIR}/utils.sh"

CLUSTER_ID="flink-example-statemachine"
APPLICATION_YAML="${SCRIPT_DIR}/data/flinkdep-cr.yaml"
TIMEOUT=300

on_exit cleanup_and_exit "$APPLICATION_YAML" $TIMEOUT $CLUSTER_ID

retry_times 5 30 "kubectl apply -f $APPLICATION_YAML" || exit 1

wait_for_jobmanager_running $CLUSTER_ID $TIMEOUT
jm_pod_name=$(get_jm_pod_name $CLUSTER_ID)

wait_for_logs $jm_pod_name "Completed checkpoint [0-9]+ for job" ${TIMEOUT} || exit 1
wait_for_status flinkdep/flink-example-statemachine '.status.jobManagerDeploymentStatus' READY ${TIMEOUT} || exit 1
wait_for_status flinkdep/flink-example-statemachine '.status.jobStatus.state' RUNNING ${TIMEOUT} || exit 1

job_id=$(kubectl logs $jm_pod_name -c flink-main-container | grep -E -o 'Job [a-z0-9]+ is submitted' | awk '{print $2}')


# Verify operator status
operator_namespace=$(get_operator_pod_namespace)
display_current_lease_info
old_operator_leader=$(find_operator_pod_with_leadership)

echo "Current operator pod with leadership is ${old_operator_leader}"
wait_for_operator_logs "${old_operator_leader}" ".default/flink-example-statemachine. Resource fully reconciled, nothing to do" ${TIMEOUT} || exit 1

# Delete the leader operator pod
delete_operator_pod_with_leadership

# Wait for 20 seconds for leader election
sleep 20
display_current_lease_info
new_operator_leader=$(find_operator_pod_with_leadership)
echo "Current operator pod with leadership is ${new_operator_leader}"

if [ "${new_operator_leader}" == "${old_operator_leader}" ];then
  echo "The new operator pod with leadership is the same as old operator pod. New operator pod haven't acquire leadership"
  exit 1
fi

wait_for_operator_logs "${new_operator_leader}" ".default/flink-example-statemachine. Resource fully reconciled, nothing to do" ${TIMEOUT} || exit 1
echo "Successfully run the Flink Kubernetes application HA test in the new operator leader"