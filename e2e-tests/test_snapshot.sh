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

# This script tests the FlinkStateSnapshot CR as follows:
# 1. Create deployment with FlinkStateSnapshot disabled and upgrade it. Then enable FlinkStateSnapshot and assert that the saved savepoint was used.
# 2. Trigger and dispose of savepoint by creating a new FlinkStateSnapshot savepoint CR
# 3. Trigger savepoint by using savepoint trigger nonce
# 4. Trigger checkpoint by using trigger nonce
# 5. Test periodic savepoints triggered by the operator
# 6. Change job to upgrade mode, suspend job and assert new FlinkStateSnapshot CR created

SCRIPT_DIR=$(dirname "$(readlink -f "$0")")
source "${SCRIPT_DIR}/utils.sh"

CLUSTER_ID="flink-example-statemachine"
APPLICATION_YAML="${SCRIPT_DIR}/data/flinkdep-cr.yaml"
APPLICATION_IDENTIFIER="flinkdep/$CLUSTER_ID"

SAVEPOINT_YAML="${SCRIPT_DIR}/data/savepoint.yaml"
SAVEPOINT_IDENTIFIER="flinksnp/example-savepoint"

TIMEOUT=300

on_exit cleanup_and_exit "$APPLICATION_YAML" $TIMEOUT $CLUSTER_ID
on_exit cleanup_snapshots "$CLUSTER_ID" $TIMEOUT

retry_times 5 30 "kubectl apply -f $APPLICATION_YAML" || exit 1
kubectl patch flinkdep ${CLUSTER_ID} --type merge --patch '{"spec":{"job":{"upgradeMode": "savepoint"},"flinkConfiguration":{"web.checkpoints.history":"1000"}}}'

wait_for_jobmanager_running $CLUSTER_ID $TIMEOUT
jm_pod_name=$(get_jm_pod_name $CLUSTER_ID)

wait_for_logs $jm_pod_name "Completed checkpoint [0-9]+ for job" ${TIMEOUT} || exit 1
wait_for_status $APPLICATION_IDENTIFIER '.status.jobManagerDeploymentStatus' READY ${TIMEOUT} || exit 1
wait_for_status $APPLICATION_IDENTIFIER '.status.jobStatus.state' RUNNING ${TIMEOUT} || exit 1



# Test upgrade by setting legacy field
kubectl patch flinkdep ${CLUSTER_ID} --type merge --patch '{"spec":{"job":{"state": "suspended"}}}'
wait_for_status $APPLICATION_IDENTIFIER '.status.lifecycleState' "SUSPENDED" ${TIMEOUT} || exit 1

location=$(kubectl get $APPLICATION_IDENTIFIER -o yaml | yq '.status.jobStatus.upgradeSavepointPath')
if [ "$location" == "" ]; then echo "Legacy savepoint location was empty"; exit 1; fi
echo "Removing upgradeSavepointPath and setting lastSavepoint"
kubectl patch flinkdep ${CLUSTER_ID} --type=merge --subresource status --patch '{"status":{"jobStatus":{"upgradeSavepointPath":null,"savepointInfo":{"lastSavepoint":{"timeStamp": 0, "location": "'$location'", "triggerNonce": 0}}}}}'

# Delete operator Pod to clear CR state cache
kubectl delete pod -n $(get_operator_pod_namespace) $(get_operator_pod_name)
sleep 20
retry_times 10 10 "kubectl wait -n $(get_operator_pod_namespace) --for=condition=Ready --timeout=${TIMEOUT}s pod/$(get_operator_pod_name)" || exit 1

echo "Restarting deployment and asserting savepoint path used"
kubectl patch flinkdep ${CLUSTER_ID} --type merge --patch '{"spec":{"job": {"state": "running" } } }'
wait_for_jobmanager_running $CLUSTER_ID $TIMEOUT
wait_for_status $APPLICATION_IDENTIFIER '.status.jobManagerDeploymentStatus' READY ${TIMEOUT} || exit 1
wait_for_status $APPLICATION_IDENTIFIER '.status.jobStatus.state' RUNNING ${TIMEOUT} || exit 1

jm_pod_name=$(get_jm_pod_name $CLUSTER_ID)
wait_for_logs $jm_pod_name "Restoring job .* from Savepoint" ${TIMEOUT} || exit 1
wait_for_logs $jm_pod_name "execution.savepoint.path, ${location}" ${TIMEOUT} || exit 1
wait_for_logs $jm_pod_name "Completed checkpoint [0-9]+ for job" ${TIMEOUT} || exit 1



# Enable FlinkStateSnapshot CRs
kubectl patch flinkdep ${CLUSTER_ID} --type merge --patch '{"spec":{"flinkConfiguration":{"kubernetes.operator.snapshot.resource.enabled":"true"}}}'
job_id=$(kubectl logs $jm_pod_name -c flink-main-container | grep -E -o 'Job [a-z0-9]+ is submitted' | awk '{print $2}')
echo "Found job ID $job_id"



# Testing manual savepoint trigger and disposal via CR
echo "Creating manual savepoint..."
retry_times 5 30 "kubectl apply -f $SAVEPOINT_YAML" || exit 1
wait_for_status $SAVEPOINT_IDENTIFIER '.status.state' "COMPLETED" $TIMEOUT || exit 1

location=$(kubectl get $SAVEPOINT_IDENTIFIER -o yaml | yq '.status.path')
if [ "$location" == "" ]; then echo "Manual savepoint location was empty"; exit 1; fi

echo "Disposing manual savepoint..."
kubectl delete $SAVEPOINT_IDENTIFIER
wait_for_logs $jm_pod_name "Disposing savepoint $location" ${TIMEOUT} || exit 1



# Testing manual savepoint via trigger nonce
kubectl patch $APPLICATION_IDENTIFIER --type merge --patch '{"spec":{"job": {"savepointTriggerNonce": 123456 } } }'

echo "Waiting for manual savepoint..."
snapshot=$(wait_for_snapshot $CLUSTER_ID "savepoint" "manual" ${TIMEOUT})
if [ "$snapshot" == "" ]; then echo "Could not find snapshot"; exit 1; fi
echo "Found snapshot with name $snapshot"

wait_for_status flinksnp/$snapshot '.status.spec.checkpoint' null $TIMEOUT || exit 1
wait_for_status $APPLICATION_IDENTIFIER '.status.jobStatus.savepointInfo.triggerId' null $TIMEOUT || exit 1
wait_for_status $APPLICATION_IDENTIFIER '.status.jobStatus.savepointInfo.triggerTimestamp' null $TIMEOUT || exit 1
if [ "$(kubectl get flinksnp/$snapshot -o yaml | yq '.status.path')" == "" ]; then echo "Manual savepoint location was empty"; exit 1; fi
kubectl delete flinksnp/$snapshot



# Testing manual checkpoint via trigger nonce
kubectl patch $APPLICATION_IDENTIFIER --type merge --patch '{"spec":{"job": {"checkpointTriggerNonce": 123456 } } }'

echo "Waiting for manual checkpoint..."
snapshot=$(wait_for_snapshot $CLUSTER_ID "checkpoint" "manual" ${TIMEOUT})
if [ "$snapshot" == "" ]; then echo "Could not find snapshot"; exit 1; fi

echo "Found checkpoint with name $snapshot"

wait_for_status flinksnp/$snapshot '.status.spec.savepoint' null $TIMEOUT || exit 1
if [ "$(kubectl get flinksnp/$snapshot -o yaml | yq '.status.path')" == "" ]; then echo "Manual checkpoint location was empty"; exit 1; fi
kubectl delete flinksnp/$snapshot


# Test periodic savepoints
kubectl patch flinkdep ${CLUSTER_ID} --type merge --patch '{"spec":{"flinkConfiguration":{"kubernetes.operator.periodic.savepoint.interval":"60s"}}}'
sleep 20

echo "Waiting for periodic savepoint..."
snapshot=$(wait_for_snapshot $CLUSTER_ID "savepoint" "periodic" ${TIMEOUT})
if [ "$snapshot" == "" ]; then echo "Could not find snapshot"; exit 1; fi

echo "Found periodic savepoint: $snapshot"
if [ "$(kubectl get flinksnp/$snapshot -o yaml | yq '.status.path')" == "" ]; then echo "Periodic savepoint location was empty"; exit 1; fi
kubectl patch flinkdep ${CLUSTER_ID} --type merge --patch '{"spec":{"flinkConfiguration":{"kubernetes.operator.periodic.savepoint.interval":""}}}'


# Test upgrade savepoint
echo "Suspending deployment..."
kubectl patch flinkdep ${CLUSTER_ID} --type merge --patch '{"spec":{"job":{"state":"suspended"}}}'
wait_for_status $APPLICATION_IDENTIFIER '.status.lifecycleState' "SUSPENDED" ${TIMEOUT} || exit 1

echo "Waiting for upgrade savepoint..."
snapshot=$(wait_for_snapshot $CLUSTER_ID "savepoint" "upgrade" ${TIMEOUT})
if [ "$snapshot" == "" ]; then echo "Could not find snapshot"; exit 1; fi

location=$(kubectl get flinksnp/$snapshot -o yaml | yq '.status.path')
if [ "$location" == "" ]; then echo "Upgrade savepoint location was empty"; exit 1; fi

wait_for_status $APPLICATION_IDENTIFIER '.status.jobStatus.upgradeSavepointPath' "$location" ${TIMEOUT} || exit 1

echo "Restarting deployment..."
kubectl patch flinkdep ${CLUSTER_ID} --type merge --patch '{"spec":{"job": {"state": "running" } } }'
wait_for_jobmanager_running $CLUSTER_ID $TIMEOUT
wait_for_status $APPLICATION_IDENTIFIER '.status.jobManagerDeploymentStatus' READY ${TIMEOUT} || exit 1
wait_for_status $APPLICATION_IDENTIFIER '.status.jobStatus.state' RUNNING ${TIMEOUT} || exit 1

jm_pod_name=$(get_jm_pod_name $CLUSTER_ID)

# Check the new JobManager recovering from latest successful checkpoint
wait_for_logs $jm_pod_name "Restoring job .* from Savepoint" ${TIMEOUT} || exit 1
wait_for_logs $jm_pod_name "execution.savepoint.path, ${location}" ${TIMEOUT} || exit 1
wait_for_logs $jm_pod_name "Completed checkpoint [0-9]+ for job" ${TIMEOUT} || exit 1

kubectl delete flinksnp/$snapshot

echo "Successfully run the FlinkStateSnapshot test"

