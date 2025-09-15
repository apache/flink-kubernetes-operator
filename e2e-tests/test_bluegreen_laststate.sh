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

# This script tests the Flink Blue/Green Deployments support as follows:
# - Create a FlinkBlueGreenDeployment which automatically starts a "Blue" FlinkDeployment
# - Once this setup is stable, we trigger a transition which will create the "Green" FlinkDeployment
# - Once it's stable, verify the "Blue" FlinkDeployment is torn down
# - Perform additional validation(s) before exiting

SCRIPT_DIR=$(dirname "$(readlink -f "$0")")
source "${SCRIPT_DIR}/utils.sh"

CLUSTER_ID="basic-bg-laststate-example"
BG_CLUSTER_ID=$CLUSTER_ID
BLUE_CLUSTER_ID=$CLUSTER_ID"-blue"
GREEN_CLUSTER_ID=$CLUSTER_ID"-green"

APPLICATION_YAML="${SCRIPT_DIR}/data/bluegreen-laststate.yaml"
APPLICATION_IDENTIFIER="flinkbgdep/$CLUSTER_ID"
BLUE_APPLICATION_IDENTIFIER="flinkdep/$BLUE_CLUSTER_ID"
GREEN_APPLICATION_IDENTIFIER="flinkdep/$GREEN_CLUSTER_ID"
TIMEOUT=300

#echo "BG_CLUSTER_ID " $BG_CLUSTER_ID
#echo "BLUE_CLUSTER_ID " $BLUE_CLUSTER_ID
#echo "APPLICATION_IDENTIFIER " $APPLICATION_IDENTIFIER
#echo "BLUE_APPLICATION_IDENTIFIER " $BLUE_APPLICATION_IDENTIFIER

retry_times 5 30 "kubectl apply -f $APPLICATION_YAML" || exit 1

sleep 1
wait_for_jobmanager_running $BLUE_CLUSTER_ID $TIMEOUT
wait_for_status $BLUE_APPLICATION_IDENTIFIER '.status.lifecycleState' STABLE ${TIMEOUT} || exit 1
wait_for_status $APPLICATION_IDENTIFIER '.status.jobStatus.state' RUNNING ${TIMEOUT} || exit 1
wait_for_status $APPLICATION_IDENTIFIER '.status.blueGreenState' ACTIVE_BLUE ${TIMEOUT} || exit 1

#blue_job_id=$(kubectl get -oyaml flinkdep/basic-bluegreen-example-blue | yq '.status.jobStatus.jobId')

#kubectl patch flinkbgdep ${BG_CLUSTER_ID} --type merge --patch '{"spec":{"template":{"spec":{"flinkConfiguration":{"rest.port":"8082","state.checkpoints.num-retained":"6"}}}}}'
kubectl patch flinkbgdep ${BG_CLUSTER_ID} --type merge --patch '{"spec":{"template":{"spec":{"flinkConfiguration":{"state.checkpoints.num-retained":"6"}}}}}'
echo "Resource patched, giving a chance for the savepoint to be taken..."
sleep 10

jm_pod_name=$(get_jm_pod_name $BLUE_CLUSTER_ID)
echo "Inspecting savepoint directory..."
kubectl exec -it $jm_pod_name -- bash -c "ls -lt /opt/flink/volume/flink-sp/"

wait_for_status $GREEN_APPLICATION_IDENTIFIER '.status.lifecycleState' STABLE ${TIMEOUT} || exit 1
kubectl wait --for=delete deployment --timeout=${TIMEOUT}s --selector="app=${BLUE_CLUSTER_ID}"
wait_for_status $APPLICATION_IDENTIFIER '.status.jobStatus.state' RUNNING ${TIMEOUT} || exit 1
wait_for_status $APPLICATION_IDENTIFIER '.status.blueGreenState' ACTIVE_GREEN ${TIMEOUT} || exit 1

green_initialSavepointPath=$(kubectl get -oyaml $GREEN_APPLICATION_IDENTIFIER | yq '.spec.job.initialSavepointPath')

echo "Deleting test B/G resources" $BG_CLUSTER_ID
kubectl delete flinkbluegreendeployments/$BG_CLUSTER_ID &
echo "Waiting for deployment to be deleted..."
kubectl wait --for=delete flinkbluegreendeployments/$BG_CLUSTER_ID

if [[ $green_initialSavepointPath == '/opt/flink/volume/flink-sp/savepoint-'* ]]; then
  echo 'Green deployment started from the expected initialSavepointPath:' $green_initialSavepointPath
else
  echo 'Unexpected initialSavepointPath:' $green_initialSavepointPath
  exit 1
fi;

echo "Successfully run the Flink Blue/Green Deployments test"