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

CLUSTER_ID="basic-bg-stateless-example"
BG_CLUSTER_ID=$CLUSTER_ID
BLUE_CLUSTER_ID=$CLUSTER_ID"-blue"
GREEN_CLUSTER_ID=$CLUSTER_ID"-green"

APPLICATION_YAML="${SCRIPT_DIR}/data/bluegreen-stateless.yaml"
APPLICATION_IDENTIFIER="flinkbgdep/$CLUSTER_ID"
BLUE_APPLICATION_IDENTIFIER="flinkdep/$BLUE_CLUSTER_ID"
GREEN_APPLICATION_IDENTIFIER="flinkdep/$GREEN_CLUSTER_ID"
TIMEOUT=300

retry_times 5 30 "kubectl apply -f $APPLICATION_YAML" || exit 1

sleep 1
wait_for_jobmanager_running $BLUE_CLUSTER_ID $TIMEOUT
wait_for_status $BLUE_APPLICATION_IDENTIFIER '.status.lifecycleState' STABLE ${TIMEOUT} || exit 1
wait_for_status $APPLICATION_IDENTIFIER '.status.jobStatus.state' RUNNING ${TIMEOUT} || exit 1
wait_for_status $APPLICATION_IDENTIFIER '.status.blueGreenState' ACTIVE_BLUE ${TIMEOUT} || exit 1

echo "PATCHING B/G deployment..."
#kubectl patch flinkbgdep ${BG_CLUSTER_ID} --type merge --patch '{"spec":{"template":{"spec":{"flinkConfiguration":{"rest.port":"8082","taskmanager.numberOfTaskSlots":"2"}}}}}'
kubectl patch flinkbgdep ${BG_CLUSTER_ID} --type merge --patch '{"spec":{"template":{"spec":{"flinkConfiguration":{"taskmanager.numberOfTaskSlots":"2"}}}}}'

wait_for_status $GREEN_APPLICATION_IDENTIFIER '.status.lifecycleState' STABLE ${TIMEOUT} || exit 1
kubectl wait --for=delete deployment --timeout=${TIMEOUT}s --selector="app=${BLUE_CLUSTER_ID}"
wait_for_status $APPLICATION_IDENTIFIER '.status.jobStatus.state' RUNNING ${TIMEOUT} || exit 1
wait_for_status $APPLICATION_IDENTIFIER '.status.blueGreenState' ACTIVE_GREEN ${TIMEOUT} || exit 1

echo "Deleting test B/G resources" $BG_CLUSTER_ID
kubectl delete flinkbluegreendeployments/$BG_CLUSTER_ID &
echo "Waiting for deployment to be deleted..."
kubectl wait --for=delete flinkbluegreendeployments/$BG_CLUSTER_ID

echo "Successfully run the Flink Blue/Green Deployments test"