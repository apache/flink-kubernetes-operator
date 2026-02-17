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

# This script tests the Blue/Green ingress rotation functionality:
# - Create a FlinkBlueGreenDeployment with parent-level ingress spec
# - Verify ingress is created and points to Blue deployment
# - Trigger a transition to Green
# - Verify ingress switches to point to Green deployment
# - Verify Blue deployment is deleted but ingress remains
# - Verify ingress configuration is preserved across transitions

SCRIPT_DIR=$(dirname "$(readlink -f "$0")")
source "${SCRIPT_DIR}/utils.sh"

CLUSTER_ID="bg-ingress-test"
BG_CLUSTER_ID=$CLUSTER_ID
BLUE_CLUSTER_ID=$CLUSTER_ID"-blue"
GREEN_CLUSTER_ID=$CLUSTER_ID"-green"

APPLICATION_YAML="${SCRIPT_DIR}/data/bluegreen-ingress.yaml"
APPLICATION_IDENTIFIER="flinkbgdep/$CLUSTER_ID"
BLUE_APPLICATION_IDENTIFIER="flinkdep/$BLUE_CLUSTER_ID"
GREEN_APPLICATION_IDENTIFIER="flinkdep/$GREEN_CLUSTER_ID"
TIMEOUT=300

echo "Deploying BlueGreen deployment with ingress..."
retry_times 5 30 "kubectl apply -f $APPLICATION_YAML" || exit 1

sleep 1
wait_for_jobmanager_running $BLUE_CLUSTER_ID $TIMEOUT
wait_for_status $BLUE_APPLICATION_IDENTIFIER '.status.lifecycleState' STABLE ${TIMEOUT} || exit 1
wait_for_status $APPLICATION_IDENTIFIER '.status.jobStatus.state' RUNNING ${TIMEOUT} || exit 1
wait_for_status $APPLICATION_IDENTIFIER '.status.blueGreenState' ACTIVE_BLUE ${TIMEOUT} || exit 1

echo "Verifying ingress created and points to Blue..."
kubectl get ingress $BG_CLUSTER_ID -n default || exit 1

# Check ingress backend points to Blue's REST service
BLUE_BACKEND=$(kubectl get ingress $BG_CLUSTER_ID -n default -o jsonpath='{.spec.rules[0].http.paths[0].backend.service.name}')
EXPECTED_BLUE_BACKEND="${BLUE_CLUSTER_ID}-rest"
if [ "$BLUE_BACKEND" != "$EXPECTED_BLUE_BACKEND" ]; then
    echo "ERROR: Ingress backend should be '$EXPECTED_BLUE_BACKEND' but got '$BLUE_BACKEND'"
    exit 1
fi
echo " Ingress correctly points to Blue deployment"

# Verify ingress annotations
REWRITE_ANNOTATION=$(kubectl get ingress $BG_CLUSTER_ID -n default -o jsonpath='{.metadata.annotations.nginx\.ingress\.kubernetes\.io/rewrite-target}')
if [ "$REWRITE_ANNOTATION" != "/" ]; then
    echo "ERROR: Expected rewrite annotation '/' but got '$REWRITE_ANNOTATION'"
    exit 1
fi
echo " Ingress annotations preserved"

echo "Triggering Blue’Green transition..."
kubectl patch flinkbgdep ${BG_CLUSTER_ID} --type merge --patch '{"spec":{"template":{"spec":{"flinkConfiguration":{"taskmanager.numberOfTaskSlots":"2"}}}}}'

# Wait for Green to be ready
wait_for_jobmanager_running $GREEN_CLUSTER_ID $TIMEOUT
wait_for_status $GREEN_APPLICATION_IDENTIFIER '.status.lifecycleState' STABLE ${TIMEOUT} || exit 1

echo "Waiting for Blue deletion..."
kubectl wait --for=delete deployment --timeout=${TIMEOUT}s --selector="app=${BLUE_CLUSTER_ID}" || exit 1

wait_for_status $APPLICATION_IDENTIFIER '.status.jobStatus.state' RUNNING ${TIMEOUT} || exit 1
wait_for_status $APPLICATION_IDENTIFIER '.status.blueGreenState' ACTIVE_GREEN ${TIMEOUT} || exit 1

echo "Verifying ingress switched to Green..."
# Ingress should still exist
kubectl get ingress $BG_CLUSTER_ID -n default || exit 1

# Check ingress backend now points to Green's REST service
GREEN_BACKEND=$(kubectl get ingress $BG_CLUSTER_ID -n default -o jsonpath='{.spec.rules[0].http.paths[0].backend.service.name}')
EXPECTED_GREEN_BACKEND="${GREEN_CLUSTER_ID}-rest"
if [ "$GREEN_BACKEND" != "$EXPECTED_GREEN_BACKEND" ]; then
    echo "ERROR: Ingress backend should be '$EXPECTED_GREEN_BACKEND' but got '$GREEN_BACKEND'"
    exit 1
fi
echo " Ingress correctly switched to Green deployment"

# Verify annotations still present after transition
REWRITE_ANNOTATION=$(kubectl get ingress $BG_CLUSTER_ID -n default -o jsonpath='{.metadata.annotations.nginx\.ingress\.kubernetes\.io/rewrite-target}')
if [ "$REWRITE_ANNOTATION" != "/" ]; then
    echo "ERROR: Annotations lost during transition"
    exit 1
fi
echo " Ingress configuration preserved across transition"

echo "Triggering Green’Blue transition to verify bidirectional switching..."
kubectl patch flinkbgdep ${BG_CLUSTER_ID} --type merge --patch '{"spec":{"template":{"spec":{"flinkConfiguration":{"taskmanager.numberOfTaskSlots":"1"}}}}}'

wait_for_jobmanager_running $BLUE_CLUSTER_ID $TIMEOUT
wait_for_status $BLUE_APPLICATION_IDENTIFIER '.status.lifecycleState' STABLE ${TIMEOUT} || exit 1
kubectl wait --for=delete deployment --timeout=${TIMEOUT}s --selector="app=${GREEN_CLUSTER_ID}" || exit 1
wait_for_status $APPLICATION_IDENTIFIER '.status.blueGreenState' ACTIVE_BLUE ${TIMEOUT} || exit 1

echo "Verifying ingress switched back to Blue..."
BLUE_BACKEND=$(kubectl get ingress $BG_CLUSTER_ID -n default -o jsonpath='{.spec.rules[0].http.paths[0].backend.service.name}')
if [ "$BLUE_BACKEND" != "$EXPECTED_BLUE_BACKEND" ]; then
    echo "ERROR: Ingress backend should be '$EXPECTED_BLUE_BACKEND' on return but got '$BLUE_BACKEND'"
    exit 1
fi
echo " Ingress correctly switched back to Blue"

echo "Cleaning up..."
kubectl delete flinkbluegreendeployments/$BG_CLUSTER_ID &
kubectl wait --for=delete flinkbluegreendeployments/$BG_CLUSTER_ID --timeout=${TIMEOUT}s

# Verify ingress is deleted with the deployment
INGRESS_DELETED=$(kubectl get ingress $BG_CLUSTER_ID -n default 2>&1 || echo "NotFound")
if [[ ! "$INGRESS_DELETED" =~ "NotFound" ]]; then
    echo "ERROR: Ingress should be deleted with BlueGreen deployment"
    exit 1
fi
echo " Ingress cleaned up correctly"

echo "Successfully run the Blue/Green ingress rotation test"
