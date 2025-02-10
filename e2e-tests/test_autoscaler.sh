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

SCRIPT_DIR=$(dirname "$(readlink -f "$0")")
source "${SCRIPT_DIR}/utils.sh"

CLUSTER_ID="flink-autoscaler-e2e"
APPLICATION_YAML="${SCRIPT_DIR}/data/autoscaler.yaml"
APPLICATION_IDENTIFIER="flinkdep/$CLUSTER_ID"
TIMEOUT=300

on_exit cleanup_and_exit "$APPLICATION_YAML" $TIMEOUT $CLUSTER_ID

retry_times 5 30 "kubectl apply -f $APPLICATION_YAML" || exit 1

wait_for_jobmanager_running $CLUSTER_ID $TIMEOUT
jm_pod_name=$(get_jm_pod_name $CLUSTER_ID)

wait_for_logs $jm_pod_name "Completed checkpoint [0-9]+ for job" ${TIMEOUT} || exit 1
wait_for_status $APPLICATION_IDENTIFIER '.status.lifecycleState' STABLE ${TIMEOUT} || exit 1
wait_for_event FlinkDeployment $CLUSTER_ID .reason==\"ScalingReport\" ${TIMEOUT} || exit 1
wait_for_event FlinkDeployment $CLUSTER_ID .reason==\"SpecChanged\" ${TIMEOUT} || exit 1

FLINK_VERSION=$(get_flink_version $APPLICATION_IDENTIFIER)
echo "Flink version: $FLINK_VERSION"
if [ "$FLINK_VERSION" = "v1_17" ]; then
  echo "Verifying full upgrade triggered"
  wait_for_event FlinkDeployment $CLUSTER_ID .reason==\"Suspended\" ${TIMEOUT} || exit 1
else
  echo "Verifying inplace scaling triggered"
  wait_for_event FlinkDeployment $CLUSTER_ID .reason==\"Scaling\" ${TIMEOUT} || exit 1
fi
wait_for_status $APPLICATION_IDENTIFIER '.status.lifecycleState' STABLE ${TIMEOUT} || exit 1

echo "Successfully run the autoscaler test"
