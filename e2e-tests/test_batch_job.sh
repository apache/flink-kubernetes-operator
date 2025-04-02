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

# This script tests basic Flink batch job operations on Kubernetes:
# 1. Deploys a FlinkDeployment for a batch job.
# 2. Waits for the JobManager to become ready.
# 3. Verifies that the job reaches the FINISHED state.
# 4. Applies a no-op spec change and verifies the job remains in the FINISHED state.
# 5. Checks the operator logs for the expected job state transition message.
# 6. Checks the JobManager logs for successful application completion.
# 7. Applies a spec change and verifies the job re-runs successfully.
SCRIPT_DIR=$(dirname "$(readlink -f "$0")")
source "${SCRIPT_DIR}/utils.sh"

CLUSTER_ID="flink-example-wordcount-batch"
APPLICATION_YAML="${SCRIPT_DIR}/data/flinkdep-batch-cr.yaml"
APPLICATION_IDENTIFIER="flinkdep/$CLUSTER_ID"
TIMEOUT=300

on_exit cleanup_and_exit "$APPLICATION_YAML" $TIMEOUT $CLUSTER_ID

retry_times 5 30 "kubectl apply -f $APPLICATION_YAML" || exit 1

wait_for_jobmanager_running $CLUSTER_ID $TIMEOUT

# Wait for the job to reach the FINISHED state.
wait_for_status $APPLICATION_IDENTIFIER '.status.jobStatus.state' FINISHED $TIMEOUT || exit 1

# Apply a no-op spec change; verify the job remains in the FINISHED state.
kubectl patch flinkdep ${CLUSTER_ID} --type merge --patch '{"spec":{"flinkConfiguration": {"kubernetes.operator.deployment.readiness.timeout": "6h" } } }'
wait_for_status $APPLICATION_IDENTIFIER '.status.jobStatus.state' FINISHED $TIMEOUT || exit 1

# Verify the job status change to FINISHED shows up in the operator logs.
operator_pod_name=$(get_operator_pod_name)
wait_for_operator_logs "$operator_pod_name" "Job status changed from .* to FINISHED" ${TIMEOUT} || exit 1

# Verify the job completed successfully in the job manager logs.
jm_pod_name=$(get_jm_pod_name $CLUSTER_ID)
wait_for_logs "$jm_pod_name" "Application completed SUCCESSFULLY" ${TIMEOUT} || exit 1

# Apply a spec change; verify the job re-runs and reaches the FINISHED state.
kubectl patch flinkdep ${CLUSTER_ID} --type merge --patch '{"spec":{"job": {"parallelism": 1 } } }'
wait_for_status $APPLICATION_IDENTIFIER '.status.jobStatus.state' RECONCILING $TIMEOUT || exit 1
wait_for_status $APPLICATION_IDENTIFIER '.status.jobStatus.state' FINISHED $TIMEOUT || exit 1

echo "Successfully ran the batch job test"