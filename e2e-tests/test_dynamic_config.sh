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

# This script tests the operator dynamic config on watched namespace:
# 1. Create a new namespace
# 2. Change the watched namespaces by patching on the flink-config-override
# 3. Monitor the operator log to find the watched namespace changed info
SCRIPT_DIR=$(dirname "$(readlink -f "$0")")
source "${SCRIPT_DIR}/utils.sh"

on_exit operator_cleanup_and_exit

TIMEOUT=360

operator_namespace=$(get_operator_pod_namespace)
operator_pod=$(get_operator_pod_name)
echo "Current operator pod is ${operator_pod}"
create_namespace dynamic

kubectl config set-context --current --namespace="${operator_namespace}"
patch_flink_config '{"data": {"config.yaml": "kubernetes.operator.watched.namespaces: default,flink,dynamic"}}'
wait_for_operator_logs "${operator_pod}" "Setting default configuration to {kubernetes.operator.watched.namespaces=default,flink,dynamic}" ${TIMEOUT} || exit 1

echo "Successfully run the dynamic property test"
