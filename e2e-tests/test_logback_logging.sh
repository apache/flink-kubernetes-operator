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

# This script tests that the operator starts correctly with the logback logging
# framework. The operator must be installed with logging.framework=logback
# before running this test, e.g.:
#   helm install flink-operator helm/flink-kubernetes-operator --set logging.framework=logback
#
# Verifications:
# 1. Operator pod is running
# 2. No SLF4J multiple-bindings warnings in logs
# 3. Operator produces log output (proves logback is active)
# 4. LOGGING_FRAMEWORK env var is set to "logback"
# 5. LOG_CONFIG env var points to logback config
# 6. logback-operator.xml is mounted in the container
# 7. log4j-operator.properties is NOT mounted

SCRIPT_DIR=$(dirname "$(readlink -f "$0")")
source "${SCRIPT_DIR}/utils.sh"

TIMEOUT=120

passed=true

operator_namespace=$(get_operator_pod_namespace)
operator_pod=$(get_operator_pod_name)
echo "Current operator pod is ${operator_pod} in namespace ${operator_namespace}"

# Check that there are no SLF4J multiple-bindings warnings
echo "Checking for SLF4J multiple bindings warnings..."
if kubectl logs "${operator_pod}" -c flink-kubernetes-operator -n "${operator_namespace}" | grep -q "SLF4J: Class path contains multiple SLF4J bindings"; then
  echo "ERROR: Found SLF4J multiple bindings warning"
  passed=false
fi

# Verify logback is actually producing output
echo "Checking that operator produces log output..."
log_lines=$(kubectl logs "${operator_pod}" -c flink-kubernetes-operator -n "${operator_namespace}" | wc -l)
if [ "$log_lines" -lt 1 ]; then
  echo "ERROR: No log output from operator"
  passed=false
fi

# Verify log format
echo "Verifying log format..."
sample_log=$(kubectl logs "${operator_pod}" -c flink-kubernetes-operator -n "${operator_namespace}" | grep "Starting Flink Kubernetes Operator" | head -1 | sed 's/\x1b\[[0-9;]*m//g')

# Check timestamp includes milliseconds (dot followed by 3 digits for logback)
if ! echo "$sample_log" | grep -qE '^[0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2}[.][0-9]{3} '; then
  echo "ERROR: Timestamp missing milliseconds. Sample: ${sample_log}"
  passed=false
fi

if ! echo "$sample_log" | grep -qE 'o\.a\.f\.k\.o\.[A-Za-z]+'; then
  echo "ERROR: Logger name not abbreviated. Sample: ${sample_log}"
  passed=false
fi

if ! echo "$sample_log" | grep -qE '\[INFO \]'; then
  echo "ERROR: Level format does not match [INFO ]. Sample: ${sample_log}"
  passed=false
fi

# Verify LOGGING_FRAMEWORK env var
echo "Verifying LOGGING_FRAMEWORK env var..."
framework=$(kubectl get pod "${operator_pod}" -n "${operator_namespace}" -o jsonpath='{.spec.containers[0].env[?(@.name=="LOGGING_FRAMEWORK")].value}')
if [ "$framework" != "logback" ]; then
  echo "ERROR: LOGGING_FRAMEWORK is '${framework}', expected 'logback'"
  passed=false
fi

# Verify LOG_CONFIG env var
echo "Verifying LOG_CONFIG env var..."
log_config=$(kubectl get pod "${operator_pod}" -n "${operator_namespace}" -o jsonpath='{.spec.containers[0].env[?(@.name=="LOG_CONFIG")].value}')
if [[ "$log_config" != *"logback"* ]]; then
  echo "ERROR: LOG_CONFIG is '${log_config}', expected it to contain 'logback'"
  passed=false
fi

# Verify logback config file is mounted
echo "Verifying logback-operator.xml is mounted..."
if ! kubectl exec "${operator_pod}" -c flink-kubernetes-operator -n "${operator_namespace}" -- test -f /opt/flink/conf/logback-operator.xml; then
  echo "ERROR: logback-operator.xml not found in container"
  passed=false
fi

# Verify log4j config files are NOT mounted
echo "Verifying log4j-operator.properties is NOT mounted..."
if kubectl exec "${operator_pod}" -c flink-kubernetes-operator -n "${operator_namespace}" -- test -f /opt/flink/conf/log4j-operator.properties 2>/dev/null; then
  echo "ERROR: log4j-operator.properties should not be mounted when using logback"
  passed=false
fi

if [ "$passed" = true ]; then
  echo "Successfully run the logback logging framework test"
else
  echo "Logback logging framework test failed"
  exit 1
fi

