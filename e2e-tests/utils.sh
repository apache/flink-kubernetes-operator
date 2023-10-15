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

function wait_for_logs {
  local jm_pod_name=$1
  local successful_response_regex=$2
  local timeout=$3

  # wait or timeout until the log shows up
  echo "Waiting for log \"$2\"..."
  for i in $(seq 1 ${timeout}); do
    if kubectl logs $jm_pod_name -c flink-main-container | grep -E "${successful_response_regex}" >/dev/null; then
      echo "Log \"$2\" shows up."
      return
    fi

    sleep 1
  done
  echo "Log $2 does not show up within a timeout of ${timeout} sec"
  exit 1
}

function wait_for_operator_logs {
  local successful_response_regex=$1
  local timeout=$2
  operator_pod_name=$(get_operator_pod_name)
  operator_pod_namespace=$(get_operator_pod_namespace)

    # wait or timeout until the log shows up
  echo "Waiting for operator log \"$1\"..."
  for i in $(seq 1 ${timeout}); do
    if kubectl logs $operator_pod_name -c flink-kubernetes-operator -n "${operator_pod_namespace}" | grep -E "${successful_response_regex}" >/dev/null; then
      echo "Log \"$1\" shows up."
      return
    fi

    sleep 1
  done
  echo "Log $1 does not show up within a timeout of ${timeout} sec"
  exit 1
}

function wait_for_status {
  local resource=$1
  local status_path=$2
  local expected_status=$3
  local timeout=$4

  echo "Waiting for $resource$status_path converging to $expected_status state..."
  for i in $(seq 1 ${timeout}); do
    status=$(kubectl get -oyaml $resource | yq $status_path)
    if [ "$status" == "$expected_status" ]; then
      echo "Successfully verified that $resource$status_path is in $expected_status state."
      return
    fi

    sleep 1
  done
  echo "Status verification for $resource$status_path failed with timeout of ${timeout}."
  echo "Status converged to $status instead of $expected_status."
  exit 1
}

function wait_for_event {
  local kind=$1
  local resource=$2
  local event_filter=$3
  local timeout=$4

  echo "Waiting for $resource event matching $event_filter..."
  for i in $(seq 1 ${timeout}); do
    test=$(kubectl get events --field-selector involvedObject.kind=$kind,involvedObject.name=$resource -oyaml | yq ".items.[] | select($event_filter)")
    if [ "$test" ]; then
      echo "Successfully verified that $resource event exists."
      return 0
    fi

    sleep 1
  done
  echo "Event verification for $resource failed with timeout of ${timeout}."
  exit 1
}

function assert_available_slots() {
  expected=$1
  CLUSTER_ID=$2
  ip=$(minikube ip)
  actual=$(curl "http://$ip/default/${CLUSTER_ID}/overview" 2>/dev/null | grep -E -o '"slots-available":[0-9]+' | awk -F':' '{print $2}')
  if [[ "${expected}" != "${actual}" ]]; then
    echo "Expected available slots: ${expected}, actual: ${actual}"
    exit 1
  fi
  echo "Successfully assert available slots"
}

function wait_for_jobmanager_running() {
    CLUSTER_ID=$1
    TIMEOUT=$2
    retry_times 30 3 "kubectl get deploy/${CLUSTER_ID}" || exit 1

    kubectl wait --for=condition=Available --timeout=${TIMEOUT}s deploy/${CLUSTER_ID} || exit 1
    jm_pod_name=$(get_jm_pod_name $CLUSTER_ID)

    echo "Waiting for jobmanager pod ${jm_pod_name} ready."
    kubectl wait --for=condition=Ready --timeout=${TIMEOUT}s pod/$jm_pod_name || exit 1

    wait_for_logs $jm_pod_name "Rest endpoint listening at" ${TIMEOUT} || exit 1
}

function get_operator_pod_namespace() {
    operator_pod_namespace=$(kubectl get pods --selector="app.kubernetes.io/name=flink-kubernetes-operator" -o jsonpath='{..metadata.namespace}' --all-namespaces)
    if [ "$(grep -c . <<<"${operator_pod_namespace}")" != 1 ]; then
      echo "Invalid operator pod namespace: ${operator_pod_namespace}" >&2
      exit 1
    fi
    echo "${operator_pod_namespace}"
}

function get_operator_pod_name() {
    operator_pod_name=$(kubectl get pods --selector="app.kubernetes.io/name=flink-kubernetes-operator" -o jsonpath='{..metadata.name}' --all-namespaces)
    if [ "$(grep -c . <<<"${operator_pod_name}")" != 1 ]; then
      echo "Invalid operator pod name: ${operator_pod_name}" >&2
      exit 1
    fi
    echo "${operator_pod_name}"
}

function get_jm_pod_name() {
    CLUSTER_ID=$1
    jm_pod_name=$(kubectl get pods --selector="app=${CLUSTER_ID},component=jobmanager" -o jsonpath='{..metadata.name}')
    if [ "$(grep -c . <<<"${jm_pod_name}")" != 1 ]; then
      echo "Invalid job manager pod name: ${jm_pod_name}" >&2
      exit 1
    fi
    echo "${jm_pod_name}"
}

function retry_times() {
    local retriesNumber=$1
    local backoff=$2
    local command="$3"

    for i in $(seq 1 ${retriesNumber})
    do
        if ${command}; then
            return 0
        fi

        echo "Command: ${command} failed. Retrying..."
        sleep ${backoff}
    done

    echo "Command: ${command} failed ${retriesNumber} times."
    return 1
}

function get_flink_version() {
  local resource=$1

  kubectl get -oyaml $resource | yq ".spec.flinkVersion"
}

function patch_flink_config() {
  local patch=$1
  operator_pod_namespace=$(get_operator_pod_namespace)
  echo "Patch flink-operator-config with config: ${patch}"

  kubectl patch cm flink-operator-config -n "${operator_pod_namespace}" --type merge -p "${patch}"
}

function check_operator_log_for_errors {
  local ignore=$1
  echo "Checking for operator log errors..."
  #https://issues.apache.org/jira/browse/FLINK-30310
  echo "Error checking is temporarily turned off."
  return 0

  operator_pod_namespace=$(get_operator_pod_namespace)
  operator_pod_name=$(get_operator_pod_name)
  echo "Operator namespace: ${operator_pod_namespace} pod: ${operator_pod_name}"

  local cmd="kubectl logs -n ${operator_pod_namespace} ${operator_pod_name}
    | grep -e '\[\s*ERROR\s*\]'
    | grep -v 'Failed to submit a listener notification task' `#https://issues.apache.org/jira/browse/FLINK-30147`
    | grep -v 'Failed to submit job to session cluster' `#https://issues.apache.org/jira/browse/FLINK-30148`
    | grep -v 'Error during event processing' `#https://issues.apache.org/jira/browse/FLINK-30149`
    | grep -v 'Error while patching status' `#https://issues.apache.org/jira/browse/FLINK-30283`
    ${ignore}"

  echo "Filter command: ${cmd}"
  errors=$(eval ${cmd} || true)

  if [ -z "${errors}" ]; then
    echo "No errors in log files."
    return 0
  else
    echo -e "Found error in log files.\n\n${errors}"
    return 1
  fi
}

function debug_and_show_logs {
    echo "Debugging failed e2e test:"
    echo "Currently existing Kubernetes resources"
    kubectl get all
    kubectl describe all

    echo "Operator logs:"
    operator_pod_namespace=$(get_operator_pod_namespace)
    operator_pod_name=$(get_operator_pod_name)
    echo "Operator namespace: ${operator_pod_namespace} pod: ${operator_pod_name}"
    kubectl logs -n "${operator_pod_namespace}" "${operator_pod_name}"

    echo "Flink logs:"
    kubectl get pods -o jsonpath='{range .items[*]}{.metadata.name}{"\n"}{end}' | while read pod;do
        echo "Printing init container logs"
        print_pod_container_logs "$pod" "{.spec.initContainers[*].name}" "{.status.initContainerStatuses[*].restartCount}"
        echo "Printing main container logs"
        print_pod_container_logs "$pod" "{.spec.containers[*].name}" "{.status.containerStatuses[*].restartCount}"
    done
}

function print_pod_container_logs {
  pod=$1
  container_path=$2
  restart_count_path=$3

  containers=(`kubectl get pods $pod -o jsonpath=$container_path`)
  restart_counts=(`kubectl get pod $pod -o jsonpath=$restart_count_path`)

  if [[ -z "$containers" ]];then
    return 0
  fi

  for idx in "${!containers[@]}"; do
    echo "--------BEGIN CURRENT LOG for $pod:${containers[idx]}--------"
    kubectl logs $pod ${containers[idx]};
    echo "--------END CURRENT LOG--------"
    if [[ ${restart_counts[idx]} -gt 0 ]];then
      echo "--------BEGIN PREVIOUS LOGS for $pod:${containers[idx]}--------"
      kubectl logs $pod ${containers[idx]} --previous
      echo "--------END PREVIOUS LOGS--------"
    fi
  done
}

function start_minikube {
    if ! retry_times 5 30 start_minikube_if_not_running; then
        echo "Could not start minikube. Aborting..."
        exit 1
    fi
    minikube addons enable ingress
}

function start_minikube_if_not_running {
    if ! minikube status; then
        echo "Starting minikube ..."
        # Please update tbe docs when changing kubernetes version
        minikube start \
        --kubernetes-version=v1.25.3 \
        --extra-config=kubelet.image-gc-high-threshold=99 \
        --extra-config=kubelet.image-gc-low-threshold=98 \
        --extra-config=kubelet.minimum-container-ttl-duration=120m \
        --extra-config=kubelet.eviction-hard="memory.available<5Mi,nodefs.available<1Mi,imagefs.available<1Mi" \
        --extra-config=kubelet.eviction-soft="memory.available<5Mi,nodefs.available<2Mi,imagefs.available<2Mi" \
        --extra-config=kubelet.eviction-soft-grace-period="memory.available=2h,nodefs.available=2h,imagefs.available=2h"
        minikube update-context
    fi

    minikube status
    return $?
}

function stop_minikube {
    echo "Stopping minikube ..."
    if ! retry_times 5 30 "minikube stop"; then
        echo "Could not stop minikube. Aborting..."
        exit 1
    fi
}

function cleanup_and_exit() {
    echo "Starting cleanup"

    if [ $TRAPPED_EXIT_CODE != 0 ];then
      debug_and_show_logs
    fi

    APPLICATION_YAML=$1
    TIMEOUT=$2
    CLUSTER_ID=$3

    kubectl config set-context --current --namespace=default
    echo "Deleting test resources"
    # We send deletion to a background process as it gets stuck sometimes
    kubectl delete -f $APPLICATION_YAML &
    echo "Waiting for deployment to be deleted..."
    kubectl wait --for=delete deployment --timeout=${TIMEOUT}s --selector="app=${CLUSTER_ID}"
    echo "Deployment deleted"
    kubectl delete cm --selector="app=${CLUSTER_ID},configmap-type=high-availability"
    echo "Cleanup completed"
}

function operator_cleanup_and_exit() {
  echo "Starting cleanup"

  if [ $TRAPPED_EXIT_CODE != 0 ];then
    debug_and_show_logs
  fi
}

function _on_exit_callback {
  # Export the exit code so that it could be used by the callback commands
  export TRAPPED_EXIT_CODE=$?
  # Un-register the callback, to avoid multiple invocations: some shells may treat some signals as subset of others.
  trap "" INT EXIT
  # Fast exit, if there is another keyboard interrupt.
  trap "exit -1" INT

  for command in "${_on_exit_commands[@]-}"; do
    eval "${command}"
  done
}

# Register for multiple signals: some shells interpret them as mutually exclusive.
trap _on_exit_callback INT EXIT

# Helper method to register a command that should be called on current script exit.
# It allows to have multiple "on exit" commands to be called, compared to the built-in `trap "$command" EXIT`.
# Note: tests should not use `trap $command INT|EXIT` directly, to avoid having "Highlander" situation.
function on_exit {
  local command="$1"

  # Keep commands in reverse order, so commands would be executed in LIFO order.
  _on_exit_commands=("${command} `echo "${@:2}"`" "${_on_exit_commands[@]-}")
}

function create_namespace() {

  NAMESPACE_NAME=${1:-default};

  NS=$(kubectl get namespace $NAMESPACE_NAME --ignore-not-found);
  if [[ "$NS" ]]; then
    echo "Skipping creation of namespace $NAMESPACE_NAME - already exists";
  else
    echo "Creating namespace $NAMESPACE_NAME";
    kubectl create namespace $NAMESPACE_NAME;
  fi;

}
