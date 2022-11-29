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

ROOT_DIR=$(dirname $(dirname "$(readlink -f "$0")"))
source "${ROOT_DIR}/e2e-tests/utils.sh"

function help() {
  # Display Help
  echo "Script to run the flink kubernetes operator e2e tests."
  echo
  echo "Syntax: run_tests.sh [-i|-f|-m|-n|-s|-c|-h] scripts"
  echo
  echo "Options:"
  echo " -i     The Flink image to use (eg. flink:1.16 / flink:1.15 / flink:1.14). Only needed if the default docker image for the Flink version need to be changed."
  echo " -f     The Flink version(s) to use (eg. v1_16 / v1_15 / v1_14). Could contain multiple values separated by ','"
  echo " -m     The run mode(s) to use (eg. native / standalone). Could contain multiple values separated by ','"
  echo " -n     The namespace(s) to install the operator to. Could contain multiple values separated by ','"
  echo " -s     If set then skips the operator build (Make sure that minikube already has a version of the operator with the name of 'flink-kubernetes-operator:ci-latest')."
  echo " -k     After the test run the operator is not uninstalled. Will exit after the first test run."
  echo " -d     If set then the script will print debug logs."
  echo " -h     Print this help."
  echo
  echo "On MAC do not forget to start the 'minikube tunnel' in a different terminal."
  echo "Visit this terminal from time to time, as it will ask you for root password."
  echo "This is needed to forward the rest endpoint requests to port 80."
  echo
  echo "Examples:"
  echo "  Start the test_application_operations.sh (default) test on a newly built and deployed operator."
  echo "  The test will use the default settings for every config value:"
  echo "    ./run_tests.sh"
  echo
  echo "  Start a single test using the currently compiled docker image of the operator which should be available on the minikube."
  echo "  The operator will be deployed in the 'flink' namespace:"
  echo "    ./run_tests.sh -s -namespace flink"
  echo
  echo "  Run multiple tests:"
  echo "    ./run_tests.sh test_application_operations.sh test_sessionjob_operations.sh"
  echo
  echo "  Run a single test and show debug logs:"
  echo "    ./run_tests.sh -d"
  echo
  echo "  Use every possible configuration:"
  echo "    ./run_tests.sh -i 'flink:1.15' -f 'v1_15' -m standalone -n flink -s -k test_sessionjob_operations.sh"
  echo
  echo "  Run every possible test configuration:"
  echo "    ./run_tests.sh -f v1_16,v1_15,v1_14,v1_13 -m native,standalone -n flink,default -d test_multi_sessionjob.sh test_application_operations.sh test_application_kubernetes_ha.sh test_sessionjob_kubernetes_ha.sh test_sessionjob_operations.sh"
  echo
  echo
  echo "  Use the script to compile and deploy the current version of the operator, and check the deployment:"
  echo "    ./run_tests.sh -k"
  echo
}

# Default values
image=""
flink_versions="v1_16"
modes="native"
namespaces="default"
skip_operator="false"
keep_operator="false"

# Flink version to Flink docker image mapping. Add new version and image here if it is supported.
versions=("v1_16" "v1_15" "v1_14" "v1_13")
images=("flink:1.16" "flink:1.15" "flink:1.14" "flink:1.13")

while getopts "i:f:m:n:hsdk" option; do
   case $option in
      i) image=$OPTARG;;
      f) flink_versions=$OPTARG;;
      m) modes=$OPTARG;;
      n) namespaces=$OPTARG;;
      s) skip_operator="true";;
      k) keep_operator="true";;
      d) DEBUG="true";;
      h) # display Help
         help
         exit;;
     \?) # Invalid option
         echo "Error: Invalid option"
         exit;;
   esac
done

scripts=${@:$OPTIND}
if [[ -z "${scripts}" ]]; then
  scripts="test_application_operations.sh"
fi

echo
echo "-----------------------------------"
echo "Start testing with the following parameters:"
echo " Image:                  ${image}"
echo " Flink version(s):       ${flink_versions}"
echo " Mode(s):                ${modes}"
echo " Operator namespace(s):  ${namespaces}"
echo " Skip operator build:    ${skip_operator}"
echo " Script() to run:        ${scripts}"
echo "-----------------------------------"

pushd $ROOT_DIR
echo
echo "-----------------------------------"
echo "Starting minikube"
echo "-----------------------------------"
echo
time start_minikube

echo
echo "-----------------------------------"
echo "Installing certificate manager"
echo "-----------------------------------"
echo
time install_cert_manager

if [[ "$skip_operator" = true ]]; then
  echo
  echo "-----------------------------------"
  echo "Skipping operator build"
  echo "-----------------------------------"
else
  echo
  echo "-----------------------------------"
  echo "Building operator"
  echo "-----------------------------------"
  echo
  time build_image
fi

if [[ ! -z $(get_operator_pod_name 2>/dev/null) ]]; then
  operator_pod_namespace=$(get_operator_pod_namespace)
  echo
  echo "-----------------------------------"
  echo "Found existing operator. Please remove and restart the script. The following commands could be used:"
  echo " helm delete flink-kubernetes-operator -n ${operator_pod_namespace}"
  echo " kubectl delete namespace flink"
  echo " kubectl delete serviceaccount flink"
  echo " kubectl delete role flink"
  echo " kubectl delete rolebinding flink-role-binding"
  echo "-----------------------------------"
  echo
  exit 1;
fi

IFS=',' read -ra flink_version_list <<< "${flink_versions}"
IFS=',' read -ra mode_list <<< "${modes}"
IFS=',' read -ra namespace_list <<< "${namespaces}"

for flink_version in ${flink_version_list[@]}; do
  test_image=${image}
  if [[ -z ${test_image} ]]; then
    position=$(get_position ${flink_version} ${versions[@]})
    if [[ -z ${position} ]]; then
      echo
      echo "-----------------------------------"
      echo "Unknown Flink version ${flink_version}, please provide an image or a known Flink version"
      echo "-----------------------------------"
      echo
      exit 1;
    fi
    test_image=${images[${position}]};
  fi

  for mode in ${mode_list[@]}; do
    prepare_tests ${test_image} ${flink_version} ${mode}
    for namespace in ${namespace_list[@]}; do
      for script in ${scripts}; do
        if [[ ${script} = "test_multi_sessionjob.sh" && ${namespace} = "default" ]]; then
          echo
          echo "-----------------------------------"
          echo "Skipping ${script} on ${namespace} namespace"
          echo "-----------------------------------"
          echo
          continue
        fi
        echo
        echo "-----------------------------------"
        echo "Installing operator to '${namespace}' namespace"
        echo "-----------------------------------"
        echo
        install_operator $namespace
        echo
        echo "-----------------------------------"
        echo "Running test:"
        echo " Namespace:      ${namespace}"
        echo " Flink version:  ${flink_version}"
        echo " Flink image:    ${test_image}"
        echo " Mode:           ${mode}"
        echo " Script:         ${script}"
        echo "-----------------------------------"
        echo
        time bash $ROOT_DIR/e2e-tests/${script}
        if [[ $? -ne 0 ]]; then
          echo
          echo "-----------------------------------"
          echo "Test failed: $ROOT_DIR/e2e-tests/${script}"
          echo "-----------------------------------"
          echo
          exit 1
        fi
        echo
        echo "-----------------------------------"
        echo "Test succeeded: $ROOT_DIR/e2e-tests/${script}"
        echo "-----------------------------------"
        if [[ "$keep_operator" = true ]]; then
          echo
          echo "-----------------------------------"
          echo "Keeping operator and exiting"
          echo "-----------------------------------"
          echo
          exit 0
        fi
        echo
        echo "-----------------------------------"
        echo "Removing operator from '${namespace}' namespace"
        echo "-----------------------------------"
        echo
        uninstall_operator $namespace
      done
    done
    revert_tests
  done
done
popd
