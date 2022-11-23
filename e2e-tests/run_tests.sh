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
  echo ""
  echo "Options:"
  echo " -i     The Flink image to use (eg. flink:1.16 / flink:1.15 / flink:1.14)."
  echo " -f     The Flink version(s) to use (eg. v1_16 / v1_15 / v1_14)."
  echo " -m     The run mode(s) to use (eg. native / standalone)."
  echo " -n     The namespace(s) to install the operator to."
  echo " -s     If set then skips the operator build (Make sure that minikube already has a version of the operator)."
  echo " -d     If set then the script will print debug logs."
  echo " -h     Print this help."
  echo ""
  echo "On MAC do not forget to start the 'minikube tunnel' in a different terminal."
  echo "Visit this terminal from time to time, as it will ask you for root password."
  echo "This is needed to forward the rest endpoint requests to port 80."
  echo ""
  echo "Examples:"
  echo "  Start the test_application_operations.sh (default) test on a newly built and deployed operator."
  echo "  The test will use the default settings for every config value:"
  echo "    ./manual_tests.sh"
  echo ""
  echo "  Start a single test using the currently deployed operator which is deployed in the 'flink' namespace:"
  echo "    ./manual_tests.sh -s -namespace flink"
  echo ""
  echo "  Run multiple tests:"
  echo "    ./manual_tests.sh test_application_operations.sh test_sessionjob_operations.sh"
  echo ""
  echo "  Run a single test and show debug logs:"
  echo "     ./manual_tests.sh -d"
  echo ""
  echo "  Use every possible configuration:"
  echo "     ./manual_tests.sh -i 'flink:1.15' -f 'v1_15' -m standalone -n flink -s -k test_sessionjob_operations.sh"
  echo ""
}

image=""
flink_versions="v1_16"
modes="native"
namespaces="default"
skip_operator="false"

versions=("v1_16" "v1_15" "v1_14" "v1_13")
images=("flink:1.16" "flink:1.15" "flink:1.14" "flink:1.13")

while getopts "i:f:m:n:hsd" option; do
   case $option in
      i) image=$OPTARG;;
      f) flink_versions=$OPTARG;;
      m) modes=$OPTARG;;
      n) namespaces=$OPTARG;;
      s) skip_operator="true";;
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
start_minikube
install_cert_manager
if [[ "$skip_operator" = true ]]; then
  echo ""
  echo "SKIPPING operator build"
  echo ""
else
  build_image
fi

if [[ ! -z $(get_operator_pod_name) ]]; then
  echo "Found existing operator. Please remove and restart the script. The following command could be used:"
  echo " helm delete flink-kubernetes-operator"
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
      echo "Unknown Flink version ${flink_version}, please provide an image or a known Flink version"
      exit 1;
    fi
    test_image=${images[${position}]};
  fi

  for mode in ${mode_list[@]}; do
    prepare_tests ${test_image} ${flink_version} ${mode}
    for namespace in ${namespace_list[@]}; do
      for script in ${scripts}; do
        install_operator $namespace
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
        echo "Running test:"
        echo " Namespace:      ${namespace}"
        echo " Flink version:  ${flink_version}"
        echo " Flink image:    ${test_image}"
        echo " Mode:           ${mode}"
        echo " Script:         ${script}"
        echo "-----------------------------------"
        echo
        time bash $ROOT_DIR/e2e-tests/${script} || exit 1
        uninstall_operator $namespace
      done
    done
    revert_tests
  done
done
popd
