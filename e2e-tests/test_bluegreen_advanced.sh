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

# This script tests the ADVANCED Blue/Green transition mode as follows:
# - Create a FlinkBlueGreenDeployment in ADVANCED mode, which starts the "Blue" FlinkDeployment
#   with the gate injected in front of its sink
# - Trigger a transition, which starts the "Green" FlinkDeployment from a savepoint of Blue
# - Verify that the gates hand over: Blue is stopped and deleted, Green becomes the active one
# - Let Green run past the cutover point, then suspend it, so that its output is committed
# - Verify the output of both: every record written exactly once, by Blue up to the cutover point
#   and by Green after it, including the late records Green only writes once past it
#
# The job is examples/flink-bluegreen-advanced-example, in the image
# flink-bluegreen-advanced-example:ci-latest. To build it for a local run, against minikube's
# docker and the Flink version under test:
#   mvn install -DskipTests -pl flink-kubernetes-operator-bluegreen-client -am
#   mvn package -DskipTests -Dflink.version=2.2.1 \
#     -pl flink-kubernetes-operator-bluegreen-client,examples/flink-bluegreen-advanced-example
#   eval $(minikube docker-env)
#   docker build --build-arg FLINK_IMAGE=flink:2.2-java17 -t flink-bluegreen-advanced-example:ci-latest \
#     examples/flink-bluegreen-advanced-example
#
# TODO: an abort after the cutover point is set (for example Green failing once Blue has handed
# over) must redeploy Blue from the transition savepoint through its savepointRedeployNonce, so that
# no record is lost; some may be written twice.

SCRIPT_DIR=$(dirname "$(readlink -f "$0")")
source "${SCRIPT_DIR}/utils.sh"

CLUSTER_ID="bg-advanced-example"
# Kept apart from CLUSTER_ID, which helpers in utils.sh overwrite with the cluster they wait for
BG_CLUSTER_ID=$CLUSTER_ID
BLUE_CLUSTER_ID=$CLUSTER_ID"-blue"
GREEN_CLUSTER_ID=$CLUSTER_ID"-green"

APPLICATION_YAML="${SCRIPT_DIR}/data/bluegreen-advanced.yaml"
APPLICATION_IDENTIFIER="flinkbgdep/$CLUSTER_ID"
BLUE_APPLICATION_IDENTIFIER="flinkdep/$BLUE_CLUSTER_ID"
GREEN_APPLICATION_IDENTIFIER="flinkdep/$GREEN_CLUSTER_ID"
OUTPUT_READER="bg-advanced-output-reader"
OUTPUT_DIR="/opt/flink/volume/output"
TIMEOUT=300

# Every 100th record of the job is late, older than the watermark by 1200 ids. Around the cutover
# point the two watermarks pass it at slightly different records, so a late record there may be
# written by both deployments or by neither: those are not checked.
LATE_ID_MODULO=100
LATE_ID_REMAINDER=50
LATE_IDS_UNCHECKED_AROUND_CUTOVER=300
LATE_IDS_BEHIND=1200

on_exit cleanup_and_exit "$APPLICATION_YAML" $TIMEOUT $CLUSTER_ID

# Prints the committed output of the job, one line per record: <color>,<id>,<eventTime>
function read_output {
  kubectl exec $OUTPUT_READER -- sh -c "cat ${OUTPUT_DIR}/* 2>/dev/null"
}

# Prints the highest record id the given color has committed, or -1
function max_written_id {
  local color=$1
  read_output | awk -F, -v color="$color" 'BEGIN { max = -1 } $1 == color && $2 + 0 > max { max = $2 + 0 } END { print max }'
}

function wait_for_written_id {
  local color=$1
  local id=$2
  local timeout=$3

  echo "Waiting for $color to commit record $id..."
  for i in $(seq 1 ${timeout}); do
    if [ "$(max_written_id $color)" -ge "$id" ]; then
      echo "$color committed record $id."
      return
    fi
    sleep 1
  done
  echo "$color did not commit record $id within a timeout of ${timeout} sec, its highest is $(max_written_id $color)"
  exit 1
}

retry_times 5 30 "kubectl apply -f $APPLICATION_YAML" || exit 1

sleep 1
wait_for_jobmanager_running $BLUE_CLUSTER_ID $TIMEOUT
wait_for_logs $(get_jm_pod_name $BLUE_CLUSTER_ID) "Gate injection applied to StreamGraph" ${TIMEOUT} || exit 1
wait_for_status $BLUE_APPLICATION_IDENTIFIER '.status.lifecycleState' STABLE ${TIMEOUT} || exit 1
wait_for_status $APPLICATION_IDENTIFIER '.status.blueGreenState' ACTIVE_BLUE ${TIMEOUT} || exit 1
kubectl wait --for=condition=Ready --timeout=${TIMEOUT}s pod/$OUTPUT_READER || exit 1

# Some output before the transition, a few checkpoints in
wait_for_written_id BLUE 500 ${TIMEOUT}

echo "Triggering the Blue -> Green transition..."
kubectl patch flinkbgdep ${BG_CLUSTER_ID} --type merge --patch '{"spec":{"template":{"spec":{"flinkConfiguration":{"execution.checkpointing.interval":"4s"}}}}}' || exit 1

wait_for_jobmanager_running $GREEN_CLUSTER_ID $TIMEOUT
wait_for_logs $(get_jm_pod_name $GREEN_CLUSTER_ID) "Gate injection applied to StreamGraph" ${TIMEOUT} || exit 1
wait_for_status $GREEN_APPLICATION_IDENTIFIER '.status.lifecycleState' STABLE ${TIMEOUT} || exit 1
wait_for_status $APPLICATION_IDENTIFIER '.status.blueGreenState' ACTIVE_GREEN ${TIMEOUT} || exit 1
wait_for_deleted $BLUE_APPLICATION_IDENTIFIER ${TIMEOUT}

# Blue was stopped with a savepoint, so all of its output is committed, and it ends about the
# cutover point. Green started further back, from the transition savepoint.
blue_max_id=$(max_written_id BLUE)
echo "Blue wrote up to record $blue_max_id"
wait_for_written_id GREEN $((blue_max_id + LATE_IDS_BEHIND + LATE_IDS_UNCHECKED_AROUND_CUTOVER)) ${TIMEOUT}

echo "Suspending Green, so that all of its output is committed..."
kubectl patch flinkbgdep ${BG_CLUSTER_ID} --type merge --patch '{"spec":{"template":{"spec":{"job":{"state":"suspended"}}}}}' || exit 1
wait_for_status $GREEN_APPLICATION_IDENTIFIER '.status.lifecycleState' SUSPENDED ${TIMEOUT} || exit 1

echo "Checking the output..."
read_output | awk -F, \
  -v late_modulo=$LATE_ID_MODULO \
  -v late_remainder=$LATE_ID_REMAINDER \
  -v unchecked=$LATE_IDS_UNCHECKED_AROUND_CUTOVER '
  {
    count[$2 + 0]++
    if ($2 + 0 > max) max = $2 + 0
  }
  $1 == "BLUE" { blue++; if ($2 + 0 > blue_max) blue_max = $2 + 0 }
  $1 == "GREEN" { green++ }
  END {
    if (!blue || !green) {
      print "Expected records from both deployments, got " blue + 0 " from Blue and " green + 0 " from Green"
      exit 1
    }
    for (id = 0; id <= max; id++) {
      if (id % late_modulo == late_remainder && id > blue_max - unchecked && id < blue_max + unchecked) {
        skipped++
        continue
      }
      if (count[id] != 1) {
        if (errors < 20) print "Record " id " was written " count[id] + 0 " times"
        errors++
      }
    }
    print "Records 0 to " max ": " blue " written by Blue, " green " by Green, Blue ending at " blue_max ", " skipped + 0 " late records around it not checked"
    if (errors) {
      print errors " records were not written exactly once"
      exit 1
    }
  }' || exit 1

echo "Successfully run the Flink Blue/Green ADVANCED mode test"
