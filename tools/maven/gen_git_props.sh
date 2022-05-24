#!/usr/bin/env bash

################################################################################
#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
# limitations under the License.
################################################################################

# fail immediately
set -o errexit
set -o nounset

CURR_DIR=`pwd`
BASE_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null && pwd )"
PROJECT_ROOT="${BASE_DIR}/../../"

TARGET_DIR="$PROJECT_ROOT/flink-kubernetes-operator/target"
mkdir -p $TARGET_DIR
touch $TARGET_DIR/git.properties

if [ -d $PROJECT_ROOT/.git ]; then
    GIT_COMMIT_ID_ABBREV="$( git log -n1 --pretty=format:%h )"
    GIT_COMMIT_TIME="$( git log -n1 --date=format:'%Y-%m-%dT%H:%M:%S%z' --pretty=format:%cd )"
    echo "git.commit.id.abbrev=$GIT_COMMIT_ID_ABBREV" >> $TARGET_DIR/git.properties
    echo "git.commit.time=$GIT_COMMIT_TIME" >> $TARGET_DIR/git.properties
fi
