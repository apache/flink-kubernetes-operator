#!/usr/bin/env bash

#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

attach_header () {
	FILE=$1
	TMP_FILE=$FILE.tmp
	HEADER=$2
	mv $FILE $TMP_FILE
	cp $HEADER $FILE
	cat $TMP_FILE >> $FILE
	rm $TMP_FILE
	echo "Added apache header to $FILE"
}


##
## Variables with defaults (if not overwritten by environment)
##
MVN=${MVN:-mvn}

##
## Required variables
##
RELEASE_VERSION=${RELEASE_VERSION}

if [ -z "${RELEASE_VERSION}" ]; then
	echo "RELEASE_VERSION is unset"
	exit 1
fi

# fail immediately
set -o errexit
set -o nounset

CURR_DIR=`pwd`
BASE_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null && pwd )"
PROJECT_ROOT="$( cd "$( dirname "${BASE_DIR}/../../../" )" >/dev/null && pwd )"

# Sanity check to ensure that resolved paths are valid; a LICENSE file should aways exist in project root
if [ ! -f ${PROJECT_ROOT}/LICENSE ]; then
    echo "Project root path ${PROJECT_ROOT} is not valid; script may be in the wrong directory."
    exit 1
fi

if [ "$(uname)" == "Darwin" ]; then
    SHASUM="shasum -a 512"
else
    SHASUM="sha512sum"
fi

###########################

RELEASE_DIR=${PROJECT_ROOT}/release
CLONE_DIR=${RELEASE_DIR}/flink-kubernetes-operator-tmp-clone

rm -rf ${RELEASE_DIR}
mkdir ${RELEASE_DIR}

# delete the temporary release directory on error
trap 'rm -rf ${RELEASE_DIR}' ERR

echo "Creating source package"

# create a temporary git clone to ensure that we have a pristine source release
git clone ${PROJECT_ROOT} ${CLONE_DIR}

cd ${CLONE_DIR}
rsync -a \
  --exclude ".git" --exclude ".gitignore" \
  --exclude ".asf.yaml" \
  --exclude "target" \
  --exclude ".idea" --exclude "*.iml" \
  --exclude ".travis.yml" \
  . flink-kubernetes-operator-${RELEASE_VERSION}

apache_header=${CLONE_DIR}/flink-kubernetes-operator-${RELEASE_VERSION}/tools/releasing/apache_header.yaml
# Package helm chart
commit_hash=$(git log -1 --pretty=format:%h --abbrev=7)

# Attach apache header to generated crd
cd flink-kubernetes-operator-${RELEASE_VERSION}/helm/flink-kubernetes-operator/crds
for FILE in *.yml; do attach_header $FILE $apache_header; done
cd ${CLONE_DIR}

# TODO: We might want to be more specific here later on what to replace
perl -pi -e "s#^  repository: .*#  repository: ghcr.io/apache/flink-kubernetes-operator#" flink-kubernetes-operator-${RELEASE_VERSION}/helm/flink-kubernetes-operator/values.yaml
perl -pi -e "s#^  tag: .*#  tag: \"${commit_hash}\"#" flink-kubernetes-operator-${RELEASE_VERSION}/helm/flink-kubernetes-operator/values.yaml

tar czf ${RELEASE_DIR}/flink-kubernetes-operator-${RELEASE_VERSION}-helm.tgz -C flink-kubernetes-operator-${RELEASE_VERSION}/helm flink-kubernetes-operator

helm repo index ${RELEASE_DIR}
attach_header ${RELEASE_DIR}/index.yaml $apache_header

gpg --armor --detach-sig ${RELEASE_DIR}/flink-kubernetes-operator-${RELEASE_VERSION}-helm.tgz
gpg --armor --detach-sig ${RELEASE_DIR}/index.yaml

# Package sources

tar czf ${RELEASE_DIR}/flink-kubernetes-operator-${RELEASE_VERSION}-src.tgz flink-kubernetes-operator-${RELEASE_VERSION}
gpg --armor --detach-sig ${RELEASE_DIR}/flink-kubernetes-operator-${RELEASE_VERSION}-src.tgz


cd ${RELEASE_DIR}
${SHASUM} flink-kubernetes-operator-${RELEASE_VERSION}-src.tgz > flink-kubernetes-operator-${RELEASE_VERSION}-src.tgz.sha512
${SHASUM} flink-kubernetes-operator-${RELEASE_VERSION}-helm.tgz > flink-kubernetes-operator-${RELEASE_VERSION}-helm.tgz.sha512
${SHASUM} index.yaml > index.yaml.sha512

rm -rf ${CLONE_DIR}

echo "Done. Source release package and signatures created under ${RELEASE_DIR}/."

cd ${CURR_DIR}
