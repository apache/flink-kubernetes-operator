#!/bin/bash
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
#
# Change this file to your need

# Specify the release version for this bundle.
BUNDLE_VERSION=1.3.0

# This is the previous bundle release. This is used to generate catalog image
# which includes the previous version for testing operator upgrade.
PREVIOUS_BUNDLE_VERSION=1.2.0

# Specify an image registry repo to push the bundle and catalog temporarily.
# For example:
# docker.io/your_org/flink-op-bundle:1.3.0
# docker.io/your_org/flink-op-catalog:1.3.0
# NOTE: If you specify a public registry such as docker.io, you need to login first.
# i.e. docker login docker.io -u your_docker_org
DOCKER_REGISTRY="ttl.sh"
DOCKER_ORG="$(head -c24 < /dev/random|base64|tr -dc "[:lower:]"|head -c24)"
IMAGE_TAG="10m"

# Specify the operator image which is used in this bundle.
# ie.: OPERATOR_IMG="docker.io/apache/flink-kubernetes-operator:${BUNDLE_VERSION}"
# https://nightlies.apache.org/flink/flink-kubernetes-operator-docs-main/docs/development/guide/#building-docker-images
OPERATOR_IMG=ghcr.io/apache/flink-kubernetes-operator:main
