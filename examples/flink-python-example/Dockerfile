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
# Check https://nightlies.apache.org/flink/flink-docs-master/docs/deployment/resource-providers/standalone/docker/#using-flink-python-on-docker for more details
FROM flink:2.2

# The flink:2.x base image runs as the non-root `flink` user; switch to root
# to install system packages, then drop privileges again before adding the job.
USER root

# install python3 and pip3
RUN apt-get update -y && \
    apt-get install -y python3 python3-pip python3-dev && \
    rm -rf /var/lib/apt/lists/* && \
    ln -s /usr/bin/python3 /usr/bin/python

# install PyFlink (PyFlink 2.2 supports Python 3.9, 3.10, 3.11, 3.12).
# --break-system-packages is required because the base image is Ubuntu 24.04
# (Noble), which enforces PEP 668.
RUN pip3 install --break-system-packages apache-flink==2.2.0

# add python script
USER flink
RUN mkdir /opt/flink/usrlib
ADD python_demo.py /opt/flink/usrlib/python_demo.py
