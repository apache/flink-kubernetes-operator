<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
-->

# Flink Kubernetes Operator Python Example

## Overview

This is an end-to-end example of running Flink Python jobs using the Flink Kubernetes Operator.


*What's in this example?*

 1. Python script of a simple streaming job
 2. DockerFile to build custom image with pyflink and python demo
 3. Example YAML for submitting the python job using the operator

## How does it work?

Flink supports Python jobs in application mode by utilizing `org.apache.flink.client.python.PythonDriver` class as the 
entry class. With the Flink Kubernetes Operator, we can reuse this class to run Python jobs as well. 

The class is packaged in flink-python_${scala_version}-${flink_version}.jar which is in the default Flink image.
So we do not need to create a new job jar. Instead, we just set `entryClass` of the job crd to 
`org.apache.flink.client.python.PythonDriver`. After applying the job yaml, the launched job manager pod will run the `main()` 
method of PythonDriver and parse arguments declared in the `args` field of the job crd.

Note, in `args` field, users must either specify `-py` option or `-pym` option. 
Besides, order of elements in `args` field matters: due to current parsing process, Flink specific options(e.g -pyfs, -py) must be placed at first and 
user-defined arguments should be placed in the end. Check the [doc](https://nightlies.apache.org/flink/flink-docs-master/docs/deployment/cli/#submitting-pyflink-jobs) for more details about PyFlink arguments.

A working example would be:
```yaml
args: ["-pyfs", "/opt/flink/usrlib/pythonjob/python_demo.py", "-pyclientexec", "/usr/local/bin/python3", "-py", "/opt/flink/usrlib/pythonjob/python_demo.py", "-myarg", "123"]
```
But the following will throw exception:
```yaml
args: ["-myarg", "123", "-pyfs", "/opt/flink/usrlib/pythonjob/python_demo.py", "-pyclientexec", "/usr/local/bin/python3", "-py", "/opt/flink/usrlib/pythonjob/python_demo.py"]
```

## Usage

The following steps assume that you have the Flink Kubernetes Operator installed and running in your environment.


**Step 1**: Put your Python script files under the `flink-python-example` directory and add your Python script into the 
Dockerfile

**Step 2**: Build docker image

Check this [doc](https://nightlies.apache.org/flink/flink-docs-master/docs/deployment/resource-providers/standalone/docker/#using-flink-python-on-docker) for more details about building Pyflink image. Note, Pyflink 1.15.3 is only supported on x86 arch.  
```bash
# Uncomment when building for local minikube env:
# eval $(minikube docker-env)

DOCKER_BUILDKIT=1 docker build . -t flink-python-example:latest
```
This step will create an image based on an official Flink base image including the Python scripts.

**Step 4**: Create FlinkDeployment Yaml and Submit

Edit the included `python-example.yaml` so that the `job.args` section points to the Python script that you wish to execute, then submit it.

```bash
kubectl apply -f python-example.yaml
```

It is possible to reuse the above image for different Python scripts as long as users make them accessible on Job Manager Pod(e.g using PodTemplate with mounted storage).
