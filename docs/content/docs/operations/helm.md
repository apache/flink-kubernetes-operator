---
title: "Helm"
weight: 1
type: docs
aliases:
- /operations/helm.html
---
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

# Helm installation

The operator installation is managed by a helm chart. To install run:

```
helm install flink-kubernetes-operator helm/flink-kubernetes-operator
```

Alternatively to install the operator (and also the helm chart) to a specific namespace:

```
helm install flink-kubernetes-operator helm/flink-kubernetes-operator --namespace flink --create-namespace
```

Note that in this case you will need to update the namespace in the examples accordingly or the `default`
namespace to the [watched namespaces](#watching-only-specific-namespaces).

## Overriding configuration parameters during Helm install

Helm provides different ways to override the default installation parameters (contained in `values.yaml`) for the Helm chart.

To override single parameters you can use `--set`, for example:
```
helm install --set image.repository=apache/flink-kubernetes-operator --set image.tag=1.0.1 flink-kubernetes-operator helm/flink-kubernetes-operator
```

You can also provide your custom values file by using the `-f` flag:
```
helm install -f myvalues.yaml flink-kubernetes-operator helm/flink-kubernetes-operator
```

For more information check the [Helm documentation](https://helm.sh/docs/helm/helm_install/).

## Validating webhook

In order to use the webhook for FlinkDeployment validation, you must install the cert-manager on the Kubernetes cluster:
```
kubectl apply -f https://github.com/jetstack/cert-manager/releases/download/v1.7.1/cert-manager.yaml
```

The webhook can be disabled during helm install by passing the `--set webhook.create=false` parameter or editing the `values.yaml` directly.

## Watching only specific namespaces

The operator supports watching a specific list of namespaces for FlinkDeployment resources. You can enable it by setting the `--set watchNamespaces={flink-test}` parameter.
When this is enabled role-based access control is only created specifically for these namespaces for the operator and the jobmanagers, otherwise it defaults to cluster scope.

## Working with Argo CD

If you are using [Argo CD](https://argoproj.github.io) to manage the operator, you will encounter the issue which complains the CRDs too long. Same with [this issue](https://github.com/prometheus-operator/prometheus-operator/issues/4439).
The recommended solution is to split the operator into two Argo apps, such as:

* The first app just for installing the CRDs with `Replace=true` directly, snippet:

```yaml
apiVersion: argoproj.io/v1alpha1
kind: Application
metadata:
  name: flink-kubernetes-operator-crds
spec:
  source:
    repoURL: https://github.com/apache/flink-kubernetes-operator
    targetRevision: main
    path: helm/flink-kubernetes-operator/crds
    syncOptions:
    - Replace=true
...
```

* The second app that installs the Helm chart with `skipCrds=true` (new feature in Argo CD 2.3.0), snippet:

```yaml
apiVersion: argoproj.io/v1alpha1
kind: Application
metadata:
  name: flink-kubernetes-operator-skip-crds
spec:
  source:
    repoURL: https://github.com/apache/flink-kubernetes-operator
    targetRevision: main
    path: helm/flink-kubernetes-operator
    helm:
      skipCrds: true
...
```

## Advanced customization techniques
The Helm chart does not aim to provide configuration options for all the possible deployment scenarios of the Operator. There are use cases for injecting common tools and/or sidecars in most enterprise environments that simply cannot be covered by public Helm charts.

Fortunately, [post rendering](https://helm.sh/docs/topics/advanced/#post-rendering) in Helm gives you the ability to manually manipulate manifests before they are installed on a Kubernetes cluster. This allows users to use tools like [kustomize](https://kustomize.io) to apply configuration changes without the need to fork public charts.

The GitHub repository for the Operator contains a simple [example](https://github.com/apache/flink-kubernetes-operator/tree/main/examples/kustomize) on how to augment the Operator Deployment with a [fluent-bit](https://docs.fluentbit.io/manual) sidecar container and adjust container resources using `kustomize`.

The example demonstrates that we can still use a `values.yaml` file to override the default Helm values for changing the log configuration, for example:

```yaml

operatorConfiguration:
  ...
  log4j2.properties: |+
    rootLogger.appenderRef.file.ref = LogFile
    appender.file.name = LogFile
    appender.file.type = File
    appender.file.append = false
    appender.file.fileName = ${sys:log.file}
    appender.file.layout.type = PatternLayout
    appender.file.layout.pattern = %d{yyyy-MM-dd HH:mm:ss,SSS} %-5p %-60c %x - %m%n

jvmArgs:
  webhook: "-Dlog.file=/opt/flink/log/webhook.log -Xms256m -Xmx256m"
  operator: "-Dlog.file=/opt/flink/log/operator.log -Xms2048m -Xmx2048m"
```

But we cannot ingest our fluent-bit sidecar for example unless we patch the deployment using `kustomize`

```yaml
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

apiVersion: apps/v1
kind: Deployment
metadata:
  name: not-important
spec:
  template:
    spec:
      containers:
        - name: flink-kubernetes-operator
          volumeMounts:
            - name: flink-log
              mountPath: /opt/flink/log
          resources:
            requests:
              memory: "2.5Gi"
              cpu: "1000m"
            limits:
              memory: "2.5Gi"
              cpu: "2000m"
        - name: flink-webhook
          volumeMounts:
            - name: flink-log
              mountPath: /opt/flink/log
          resources:
            requests:
              memory: "0.5Gi"
              cpu: "200m"
            limits:
              memory: "0.5Gi"
              cpu: "500m"
        - name: fluentbit
          image: fluent/fluent-bit:1.8.12
          command: [ 'sh','-c','/fluent-bit/bin/fluent-bit -i tail -p path=/opt/flink/log/*.log -p multiline.parser=java -o stdout' ]
          volumeMounts:
            - name: flink-log
              mountPath: /opt/flink/log
      volumes:
        - name: flink-log
          emptyDir: { }
```

You can try out the example using the following command:
```shell
helm install flink-kubernetes-operator helm/flink-kubernetes-operator -f examples/kustomize/values.yaml --post-renderer examples/kustomize/render
```

By examining the sidecar output you should see that the logs from both containers are being processed from the shared folder:

```shell
[2022/04/06 10:04:36] [ info] [input:tail:tail.0] inotify_fs_add(): inode=3812411 watch_fd=1 name=/opt/flink/log/operator.log
[2022/04/06 10:04:36] [ info] [input:tail:tail.0] inotify_fs_add(): inode=3812412 watch_fd=2 name=/opt/flink/log/webhook.log
```

Check out the [kustomize](https://github.com/kubernetes-sigs/kustomize/tree/master/examples) repo for more advanced examples.
> Please note that post-render mechanism will always override the Helm template values.
