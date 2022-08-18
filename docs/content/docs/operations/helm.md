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
helm install --set image.repository=apache/flink-kubernetes-operator --set image.tag={{< stable >}}{{< version >}}{{< /stable >}}{{< unstable >}}latest{{< /unstable >}} flink-kubernetes-operator helm/flink-kubernetes-operator
```

You can also provide your custom values file by using the `-f` flag:
```
helm install -f myvalues.yaml flink-kubernetes-operator helm/flink-kubernetes-operator
```

The configurable parameters of the Helm chart and which default values as detailed in the following table:

| Parameters | Description | Default Value |
| ---------- | ----------- | ------------- |
| watchNamespaces | List of kubernetes namespaces to watch for FlinkDeployment changes, empty means all namespaces. | |
| image.repository | The image repository of flink-kubernetes-operator. | ghcr.io/apache/flink-kubernetes-operator |
| image.pullPolicy | The image pull policy of flink-kubernetes-operator. | IfNotPresent |
| image.tag | The image tag of flink-kubernetes-operator. | latest |
| rbac.create | Whether to enable RBAC to create for said namespaces. | true |
| rbac.nodesRule.create | Whether to add RBAC rule to list nodes which is needed for rest-service exposed as NodePort type. | false |
| operatorPod.annotations | Custom annotations to be added to the operator pod (but not the deployment). | |
| operatorPod.labels | Custom labels to be added to the operator pod (but not the deployment). | |
| operatorPod.env | Custom env to be added to the operator pod. | |
| operatorServiceAccount.create | Whether to enable operator service account to create for flink-kubernetes-operator. | true |
| operatorServiceAccount.annotations | The annotations of operator service account. | |
| operatorServiceAccount.name | The name of operator service account. | flink-operator |
| jobServiceAccount.create | Whether to enable job service account to create for flink jobmanager/taskmanager pods. | true |
| jobServiceAccount.annotations | The annotations of job service account. | "helm.sh/resource-policy": keep |
| jobServiceAccount.name | The name of job service account. | flink |
| operatorVolumeMounts.create | Whether to enable operator volume mounts to create for flink-kubernetes-operator. | false |
| operatorVolumeMounts.data | List of mount paths of operator volume mounts.  | - name: flink-artifacts<br/>&nbsp;&nbsp;mountPath: /opt/flink/artifacts |
| operatorVolumes.create | Whether to enable operator volumes to create for flink-kubernetes-operator. | false |
| operatorVolumes.data | The ConfigMap of operator volumes. | - name: flink-artifacts<br/>&nbsp;&nbsp;hostPath:<br/>&nbsp;&nbsp;&nbsp;&nbsp;path: /tmp/flink/artifacts<br/>&nbsp;&nbsp;&nbsp;&nbsp;type: DirectoryOrCreate |
| podSecurityContext | Defines privilege and access control settings for a pod or container for pod security context.  | runAsUser: 9999<br/>runAsGroup: 9999 |
| operatorSecurityContext | Defines privilege and access control settings for a pod or container for operator security context.  | |
| webhookSecurityContext | Defines privilege and access control settings for a pod or container for webhook security context. | |
| webhook.create | Whether to enable validating and mutating webhooks for flink-kubernetes-operator.                        | true |
| webhook.mutator.create | Enable or disable mutating webhook, overrides `webhook.create` | |
| webhook.validator.create | Enable or disable validating webhook, overrides `webhook.create` | |
| webhook.keystore | The ConfigMap of webhook key store. | useDefaultPassword: true |
| defaultConfiguration.create | Whether to enable default configuration to create for flink-kubernetes-operator. | true |
| defaultConfiguration.append | Whether to append configuration files with configs.  | true |
| defaultConfiguration.flink-conf.yaml | The default configuration of flink-conf.yaml. | kubernetes.operator.metrics.reporter.slf4j.factory.class: org.apache.flink.metrics.slf4j.Slf4jReporterFactory<br/>kubernetes.operator.metrics.reporter.slf4j.interval: 5 MINUTE<br/>kubernetes.operator.reconcile.interval: 15 s<br/>kubernetes.operator.observer.progress-check.interval: 5 s |
| defaultConfiguration.log4j-operator.properties | The default configuration of log4j-operator.properties. | |
| defaultConfiguration.log4j-console.properties | The default configuration of log4j-console.properties. | |
| metrics.port | The metrics port on the container for default configuration. | |
| imagePullSecrets | The image pull secrets of flink-kubernetes-operator. | |
| nameOverride | Overrides the name with the specified name. | |
| fullnameOverride | Overrides the fullname with the specified full name. | |
| jvmArgs.webhook | The JVM start up options for webhook. | |
| jvmArgs.operator |  The JVM start up options for operator. | |
| operatorHealth.port |  Operator health endpoint port to be used by the probes. | 8085 |
| operatorHealth.livenessProbe |  Liveness probe configuration for the operator using the health endpoint. Only time settings should be configured, endpoint is set automatically based on port. | |
| operatorHealth.startupProbe |  Startup probe configuration for the operator using the health endpoint. Only time settings should be configured, endpoint is set automatically based on port. | |

For more information check the [Helm documentation](https://helm.sh/docs/helm/helm_install/).

## Operator webhooks

In order to use the webhooks in the operator, you must install the cert-manager on the Kubernetes cluster:
```
kubectl apply -f https://github.com/jetstack/cert-manager/releases/download/v1.8.2/cert-manager.yaml
```

The webhooks can be disabled during helm install by passing the `--set webhook.create=false` parameter or editing the `values.yaml` directly.

## Watching only specific namespaces

The operator supports watching a specific list of namespaces for FlinkDeployment resources. You can enable it by setting the `--set watchNamespaces={flink-test}` parameter.
When this is enabled role-based access control is only created specifically for these namespaces for the operator and the jobmanagers, otherwise it defaults to cluster scope.

<span class="label label-info">Note</span> When working with webhook in a specified namespace, users should pay attention to the definition of `namespaceSelector.matchExpressions` in `webhook.yaml`. Currently, the default implementation of webhook relies on the `kubernetes.io/metadata.name` label to filter the validation requests
so that only validation requests from the specified namespace will be processed. The `kubernetes.io/metadata.name` label is automatically attached since k8s [1.21.1](https://github.com/kubernetes/kubernetes/blob/master/CHANGELOG/CHANGELOG-1.21.md#v1211).

As a result, for users who run the flink kubernetes operator with older k8s version, they may label the specified namespace by themselves before installing the operator with helm:

```
kubectl label namespace <target namespace name> kubernetes.io/metadata.name=<target namespace name>
```

Besides, users can define their own namespaceSelector to filter the requests due to customized requirements.

For example, if users label their namespace with key-value pair {customized_namespace_key: &lt;target namespace name&gt; }
the corresponding namespaceSelector that only accepts requests from this namespace could be:
```yaml
namespaceSelector:
  matchExpressions:
    - key: customized_namespace_key
    operator: In
    values: [{{- range .Values.watchNamespaces }}{{ . | quote }},{{- end}}]
```
Check out this [document](https://kubernetes.io/docs/reference/access-authn-authz/extensible-admission-controllers/) for more details.

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

defaultConfiguration:
  ...
  log4j-operator.properties: |+
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
