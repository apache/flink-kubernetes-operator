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

The operator installation is managed by a helm chart. To install with the chart bundled in the source code run:

```
helm install flink-kubernetes-operator helm/flink-kubernetes-operator
```

To install from our Helm Chart Reporsitory run:

```
helm repo add flink-operator-repo https://downloads.apache.org/flink/flink-kubernetes-operator-<OPERATOR-VERSION>/
helm install flink-kubernetes-operator flink-operator-repo/flink-kubernetes-operator
```

Alternatively to install the operator (and also the helm chart) to a specific namespace add the arguments `--namespace` and `--create-namespace` ex:

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
Note, you should escape special characters in your `--set` lines, for example:
```
helm install --set defaultConfiguration."log4j-operator\.properties"=rootLogger.level\=DEBUG flink-kubernetes-operator helm/flink-kubernetes-operator
```

You can also provide your custom values file by using the `-f` flag:
```
helm install -f myvalues.yaml flink-kubernetes-operator helm/flink-kubernetes-operator
```

The configurable parameters of the Helm chart and which default values as detailed in the following table:

| Parameters                                     | Description                                                                                                                                                    | Default Value                                                                                                                                                                                                                                                                                  |
|------------------------------------------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| defaultConfiguration.append                    | Whether to append configuration files with configs.                                                                                                            | true                                                                                                                                                                                                                                                                                           |
| defaultConfiguration.config.yaml               | The newer configuration file format for flink that will be enforced in Flink 2.0. Note this was introduced in flink 1.19.                                      | kubernetes.operator.metrics.reporter.slf4j.factory.class: org.apache.flink.metrics.slf4j.Slf4jReporterFactory<br/>kubernetes.operator.metrics.reporter.slf4j.interval: 5 MINUTE<br/>kubernetes.operator.reconcile.interval: 15 s<br/>kubernetes.operator.observer.progress-check.interval: 5 s |
| defaultConfiguration.create                    | Whether to enable default configuration to create for flink-kubernetes-operator.                                                                               | true                                                                                                                                                                                                                                                                                           |
| defaultConfiguration.flink-conf.yaml           | The default configuration of flink-conf.yaml.                                                                                                                  | kubernetes.operator.metrics.reporter.slf4j.factory.class: org.apache.flink.metrics.slf4j.Slf4jReporterFactory<br/>kubernetes.operator.metrics.reporter.slf4j.interval: 5 MINUTE<br/>kubernetes.operator.reconcile.interval: 15 s<br/>kubernetes.operator.observer.progress-check.interval: 5 s |
| defaultConfiguration.log4j-console.properties  | The default configuration of log4j-console.properties.                                                                                                         |                                                                                                                                                                                                                                                                                                |
| defaultConfiguration.log4j-operator.properties | The default configuration of log4j-operator.properties.                                                                                                        |                                                                                                                                                                                                                                                                                                |
| defaultConfiguration.logback-operator.xml      | The default configuration of logback-operator.xml. Used when `logging.framework` is set to `logback`.                                                          |                                                                                                                                                                                                                                                                                                |
| defaultConfiguration.logback-console.xml       | The default configuration of logback-console.xml. Used when `logging.framework` is set to `logback`.                                                           |                                                                                                                                                                                                                                                                                                |
| fullnameOverride                               | Overrides the fullname with the specified full name.                                                                                                           |                                                                                                                                                                                                                                                                                                |
| image.digest                                   | The image tag of flink-kubernetes-operator. If set then it takes precedence and the image tag will be ignored.                                                 |                                                                                                                                                                                                                                                                                                |
| image.pullPolicy                               | The image pull policy of flink-kubernetes-operator.                                                                                                            | IfNotPresent                                                                                                                                                                                                                                                                                   |
| image.repository                               | The image repository of flink-kubernetes-operator.                                                                                                             | ghcr.io/apache/flink-kubernetes-operator                                                                                                                                                                                                                                                       |
| image.tag                                      | The image tag of flink-kubernetes-operator.                                                                                                                    | latest                                                                                                                                                                                                                                                                                         |
| imagePullSecrets                               | The image pull secrets of flink-kubernetes-operator.                                                                                                           |                                                                                                                                                                                                                                                                                                |
| jobServiceAccount.annotations                  | The annotations of job service account.                                                                                                                        | "helm.sh/resource-policy": keep                                                                                                                                                                                                                                                                |
| jobServiceAccount.create                       | Whether to enable job service account to create for flink jobmanager/taskmanager pods.                                                                         | true                                                                                                                                                                                                                                                                                           |
| jobServiceAccount.name                         | The name of job service account.                                                                                                                               | flink                                                                                                                                                                                                                                                                                          |
| jvmArgs.operator                               | The JVM start up options for operator.                                                                                                                         |                                                                                                                                                                                                                                                                                                |
| jvmArgs.webhook                                | The JVM start up options for webhook.                                                                                                                          |                                                                                                                                                                                                                                                                                                |
| logging.framework                              | The logging framework to use for the operator. Supported values: `log4j2`, `logback`. Controls which config files are mounted and which JVM flag is passed.    | log4j2                                                                                                                                                                                                                                                                                         |
| metrics.port                                   | The metrics port on the container for default configuration.                                                                                                   |                                                                                                                                                                                                                                                                                                |
| nameOverride                                   | Overrides the name with the specified name.                                                                                                                    |                                                                                                                                                                                                                                                                                                |
| operatorHealth.livenessProbe                   | Liveness probe configuration for the operator using the health endpoint. Only time settings should be configured, endpoint is set automatically based on port. |                                                                                                                                                                                                                                                                                                |
| operatorHealth.port                            | Operator health endpoint port to be used by the probes.                                                                                                        | 8085                                                                                                                                                                                                                                                                                           |
| operatorHealth.startupProbe                    | Startup probe configuration for the operator using the health endpoint. Only time settings should be configured, endpoint is set automatically based on port.  |                                                                                                                                                                                                                                                                                                |
| operatorPod.annotations                        | Custom annotations to be added to the operator pod (but not the deployment).                                                                                   |                                                                                                                                                                                                                                                                                                |
| operatorPod.dnsConfig                          | DNS configuration to be used by the operator pod.                                                                                                              |                                                                                                                                                                                                                                                                                                |
| operatorPod.dnsPolicy                          | DNS policy to be used by the operator pod.                                                                                                                     |                                                                                                                                                                                                                                                                                                |
| operatorPod.env                                | Custom env to be added to the operator pod.                                                                                                                    |                                                                                                                                                                                                                                                                                                |
| operatorPod.envFrom                            | Custom envFrom settings to be added to the operator pod.                                                                                                       |                                                                                                                                                                                                                                                                                                |
| operatorPod.labels                             | Custom labels to be added to the operator pod and deployment.                                                                                                  |                                                                                                                                                                                                                                                                                                |
| operatorPod.nodeSelector                       | Custom nodeSelector to be added to the operator pod.                                                                                                           |                                                                                                                                                                                                                                                                                                |
| operatorPod.resources                          | Custom resources block to be added to the operator pod on main container.                                                                                      |                                                                                                                                                                                                                                                                                                |
| operatorPod.tolerations                        | Custom tolerations to be added to the operator pod.                                                                                                            |                                                                                                                                                                                                                                                                                                |
| operatorPod.topologySpreadConstraints          | Custom topologySpreadConstraints to be added to the operator pod.                                                                                              |                                                                                                                                                                                                                                                                                                |
| operatorPod.webhook.container.env              | Custom env to be added to the flink-webhook container                                                                                                          |                                                                                                                                                                                                                                                                                                |
| operatorPod.webhook.resources                  | Custom resources block to be added to the operator pod on flink-webhook container.                                                                             |                                                                                                                                                                                                                                                                                                |
| operatorSecurityContext                        | Defines privilege and access control settings for a pod or container for operator security context.                                                            |                                                                                                                                                                                                                                                                                                |
| operatorServiceAccount.annotations             | The annotations of operator service account.                                                                                                                   |                                                                                                                                                                                                                                                                                                |
| operatorServiceAccount.create                  | Whether to enable operator service account to create for flink-kubernetes-operator.                                                                            | true                                                                                                                                                                                                                                                                                           |
| operatorServiceAccount.name                    | The name of operator service account.                                                                                                                          | flink-operator                                                                                                                                                                                                                                                                                 |
| operatorVolumeMounts.create                    | Whether to enable operator volume mounts to create for flink-kubernetes-operator.                                                                              | false                                                                                                                                                                                                                                                                                          |
| operatorVolumeMounts.data                      | List of mount paths of operator volume mounts.                                                                                                                 | - name: flink-artifacts<br/>&nbsp;&nbsp;mountPath: /opt/flink/artifacts                                                                                                                                                                                                                        |
| operatorVolumes.create                         | Whether to enable operator volumes to create for flink-kubernetes-operator.                                                                                    | false                                                                                                                                                                                                                                                                                          |
| operatorVolumes.data                           | The ConfigMap of operator volumes.                                                                                                                             | - name: flink-artifacts<br/>&nbsp;&nbsp;hostPath:<br/>&nbsp;&nbsp;&nbsp;&nbsp;path: /tmp/flink/artifacts<br/>&nbsp;&nbsp;&nbsp;&nbsp;type: DirectoryOrCreate                                                                                                                                   |
| podSecurityContext                             | Defines privilege and access control settings for a pod or container for pod security context.                                                                 | runAsUser: 9999<br/>runAsGroup: 9999                                                                                                                                                                                                                                                           |
| postStart                                      | The postStart hook configuration for the main container.                                                                                                       |                                                                                                                                                                                                                                                                                                |
| rbac.create                                    | Whether to enable RBAC to create for said namespaces.                                                                                                          | true                                                                                                                                                                                                                                                                                           |
| rbac.nodesRule.create                          | Whether to add RBAC rule to list nodes which is needed for rest-service exposed as NodePort type.                                                              | false                                                                                                                                                                                                                                                                                          |
| replicas                                       | Operator replica count. Must be 1 unless leader election is configured.                                                                                        | 1                                                                                                                                                                                                                                                                                              |
| strategy.type                                  | Operator pod upgrade strategy. Must be Recreate unless leader election is configured.                                                                          | Recreate                                                                                                                                                                                                                                                                                       |
| tls.create                                     | Whether to mount an optional secret containing a tls truststore for the flink-kubernetes-operator.                                                             | false                                                                                                                                                                                                                                                                                          |
| tls.secretKeyRef.key                           | The key that holds this password                                                                                                                               | password                                                                                                                                                                                                                                                                                       |
| tls.secretKeyRef.name                          | The name of the secret containing the password for the java keystore/truststore                                                                                | operator-certificate-password                                                                                                                                                                                                                                                                  |
| tls.secretName                                 | The name of the tls secret                                                                                                                                     | flink-operator-cert                                                                                                                                                                                                                                                                            |
| watchNamespaces                                | List of kubernetes namespaces to watch for FlinkDeployment changes, empty means all namespaces.                                                                |                                                                                                                                                                                                                                                                                                |
| webhook.create                                 | Whether to enable validating and mutating webhooks for flink-kubernetes-operator.                                                                              | true                                                                                                                                                                                                                                                                                           |
| webhook.keystore                               | The ConfigMap of webhook key store.                                                                                                                            | useDefaultPassword: true                                                                                                                                                                                                                                                                       |
| webhook.mutator.create                         | Enable or disable mutating webhook, overrides `webhook.create`                                                                                                 |                                                                                                                                                                                                                                                                                                |
| webhook.serviceLabels                          | The labels for flink-operator-webhook-service-resource.                                                                                                        |                                                                                                                                                                                                                                                                                                |
| webhook.validator.create                       | Enable or disable validating webhook, overrides `webhook.create`                                                                                               |                                                                                                                                                                                                                                                                                                |
| webhookSecurityContext                         | Defines privilege and access control settings for a pod or container for webhook security context.                                                             |                                                                                                                                                                                                                                                                                                |

For more information check the [Helm documentation](https://helm.sh/docs/helm/helm_install/).

__Notice__: The pod resources should be set as your workload in different environments to archive a matched K8s pod QoS. See also [Pod Quality of Service Classes](https://kubernetes.io/docs/concepts/workloads/pods/pod-qos/#quality-of-service-classes).

## Operator webhooks

In order to use the webhooks in the operator, you must install the cert-manager on the Kubernetes cluster:
```
kubectl apply -f https://github.com/jetstack/cert-manager/releases/download/v1.18.2/cert-manager.yaml
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

If you are using [Argo CD](https://argoproj.github.io) to manage the operator,
the simplest example could look like this.

```yaml
apiVersion: argoproj.io/v1alpha1
kind: Application
metadata:
  name: flink-kubernetes-operator
spec:
  source:
    repoURL: https://github.com/apache/flink-kubernetes-operator
    targetRevision: main
    path: helm/flink-kubernetes-operator
...
```

Check out [Argo CD documents](https://argo-cd.readthedocs.io/en/stable/user-guide/helm/) for more details.

## Advanced customization techniques
The Helm chart does not aim to provide configuration options for all the possible deployment scenarios of the Operator. There are use cases for injecting common tools and/or sidecars in most enterprise environments that cannot be covered by public Helm charts.

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
