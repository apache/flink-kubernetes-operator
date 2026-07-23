---
title: "Cert Manager"
weight: 3
type: docs
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

# Cert Manager

The operator's admission webhooks serve over TLS, and the Helm chart delegates the certificate handling to [cert-manager](https://cert-manager.io/).

## Installation

Whenever the webhooks are enabled (`webhook.create`, on by default), cert-manager must be installed on the Kubernetes cluster before the operator:

```
kubectl apply -f https://github.com/cert-manager/cert-manager/releases/latest/download/cert-manager.yaml
```

{{< hint warning >}}
The commands on this page always install and remove the newest cert-manager release. To pin a specific version, or to match an older Kubernetes cluster, use the versioned manifest from the [cert-manager releases](https://github.com/cert-manager/cert-manager/releases) page, and always remove cert-manager with the manifest of the version that was actually installed.
{{< /hint >}}

## Webhook Certificate

With the webhooks enabled, the chart creates two cert-manager resources alongside the operator: a self-signed `Issuer` and a `Certificate` covering the webhook service names, issued into the `webhook-server-cert` secret that the operator deployment mounts. The certificate also carries a PKCS12 keystore for the webhook's Java runtime, protected by the default webhook password secret or by a custom one set through `webhook.keystore.passwordSecretRef`. For FIPS-compliant environments, the keystore encryption profile is selected with `webhook.keystore.pkcs12Profile`, listed with the other parameters on the Helm [Installation]({{< ref "docs/deployment/helm/installation" >}}) page.

Cert-manager also closes the trust chain from the other side: its CA injector patches the certificate's CA bundle into the Mutating and Validating webhook configurations, through the `cert-manager.io/inject-ca-from` annotation, so the Kubernetes API server trusts the webhook it calls.

## Disabling the Webhooks

The webhooks can be disabled at installation time by passing the `--set webhook.create=false` parameter or editing the `values.yaml` directly, with `webhook.validator.create` and `webhook.mutator.create` overriding it per webhook. With all webhooks disabled, neither cert-manager nor any of the resources above are required.

## Uninstallation

Since cert-manager is installed as a prerequisite rather than as part of the operator's Helm release, uninstalling the operator leaves it in place. It is removed with the same manifest it was installed from, but only when no other workloads in the cluster depend on it, cert-manager is commonly shared infrastructure:

```
kubectl delete -f https://github.com/cert-manager/cert-manager/releases/latest/download/cert-manager.yaml
```
