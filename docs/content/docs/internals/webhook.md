---
title: "Webhook"
weight: 4
type: docs
aliases:
- /internals/webhook-flow.html
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

# Webhook

The admission webhook catches invalid resources at the API server, before they are stored, so a broken spec fails the `kubectl apply` instead of surfacing as an error status on the next reconcile pass. This page covers the webhook process itself, its TLS handling, the admission configurations that route requests to it, and the validator and mutator chains. The concept-level view lives under [Architecture → Admission Control]({{< ref "docs/concepts/architecture#admission-control" >}}).

## Process and Wiring

The webhook is its own process, built from the `flink-kubernetes-webhook` module and started as the `flink-webhook` sidecar container in the operator pod from the same image (`/docker-entrypoint.sh webhook`), entering at `FlinkOperatorWebhook.main()`. It runs a Netty HTTP server on the port given by `WEBHOOK_SERVER_PORT` (9443 in the chart) and serves exactly two paths, `/validate` and `/mutate`. Any other path is answered with an error.

Although it is a separate process, it is wired from the same building blocks as the operator: `FlinkConfigManager` loads the same operator configuration, including the watched namespaces with dynamic namespace changes propagated at runtime, and the validator and mutator sets are discovered through the same SPI mechanism the operator uses, so admission and the reconcile loop always enforce identical rules. Custom implementations plug in as described under [Plugins → Custom Flink Resource Validators]({{< ref "docs/deployment/plugins#custom-flink-resource-validators" >}}) and [Plugins → Custom Flink Resource Mutators]({{< ref "docs/deployment/plugins#custom-flink-resource-mutators" >}}).

Because admission decisions can depend on other resources, the webhook maintains its own informers through `InformerManager`: `FlinkDeployment` and `FlinkSessionJob` informers, one per watched namespace or a single cluster-wide one when all namespaces are watched, give the handlers a live, cache-backed view without querying the API server on every request.

## TLS

The server reads its keystore from `WEBHOOK_KEYSTORE_FILE`, `WEBHOOK_KEYSTORE_TYPE`, and `WEBHOOK_KEYSTORE_PASSWORD`. Without a configured keystore the webhook runs plain HTTP, useful only for local development. The API server requires TLS for admission webhooks.

In the chart, cert-manager issues the certificate: an Issuer and a Certificate produce the `webhook-server-cert` Secret carrying a PKCS12 keystore, mounted into the webhook container, with the keystore password sourced from the generated webhook Secret or a user-provided `passwordSecretRef`. FIPS-compliant environments can force the `Modern2023` encryption profile through `webhook.keystore.pkcs12Profile`. The CA bundle is injected into both admission configurations through the `cert-manager.io/inject-ca-from` annotation.

Certificate rotation needs no restart: the SSL context is a `ReloadableSslContext`, and a `FileSystemWatchService` watching the keystore directory reloads it whenever cert-manager renews the Secret.

## Admission Configurations

The chart registers one validating and one mutating configuration, enabled together by default and individually toggleable through the `webhook.validator.create` and `webhook.mutator.create` values. Both point at the `flink-operator-webhook-service` Service and fire on `CREATE` and `UPDATE`:

| Configuration | Path        | Resources                                                     |
|---------------|-------------|---------------------------------------------------------------|
| Validating    | `/validate` | `flinkdeployments`, `flinksessionjobs`, `flinkstatesnapshots` |
| Mutating      | `/mutate`   | `flinkdeployments`, `flinksessionjobs`                        |

Both configurations use `failurePolicy: Fail`: while the webhook is enabled but unreachable, creates and updates of these resources are rejected by the API server. When watched namespaces are configured, a namespace selector scopes both configurations, so resources outside the watched set skip admission entirely. `FlinkBlueGreenDeployment` appears in neither configuration: its nested spec is first validated when the blue/green controller creates the child `FlinkDeployment` resources, which pass through admission like any other client's writes.

## Validation Path

`AdmissionHandler` decodes the `AdmissionReview` and hands it to the JOSDK admission framework, which calls `FlinkValidator`. Canary resources are exempt. The request is dispatched by kind: a deployment is validated on its own, a session job is validated together with its parent session cluster looked up from the informer cache, and a snapshot resolves its `jobReference` target the same way, so referential rules are enforced at admission time. The first failing validator rejects the request, and the API server surfaces the message directly to the caller.

The same validators run again inside every reconcile pass: the webhook is optional, so the reconcile loop remains the authority, as described under [Controllers → Validation]({{< ref "docs/internals/controllers#validation" >}}).

## Mutation Path

`FlinkMutator` dispatches by kind and runs every discovered mutator in sequence. The stock `DefaultFlinkMutator` changes almost nothing: deployments and snapshots pass through untouched, and a session job receives the `target.session` label derived from `spec.deploymentName`, making the session jobs of a cluster selectable through a plain label query. The SPI also offers a snapshot mutation hook, though the chart's mutating configuration does not route `FlinkStateSnapshot` resources to the webhook.

One subtlety in the response handling: after the mutators run, the result is compared against the original object, and when nothing changed the original is returned as is. This keeps the serialization round-trip from producing a spurious patch for an unchanged resource.
