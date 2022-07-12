---
title: "Operator Lifecycle Manager"
weight: 5
type: docs
aliases:
- /operations/olm.html
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

# Managing Flink Operator Using OLM
The Operator Lifecycle Management(OLM) helps install, update, and manage the lifecycle of all Operators in a Kubernetes Clusters. The [Operatorhub](https://operatorhub.io/) contains a catalog of community shared Kubernetes operators which are ready to be deployed using OLM.
## Prerequisite
1. Kubernetes Cluster. Use [minikube](https://minikube.sigs.k8s.io/docs/start/) to create cluster locally if needed.
2. Install the `operator-sdk` CLI from [here](https://sdk.operatorframework.io/docs/installation/).

    The CLI binary provides commands to easily install/uninstall OLM in a Kubernetes cluster and also facilitates the development process of OLM bundles.
## Quick Start

1. Install OLM on your Kubernetes Cluster.
   ```sh
   operator-sdk olm install
   ```

   Once installed, you should see similar message below:
   ```
    operator-sdk olm install
    INFO[0000] Fetching resources for resolved version "latest"
    INFO[0018] Creating CRDs and resources
    INFO[0018]   Creating CustomResourceDefinition "catalogsources.operators.coreos.com"
    INFO[0018]   Creating CustomResourceDefinition "clusterserviceversions.operators.coreos.com"
    INFO[0018]   Creating CustomResourceDefinition "installplans.operators.coreos.com"
    INFO[0019]   Creating CustomResourceDefinition "olmconfigs.operators.coreos.com"
    INFO[0019]   Creating CustomResourceDefinition "operatorconditions.operators.coreos.com"
    INFO[0019]   Creating CustomResourceDefinition "operatorgroups.operators.coreos.com"
    INFO[0019]   Creating CustomResourceDefinition "operators.operators.coreos.com"
    INFO[0019]   Creating CustomResourceDefinition "subscriptions.operators.coreos.com"
    INFO[0019]   Creating Namespace "olm"
    INFO[0019]   Creating Namespace "operators"
    INFO[0019]   Creating ServiceAccount "olm/olm-operator-serviceaccount"
    INFO[0019]   Creating ClusterRole "system:controller:operator-lifecycle-manager"
    INFO[0019]   Creating ClusterRoleBinding "olm-operator-binding-olm"
    INFO[0019]   Creating OLMConfig "cluster"
    INFO[0022]   Creating Deployment "olm/olm-operator"
    INFO[0022]   Creating Deployment "olm/catalog-operator"
    INFO[0022]   Creating ClusterRole "aggregate-olm-edit"
    INFO[0022]   Creating ClusterRole "aggregate-olm-view"
    INFO[0022]   Creating OperatorGroup "operators/global-operators"
    INFO[0022]   Creating OperatorGroup "olm/olm-operators"
    INFO[0022]   Creating ClusterServiceVersion "olm/packageserver"
    INFO[0022]   Creating CatalogSource "olm/operatorhubio-catalog"
    INFO[0022] Waiting for deployment/olm-operator rollout to complete
    INFO[0022]   Waiting for Deployment "olm/olm-operator" to rollout: 0 of 1 updated replicas are available
    INFO[0052]   Deployment "olm/olm-operator" successfully rolled out
    INFO[0052] Waiting for deployment/catalog-operator rollout to complete
    INFO[0052]   Waiting for Deployment "olm/catalog-operator" to rollout: 0 of 1 updated replicas are available
    INFO[0053]   Deployment "olm/catalog-operator" successfully rolled out
    INFO[0053] Waiting for deployment/packageserver rollout to complete
    INFO[0053]   Waiting for Deployment "olm/packageserver" to rollout: 0 of 2 updated replicas are available
    INFO[0057]   Deployment "olm/packageserver" successfully rolled out
    INFO[0058] Successfully installed OLM version "latest"

    NAME                                            NAMESPACE    KIND                        STATUS
    catalogsources.operators.coreos.com                          CustomResourceDefinition    Installed
    clusterserviceversions.operators.coreos.com                  CustomResourceDefinition    Installed
    installplans.operators.coreos.com                            CustomResourceDefinition    Installed
    olmconfigs.operators.coreos.com                              CustomResourceDefinition    Installed
    operatorconditions.operators.coreos.com                      CustomResourceDefinition    Installed
    operatorgroups.operators.coreos.com                          CustomResourceDefinition    Installed
    operators.operators.coreos.com                               CustomResourceDefinition    Installed
    subscriptions.operators.coreos.com                           CustomResourceDefinition    Installed
    olm                                                          Namespace                   Installed
    operators                                                    Namespace                   Installed
    olm-operator-serviceaccount                     olm          ServiceAccount              Installed
    system:controller:operator-lifecycle-manager                 ClusterRole                 Installed
    olm-operator-binding-olm                                     ClusterRoleBinding          Installed
    cluster                                                      OLMConfig                   Installed
    olm-operator                                    olm          Deployment                  Installed
    catalog-operator                                olm          Deployment                  Installed
    aggregate-olm-edit                                           ClusterRole                 Installed
    aggregate-olm-view                                           ClusterRole                 Installed
    global-operators                                operators    OperatorGroup               Installed
    olm-operators                                   olm          OperatorGroup               Installed
    packageserver                                   olm          ClusterServiceVersion       Installed
    operatorhubio-catalog                           olm          CatalogSource               Installed
    ```

2. Install Flink Operator
    ```sh
    cat << EOF | kubectl apply -f -
    apiVersion: operators.coreos.com/v1alpha2
    kind: OperatorGroup
    metadata:
      name: default-og
      namespace: default
    ---
    apiVersion: operators.coreos.com/v1alpha1
    kind: Subscription
    metadata:
      name: flink-operator
      namespace: default
    spec:
      channel: alpha
      name: flink-kubernetes-operator
      source: operatorhubio-catalog
      sourceNamespace: olm
    EOF
    ```
    Watch the installation process
    ```sh
    kubectl get csv --watch
    ```

    When installed successfully you should see
    ```
    NAME                               DISPLAY                     VERSION   REPLACES   PHASE
    flink-kubernetes-operator.v1.0.1   Flink Kubernetes Operator   1.0.1                Succeeded
    ```
3. (Optional) [Submit a Flink Job](https://nightlies.apache.org/flink/flink-kubernetes-operator-docs-main/docs/try-flink-kubernetes-operator/quick-start/#submitting-a-flink-job)


# OLM Bundle Development Process
The https://github.com/k8s-operatorhub/community-operators repo is the canonical source of the community operators which can be deployed by OLM. Developers may share their operators by submitting OLM bundles. The steps below demonstrate the process of updating, testing the OLM bundle locally, and submit a PR.

1. Download the [opm cli](https://github.com/operator-framework/operator-registry/releases). This binary is used to build a catalog image.
    ```sh
    # using Mac as an example
    chomod +x <pathto>/darwin-amd64-opm
    mv <pathto>/darwin-amd64-opm /usr/local/bin/opm
    ```

2. Modify the exiting bundle
    ```
    git clone https://github.com/k8s-operatorhub/community-operators.git
    cd community-operators/operators/flink-kubernetes-operator
    ```
    Flink operator is packaged as a [bundle format](https://github.com/operator-framework/community-operators/blob/master/docs/testing-operators.md#bundle-format-supported-with-0140-or-newer). The top level folders are operator versions containing the manifests and metadata folders.
    ```
    tree flink-kubernetes-operator/
    ├── 1.0.1
    │   ├── bundle.Dockerfile
    │   ├── manifests
    │   │   ├── flink-keystore.secret.yaml
    │   │   ├── flink-kubernetes-operator.clusterserviceversion.yaml
    │   │   ├── flink-kubernetes-operator.configmap.yaml
    │   │   ├── flinkdeployments.flink.apache.org-v1.yml
    │   │   └── flinksessionjobs.flink.apache.org-v1.yml
    │   └── metadata
    │       └── annotations.yaml
    └── ci.yaml
    ```
    The manifests folder must have one ClusterServiceVersion(CSV) and CustomResourceDefiniton(CRD) manifests. The CSV
    is used to define deployments, RBACs, required CRDs, and WebhookDefinitions, etc. Applying the CSV will automatically generate resources that are necessary to run the operator. A list of of [supported resources](https://olm.operatorframework.io/docs/tasks/creating-operator-manifests/#packaging-additional-objects-alongside-an-operator), such as configmap and secret, may also be included in the manifests folder. See [CSV how-to](https://olm.operatorframework.io/docs/tasks/creating-operator-manifests/#writing-your-operator-manifests) for detail.

3. Test locally

    Before submitting any changes, do the following steps to validate and test operator deployment locally.

    To validate bundle format:
    ```sh
    cd community-operators/operators/flink-kubernetes-operator/1.0.1/
    operator-sdk bundle validate ./ --select-optional name=operatorhub
    ```
    You should see the following if the bundle is valid:
    ```
    INFO[0000] All validation tests have completed successfully
    ```

    Build bundle image and publish
    ```sh
    export namespace=<your namespace>
    docker build -f bundle.Dockerfile -t docker.io/${namespace}/my-flink-operator-bundle:1.0.1
    docker push docker.io/${namespace}/my-flink-operator-bundle:1.0.1
    ```

    Build catalog image and publish
    ```
    opm index add --bundles docker.io/${namespace}/my-flink-operator-bundle:1.0.1 --tag docker.io/${namespace}/my-flink-catalog:latest
    docker push docker.io/${namespace}/my-flink-catalog:latest
    ```

    Deploy your own catalog service into the cluster
    ```sh
    cat <<EOF | kubectl apply -f -
    apiVersion: operators.coreos.com/v1alpha1
    kind: CatalogSource
    metadata:
      name: my-flink-catalog
      namespace: default
    spec:
      displayName: “my flink catalog”
      sourceType: grpc
      image: docker.io/${namespace}/my-flink-catalog:latest
    EOF
    ```
    Once the catalog service is running, you should see messages similar to
    ```
    kubectl get catsrc
    NAME               DISPLAY              TYPE   PUBLISHER   AGE
    my-flink-catalog   “my flink catalog”   grpc               7s
    ```
    Install the operator by subscribing it from your catalog service
    ```sh
    cat <<EOF | kubectl apply -f -
    apiVersion: operators.coreos.com/v1alpha2
    kind: OperatorGroup
    metadata:
      name: default-og
      namespace: default
    ---
    apiVersion: operators.coreos.com/v1alpha1
    kind: Subscription
    metadata:
      name: my-flink-operator-subscription
      namespace: default
    spec:
      channel: alpha
      name: flink-kubernetes-operator
      source: my-flink-catalog
      sourceNamespace: default
    EOF
    ```
    Once installed, you should see
    ```
    kubectl get csv
    NAME                               DISPLAY                     VERSION   REPLACES   PHASE
    flink-kubernetes-operator.v1.0.1   Flink Kubernetes Operator   1.0.1                Succeeded
    ```
    Proceed to [submit a Flink Job](https://nightlies.apache.org/flink/flink-kubernetes-operator-docs-main/docs/try-flink-kubernetes-operator/quick-start/#submitting-a-flink-job) and verify logs.

4. Finally, submit a PR from a forked repo. See [contributor guide](https://github.com/operator-framework/community-operators/blob/master/docs/contributing-via-pr.md) for more detail.

# Additional Resources
## Delete Flink operator
Delete the Flink operator that was installed by the OLM involves the following steps
1. Remove the CSV
    ```sh
    kubectl delete csv flink-kubernetes-operator.v1.0.1
    ```
2. List subscription
    ```
    kubectl get sub
    NAME             PACKAGE                     SOURCE                  CHANNEL
    flink-operator   flink-kubernetes-operator   operatorhubio-catalog   alpha
    ```
    and then remove the Flink operator subscription
    ```sh
    kubectl delete sub flink-operator
    ```
3. (Optional) Remove catalog

    You may delete the catalog service deployed during the development process mentioned [above](#olm-bundle-development-process)
    ```sh
    kubectl delete catsrc my-flink-catalog
    ```
## Webhook Certificate
The Flink operator has a [validation webhook](https://nightlies.apache.org/flink/flink-kubernetes-operator-docs-main/docs/operations/helm/#validating-webhook) for CR validation. The webhook service may require ssl communication with the Kubernetes API server in your cluster.

By default, OLM does not support creating resources for third party certificate manager such as [cert-manager](https://github.com/jetstack/cert-manager); therefore any bundled certificate or issuers CR manifests are not deployed by OLM. Consequently, you would not see a cert-manager service in your cluster when you deploy Flink operator with OLM.

Instead, the OLM can automatically create [self-signed certificates](https://olm.operatorframework.io/docs/advanced-tasks/adding-admission-and-conversion-webhooks/#certificate-authority-requirements
) as volumes(i.e. apiservice-cert and webhook-cert) and append to the Flink operator deployment. For example, you may see something like in the Flink deployment instance
```
kubectl get flink-kubernetes-operator -o yaml

...
kind: deploy
spec
  template:
    spec:
      volumes:
      ...
      - name: apiservice-cert
        secret:
          defaultMode: 420
          items:
          - key: tls.crt
            path: apiserver.crt
          - key: tls.key
            path: apiserver.key
          secretName: flink-kubernetes-operator-service-cert
      - name: webhook-cert
        secret:
          defaultMode: 420
          items:
          - key: tls.crt
            path: tls.crt
          - key: tls.key
            path: tls.key
          secretName: flink-kubernetes-operator-service-cert
```

The certificate key pair are used to [generate a keystore]((https://github.com/k8s-operatorhub/community-operators/blob/1e3471b35a74066e19740f93ec73ed34525b8298/operators/flink-kubernetes-operator/1.0.1/manifests/flink-kubernetes-operator.clusterserviceversion.yaml#L286)) file which the [Flink webhook](https://github.com/k8s-operatorhub/community-operators/blob/1e3471b35a74066e19740f93ec73ed34525b8298/operators/flink-kubernetes-operator/1.0.1/manifests/flink-kubernetes-operator.clusterserviceversion.yaml#L345-L348) can consume.
