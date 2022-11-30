# OLM Integration

The document describes the automated flink-kubernetes-operator OLM bundle generation process for each release. For every release, we publish new bundle to two online catalogs. Each catalog has a Github repository that uses similar publishing process.
![image](https://user-images.githubusercontent.com/7155778/202823924-8d08561f-1b5f-4e96-9f87-adc171e699c9.png)
The [community-operators-prod catalog](https://github.com/redhat-openshift-ecosystem/community-operators-prod) comes with OCP (Openshift Container Platform) by default. The [community-operator catalog](https://github.com/k8s-operatorhub/community-operators) can be optionally installed on kubernetes clusters.

## Prerequisite

- [minikube](https://minikube.sigs.k8s.io/docs/)(recommended), [kind](https://kind.sigs.k8s.io/), or [OpenShift Local](https://developers.redhat.com/products/openshift-local/overview)
- [kubectl](https://kubernetes.io/docs/tasks/tools/)(Kubernetes) or [oc](https://docs.openshift.com/container-platform/4.11/cli_reference/openshift_cli/getting-started-cli.html#installing-openshift-cli)(OpenShift)
- [Docker](https://www.docker.com/)

## Generate the bundle locally
Clone the olm repo into the flink-kubernetes-operator repo:
```sh
git clone https://github.com/apache/flink-kubernetes-operator.git
cd flink-kubernetes-operator/tools/olm
```

Change the variables in `env.sh` if needed:
```sh
vi env.sh
```

Generate bundle and catalog images and push them to repos specified in env.sh.
```sh
./generate-olm-bundle.sh
```

Verify the bundle is generated in the target folder. We only need the bundle folder which has the following files:
```
1.3.0/
├── bundle.Dockerfile
├── manifests
│   ├── flink.apache.org_flinkdeployments.yaml
│   ├── flink.apache.org_flinksessionjobs.yaml
│   ├── flink-kubernetes-operator.clusterserviceversion.yaml
│   ├── flink-operator-config_v1_configmap.yaml
│   ├── flink-operator-webhook-secret_v1_secret.yaml
│   ├── flink_rbac.authorization.k8s.io_v1_role.yaml
│   ├── flink-role-binding_rbac.authorization.k8s.io_v1_rolebinding.yaml
│   └── flink_v1_serviceaccount.yaml
└── metadata
    └── annotations.yaml
```

## Deploy bundle using Subscription
With the bundle and catalog images pushed to a image registry, we can now deploy the operator by
creating a Subscription.

Kubernetes does not come with OLM. To deploy the OLM operator on a cluster, run the following:
```sh
curl -sL https://github.com/operator-framework/operator-lifecycle-manager/releases/download/v0.22.0/install.sh | bash -s v0.22.0
```
Deploy private catalog to serves the bundle on a cluster:
```sh
# Deploy the catalog src
cat <<EOF | kubectl apply -f -
apiVersion: operators.coreos.com/v1alpha1
kind: CatalogSource
metadata:
  name: olm-flink-operator-catalog
  namespace: default
spec:
  sourceType: grpc
  image: "${DOCKER_REGISTRY}/${DOCKER_ORG}/flink-op-catalog:${BUNDLE_VERSION}"
EOF
# wait for the catalog server pod to be ready
kubectl get po -w
```
Create subscription to deploy the flink-kubernetes-operator:
```sh
# Create a OperatorGroup and subscription in the default namespace:
cat <<EOF | kubectl apply -f -
apiVersion: operators.coreos.com/v1alpha2
kind: OperatorGroup
metadata:
  name: default-og
  namespace: default
spec:
  # if not set, default to watch all namespaces
  targetNamespaces:
  - default
---
apiVersion: operators.coreos.com/v1alpha1
kind: Subscription
metadata:
  name: flink-kubernetes-operator
  namespace: default
spec:
  channel: alpha
  name: flink-kubernetes-operator
  source: olm-flink-operator-catalog
  sourceNamespace: default
  # For testing upgrade from previous version
  #installPlanApproval: Automatic # Manual
  #startingCSV: "flink-kubernetes-operator.v${PREVIOUS_BUNDLE_VERSION}"
EOF
# wait for the flink-kubernetes-operator pod to be ready
kubectl get po -w
```

Deploy a Flink job to verify the operator:
```sh
kubectl create -f https://raw.githubusercontent.com/apache/flink-kubernetes-operator/release-1.2/examples/basic.yaml
```

After verifying, the bundle image and catalog image are no longer needed. We only need the bundle folder to run the CI test suits.


## Run CI test suits before creating PR

Clone your forked community-operators repo
```sh
git clone https://github.com/tedhtchang/community-operators.git
```
The bundle(i.e. 1.3.0) folder is all we need to commit into the community-operators repo. Copy the new bundle into the forked community-operators repo
```sh
cp -r flink-kubernetes-operator/tools/olm/target/1.3.0 community-operators/operators/flink-kubernetes-operator/
```

Run test suites
```sh
cd community-operators
OPP_PRODUCTION_TYPE=k8s  OPP_AUTO_PACKAGEMANIFEST_CLUSTER_VERSION_LABEL=1 bash <(curl -sL https://raw.githubusercontent.com/redhat-openshift-ecosystem/community-operators-pipeline/ci/latest/ci/scripts/opp.sh) all operators/flink-kubernetes-operator/1.3.0
```
The expected output:
```
...
Test 'kiwi' : [ OK ]
Test 'lemon_latest' : [ OK ]
Test 'orange_latest' : [ OK ]
```

After the tests pass, commit and push the new bundle to a branch and then create a PR in the community-operator repo.

Repeat the above steps for adding new bundle to the [community-operator-prod](https://github.com/redhat-openshift-ecosystem/community-operators-prod) repo. For this repo, run the CI test suits using:
```sh
cd community-operators-prod
OPP_PRODUCTION_TYPE=ocp  OPP_AUTO_PACKAGEMANIFEST_CLUSTER_VERSION_LABEL=1 bash <(curl -sL https://raw.githubusercontent.com/redhat-openshift-ecosystem/community-operators-pipeline/ci/latest/ci/scripts/opp.sh) all operators/flink-kubernetes-operator/1.3.0
```
After the tests pass, commit and push the new bundle to a branch and then create a PR in the community-operator-prod repo.

See detail for running CI test suits [here](https://k8s-operatorhub.github.io/community-operators/operator-test-suite/).
