# Apache Flink Kubernetes Operator

A Kubernetes operator for Apache Flink, implemented in Java. See [FLIP-212](https://cwiki.apache.org/confluence/display/FLINK/FLIP-212%3A+Introduce+Flink+Kubernetes+Operator).

## Documentation

Our full [documentation](https://nightlies.apache.org/flink/flink-kubernetes-operator-docs-main/) is hosted by the 
[ASF](https://www.apache.org/), please check out details there as this document only describes the basics.

## Installation

The operator installation is managed by a helm chart. To install run:

```
helm install flink-operator helm/flink-operator
```

Alternatively to install the operator (and also the helm chart) to a specific namespace:

```
helm install flink-operator helm/flink-operator --namespace flink-operator --create-namespace
```

Note that in this case you will need to update the namespace in the examples accordingly or the `default`
namespace to the [watched namespaces](#watching-only-specific-namespaces).

### Validating webhook

In order to use the webhook for FlinkDeployment validation, you must install the cert-manager on the Kubernetes cluster:
```
kubectl apply -f https://github.com/jetstack/cert-manager/releases/download/v1.7.1/cert-manager.yaml
```

The webhook can be disabled during helm install by passing the `--set webhook.create=false` parameter or editing the `values.yaml` directly.

### Watching only specific namespaces

The operator supports watching a specific list of namespaces for FlinkDeployment resources. You can enable it by setting the `--set watchNamespaces={flink-test}` parameter.
When this is enabled role-based access control is only created specifically for these namespaces for the operator and the jobmanagers, otherwise it defaults to cluster scope.

## User Guide
### Create a new Flink deployment
The flink-operator will watch the CRD resources and submit a new Flink deployment once the CR is applied.
```
kubectl create -f examples/basic.yaml
```

### Delete a Flink deployment
```
kubectl delete -f examples/basic.yaml

OR

kubectl delete flinkdep {dep_name}
```

### Get/List Flink deployments
Get all the Flink deployments running in the K8s cluster
```
kubectl get flinkdep
```
Describe a specific Flink deployment to show the status(including job status, savepoint, etc.)
```
kubectl describe flinkdep {dep_name}
```

## CI/CD
We use [GitHub Actions](https://help.github.com/en/actions/getting-started-with-github-actions/about-github-actions) to help you automate your software development workflows in the same place you store code and collaborate on pull requests and issues.
You can write individual tasks, called actions, and combine them to create a custom workflow.
Workflows are custom automated processes that you can set up in your repository to build, test, package, release, or deploy any code project on GitHub.

Considering the cost of running the builds, the stability, and the maintainability, flink-kubernetes-operator chose GitHub Actions and build the whole CI/CD solution on it.
All the unit tests, integration tests, and the end-to-end tests will be triggered for each PR.

Note: Please make sure the CI passed before merging.