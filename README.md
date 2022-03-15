# Apache Flink Kubernetes Operator

A Kubernetes operator for Apache Flink, implemented in Java. See [FLIP-212](https://cwiki.apache.org/confluence/display/FLINK/FLIP-212%3A+Introduce+Flink+Kubernetes+Operator).

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
## Developer Guide

### Building docker images
```
docker build . -t <repo>/flink-java-operator:latest
docker push <repo>/flink-java-operator:latest
```
### Installing the operator locally
```
helm install flink-operator helm/flink-operator --set image.repository=<repo> --set image.tag=latest
```

### Running the operator locally
You can run or debug the `FlinkOperator` from your preferred IDE. The operator itself is accessing the deployed Flink clusters through the REST interface. When running locally the `rest.port` and `rest.address` Flink configuration parameters must be modified to a locally accessible value.

When using `minikube tunnel` the rest service is exposed on `localhost:8081`
```
> minikube tunnel

> kubectl get services
NAME                         TYPE           CLUSTER-IP     EXTERNAL-IP   PORT(S)             AGE
basic-session-example        ClusterIP      None           <none>        6123/TCP,6124/TCP   14h
basic-session-example-rest   LoadBalancer   10.96.36.250   127.0.0.1     8081:30572/TCP      14h
```
The operator picks up the default log and flink configurations from `/opt/flink/conf`. You can put the rest configuration parameters here:
```
cat /opt/flink/conf/flink-conf.yaml
rest.port: 8081
rest.address: localhost
```
### CI/CD
[GitHub Actions](https://help.github.com/en/actions/getting-started-with-github-actions/about-github-actions) help you automate your software development workflows in the same place you store code and collaborate on pull requests and issues.
You can write individual tasks, called actions, and combine them to create a custom workflow.
Workflows are custom automated processes that you can set up in your repository to build, test, package, release, or deploy any code project on GitHub.

Considering the cost of running the builds, the stability, and the maintainability, flink-kubernetes-operator chose GitHub Actions and build the whole CI/CD solution on it.
All the unit tests, integration tests, and the end-to-end tests will be triggered for each PR.

Note: Please make sure the CI passed before merging.