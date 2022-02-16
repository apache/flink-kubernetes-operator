# flink-kubernetes-operator
Temporary repository for Flink Kubernetes Operator. The content will be moved to OSS repo once created an IPR. Check [FLIP-212](https://cwiki.apache.org/confluence/display/FLINK/FLIP-212%3A+Introduce+Flink+Kubernetes+Operator) further info.

## Installation

The operator is managed helm chart. To install run:
```
 cd helm/flink-operator
 helm install flink-operator .
```

In order to use the webhook for FlinkDeployment validation, you must install the cert-manager on the Kubernetes cluster:
```
kubectl apply -f https://github.com/jetstack/cert-manager/releases/download/v1.7.1/cert-manager.yaml
```

The webhook can be disabled during helm install by passing the `--set webhook.create=false` parameter or editing the `values.yaml` directly.

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
docker push <repo>flink-java-operator:latest
helm install flink-operator . --set image.repository=<repo> --set image.tag=latest
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
