# flink-kubernetes-operator
Temporary repository for Flink Kubernetes Operator. The content will be moved to OSS repo once created and IPR.

## How to Build
```
mvn clean install
```

## How to Run
* Make Sure that FlinkApplication Custom Resource Definition is already applied onto the cluster. The CRD could be find [here](deploy/crd.yaml). If not, issue the following commands to apply:
```
kubectl apply -f deploy/crd.yaml
```
* Build Docker Image
```
docker build . -t docker.apple.com/gyula_fora/flink-java-operator:latest
```
* Start flink-operator deployment
A new `ServiceAccount` "flink-operator" will be created with enough permission to create/list pods and services.
```
kubectl apply -f deploy/flink-operator.yaml
```
* Create a new Flink application
The flink-operator will watch the CRD resources and submit a new Flink application once the CR it applied.
```
kubectl apply -f deploy/cr.yaml
```

* Delete a Flink application
```
kubectl delete -f deploy/cr.yaml

OR

kubectl delete flinkapp {app_name}
```

* Get/List Flink applications
Get all the Flink applications running in the K8s cluster
```
kubectl get flinkapp
```

Describe a specific Flink application to show the status(including job status, savepoint, ect.)
```
kubectl describe flinkapp {app_name}
```
