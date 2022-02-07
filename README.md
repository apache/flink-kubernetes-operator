# flink-kubernetes-operator
Temporary repository for Flink Kubernetes Operator. The content will be moved to OSS repo once created an IPR. Check [FLIP-212](https://cwiki.apache.org/confluence/display/FLINK/FLIP-212%3A+Introduce+Flink+Kubernetes+Operator) further info.

## How to Build
```
mvn clean install
```

## How to Run
* Make Sure that `FlinkDeployment` Custom Resource Definition is already applied onto the cluster. If not, issue the following commands to apply:
```
kubectl create -f target/classes/META-INF/fabric8/flinkdeployments.flink.io-v1.yml
```
* (Optional) Build Docker Image
```
docker build . -t docker.apple.com/gyula_fora/flink-java-operator:latest
```
* Start flink-operator deployment. A new `ServiceAccount` "flink-operator" will be created with enough permission to create/list pods and services.
```
kubectl create -f deploy/rbac.yaml
kubectl create -f deploy/flink-operator.yaml
```
* Create a new Flink deployment
The flink-operator will watch the CRD resources and submit a new Flink deployment once the CR it applied.
```
kubectl create -f examples/basic.yaml
```

* Delete a Flink deployment
```
kubectl delete -f create/basic.yaml

OR

kubectl delete flinkdep {dep_name}
```

* Get/List Flink deployments
Get all the Flink deployments running in the K8s cluster
```
kubectl get flinkdep
```

Describe a specific Flink deployment to show the status(including job status, savepoint, ect.)
```
kubectl describe flinkdep {dep_name}
```
## How to Debug
You can run or debug the `FlinkOperator` from your preferred IDE. The operator itself is accessing the deployed Flink clusters through the REST interface. When running locally the `rest.port` and `rest.address` Flink configuration parameters must be modified to a locally accessible value. 

When using `minikube tunnel` the rest service is exposed on `localhost:8081`
```
> minikube tunnel

> kubectl get services
NAME                         TYPE           CLUSTER-IP     EXTERNAL-IP   PORT(S)             AGE
basic-session-example        ClusterIP      None           <none>        6123/TCP,6124/TCP   14h
basic-session-example-rest   LoadBalancer   10.96.36.250   127.0.0.1     8081:30572/TCP      14h
```
The operator pics up the default log and flink configurations from `/opt/flink/conf`. You can put the rest configuration parameters here:
```
cat /opt/flink/conf/flink-conf.yaml
rest.port: 8081
rest.address: localhost
```

