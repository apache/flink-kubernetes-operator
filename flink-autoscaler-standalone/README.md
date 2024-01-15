# Flink Autoscaler Standalone

## What's the autoscaler standalone?

`Flink Autoscaler Standalone` is an implementation of `Flink Autoscaler`, it runs as 
a separate java process. It computes the reasonable parallelism of all job vertices 
by monitoring the metrics, such as: processing rate, busy time, etc. Please see 
[Autoscaler official doc](https://nightlies.apache.org/flink/flink-kubernetes-operator-docs-main/docs/custom-resource/autoscaler/) 
for an overview of how autoscaling works.

`Flink Autoscaler Standalone` rescales flink job in-place by rest api of 
[Externalized Declarative Resource Management](https://nightlies.apache.org/flink/flink-docs-master/docs/deployment/elastic_scaling/#externalized-declarative-resource-management).
`RescaleApiScalingRealizer` is the default implementation of `ScalingRealizer`, 
it uses the Rescale API to apply parallelism changes.

Kubernetes Operator is well integrated with Autoscaler, we strongly recommend using 
Kubernetes Operator directly for the kubernetes flink jobs, and only flink jobs in 
non-kubernetes environments use Autoscaler Standalone.

## How To Use

Currently, `Flink Autoscaler Standalone` only supports a single Flink cluster.
It can be any type of Flink cluster, includes: 

- Flink Standalone Cluster
- MiniCluster
- Flink yarn session cluster
- Flink yarn application cluster
- Flink kubernetes session cluster
- Flink kubernetes application cluster
- etc

You can start a Flink Streaming job with the following ConfigOptions.

```
# Enable Adaptive scheduler to play the in-place rescaling.
jobmanager.scheduler : adaptive

# Enable autoscale and scaling
job.autoscaler.enabled : true
job.autoscaler.scaling.enabled : true
job.autoscaler.stabilization.interval : 1m
job.autoscaler.metrics.window : 3m
```

Note: In-place rescaling is only supported since Flink 1.18. Flink jobs before version 
1.18 cannot be scaled automatically, but you can view the ScalingReport in Log. 
ScalingReport will show the recommended parallelism for each vertex.

After the flink job starts, please start the StandaloneAutoscaler process by the 
following command. 

```
java -cp flink-autoscaler-standalone-1.7-SNAPSHOT.jar \
org.apache.flink.autoscaler.standalone.StandaloneAutoscalerEntrypoint \
--flinkClusterHost localhost \
--flinkClusterPort 8081
```

Updating the `flinkClusterHost` and `flinkClusterPort` based on your flink cluster. 
In general, the host and port are the same as Flink WebUI.

## Extensibility of autoscaler standalone

Please click [here](../flink-autoscaler/README.md) to check out extensibility of generic autoscaler.

`Autoscaler Standalone` isn't responsible for job management, so it doesn't have job information.
`Autoscaler Standalone` defines the `JobListFetcher` interface in order to get the 
`JobAutoScalerContext` of the job. It has a control loop that periodically calls 
`JobListFetcher#fetch` to fetch the job list and scale these jobs.

Currently `FlinkClusterJobListFetcher` is the only implementation of the `JobListFetcher` 
interface, that's why `Flink Autoscaler Standalone` only supports a single Flink cluster so far.
We will implement `YarnJobListFetcher` in the future, `Flink Autoscaler Standalone` will call 
`YarnJobListFetcher#fetch` to fetch job list from yarn cluster periodically.
