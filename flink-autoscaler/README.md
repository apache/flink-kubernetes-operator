# Flink Autoscaler

This module contains the implementation for autoscaling Flink jobs.
Please see [FLIP-271](https://cwiki.apache.org/confluence/display/FLINK/FLIP-271%3A+Autoscaling)
for an overview of how autoscaling works. Also see the [docs](../docs).

## How To Use

If you want to use this code as part of the Flink Kubernetes Operator, you don't need to
do anything. The code here is already bundled with the Flink Kubernetes operator.
If you are looking to use autoscaling with a different orchestration framework, read on!

## Interfacing with the autoscaler

While earlier implementations were dependent on other modules of this repository,
thus tightly coupling the autoscaler implementation with Kubernetes, this no
longer is the case. This module is not dependent on any either modules in this repository.
It lives here for historical reasons but also to ease the release process for its main
consumer, the Flink Kubernetes operator. Regardless, any downstream project is welcome
to consume the `flink-autoscaler` artifact and use autoscaling with its own orchestration
framework.

Under the hood, the autoscaler exposes a set of interfaces for storing autoscaler state,
handling autoscaling events, and executing scaling decisions. How these are implemented
is specific to the orchestration framework used (e.g. Kubernetes), but the interfaces are
designed to be as generic as possible. The following are the introduction of these 
generic interfaces:

- **AutoScalerEventHandler** : Handling autoscaler events, such as: ScalingReport,
  AutoscalerError, etc. `LoggingEventHandler` is the default implementation, it logs events.
- **AutoScalerStateStore** : Storing all state during scaling. `InMemoryAutoScalerStateStore`
  is the default implementation, it's based on the Java Heap, so the state will be discarded
  after process restarts. We will implement persistent State Store in the future, such as
  : `JdbcAutoScalerStateStore`.
- **ScalingRealizer** : Applying scaling actions.
- **JobAutoScalerContext** : Including all details related to the current job.

Currently, the Autoscaler Standalone has been implemented, it supports a single Flink cluster, 
and other implementation in the making, e.g. YARN cluster. Please click [here](../flink-autoscaler-standalone/README.md) 
if you are interested in Autoscaler Standalone.
