# Apache Flink Kubernetes Operator

A Kubernetes operator for Apache Flink, implemented in Java. It allows users to manage Flink applications and their lifecycle through native k8s tooling like kubectl.

<img alt="Operator Overview" width="100%" src="docs/static/img/overview.svg">

## Documentation & Getting Started

Please check out the full [documentation](https://nightlies.apache.org/flink/flink-kubernetes-operator-docs-main/), hosted by the
[ASF](https://www.apache.org/), for detailed information and user guides.

Check our [quick-start](https://nightlies.apache.org/flink/flink-kubernetes-operator-docs-main/docs/try-flink-kubernetes-operator/quick-start/) guide for simple setup instructions to get you started with the operator.

## Features at a glance

 - Deploy and monitor Flink Application, Session and Job deployments
 - Upgrade, suspend and delete deployments
 - Full logging and metrics integration
 - Flexible deployments and native integration with Kubernetes tooling
 - Flink Job Autoscaler

For the complete feature-set please refer to our [documentation](https://nightlies.apache.org/flink/flink-kubernetes-operator-docs-main/docs/concepts/overview/).

## Project Status

### Project status: Production Ready

### Current API version: `v1beta1`

To download the latest stable version please visit the [Flink Downloads Page](https://flink.apache.org/downloads.html).
The official operator images are also available on [Dockerhub](https://hub.docker.com/r/apache/flink-kubernetes-operator/tags).

Please check out our docs to read about the [upgrade process](https://nightlies.apache.org/flink/flink-kubernetes-operator-docs-main/docs/operations/upgrade/) and our [backward compatibility guarantees](https://nightlies.apache.org/flink/flink-kubernetes-operator-docs-main/docs/operations/compatibility/).

## Support

Don’t hesitate to ask!

Contact the developers and community on the [mailing lists](https://flink.apache.org/community.html#mailing-lists) if you need any help.

[Open an issue](https://issues.apache.org/jira/browse/FLINK) if you found a bug in Flink.

## Contributing

You can learn more about how to contribute in the [Apache Flink website](https://flink.apache.org/contributing/how-to-contribute.html). For code contributions, please read carefully the [Contributing Code](https://flink.apache.org/contributing/contribute-code.html) section for an overview of ongoing community work.

## License

The code in this repository is licensed under the [Apache Software License 2](LICENSE).
