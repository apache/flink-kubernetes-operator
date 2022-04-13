# Apache Flink Kubernetes Operator

A Kubernetes operator for Apache Flink, implemented in Java. It allows users to manage Flink applications and their lifecycle through native k8s tooling like kubectl.

<img alt="Operator Overview" width="100%" src="docs/static/img/overview.svg">

## Documentation & Getting Started

Please check out the full [documentation](https://nightlies.apache.org/flink/flink-kubernetes-operator-docs-main/), hosted by the
[ASF](https://www.apache.org/), for detailed information and user guides.

Check our [quick-start](https://nightlies.apache.org/flink/flink-kubernetes-operator-docs-main/docs/try-flink-kubernetes-operator/quick-start/) guide for simple setup instructions to get you started with the operator.

## Features at a glance

 - Deploy and monitor Flink Application and Session deployments
 - Upgrade, suspend and delete deployments
 - Full logging and metrics integration
 - Flexible deployments and native integration with Kubernetes tooling

## Project Status

### Project status: beta

### Current API version: `v1beta1`

If you are currently using the v1alpha1 version of the APIs in your manifests, please update them to use the v1beta1 version by changing apiVersion: "flink.apache.org/v1alpha1" to apiVersion: "flink.apache.org/v1beta1".

## Version Matrix

| Operator Version | API Version | Flink Release  | Operator Image Tag | Helm Chart Version |
| ------------- | ------------- | ------------- | ------------- | ------------- |
| `latest` (main HEAD) | `v1beta1` | 1.14 |  |  |
| `release-0.1.0` | `v1alpha1` | 1.14 | `2c166e3` | 0.1.0 |

## Support

Don’t hesitate to ask!

Contact the developers and community on the [mailing lists](https://flink.apache.org/community.html#mailing-lists) if you need any help.

[Open an issue](https://issues.apache.org/jira/browse/FLINK) if you found a bug in Flink.

## Contributing

You can learn more about how to contribute in the [Apache Flink website](https://flink.apache.org/contributing/how-to-contribute.html). For code contributions, please read carefully the [Contributing Code](https://flink.apache.org/contributing/contribute-code.html) section for an overview of ongoing community work.

## License

The code in this repository is licensed under the [Apache Software License 2](LICENSE).
