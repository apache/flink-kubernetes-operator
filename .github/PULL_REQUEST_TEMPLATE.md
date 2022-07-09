<!--
*Thank you very much for contributing to the Apache Flink Kubernetes Operator - we are happy that you want to help us improve the project. To help the community review your contribution in the best possible way, please go through the checklist below, which will get the contribution into a shape in which it can be best reviewed.*

## Contribution Checklist

  - Make sure that the pull request corresponds to a [JIRA issue](https://issues.apache.org/jira/projects/FLINK/issues). Exceptions are made for typos in JavaDoc or documentation files, which need no JIRA issue.
  
  - Name the pull request in the form "[FLINK-XXXX] [component] Title of the pull request", where *FLINK-XXXX* should be replaced by the actual issue number. Skip *component* if you are unsure about which is the best component.
  Typo fixes that have no associated JIRA issue should be named following this pattern: `[hotfix][docs] Fix typo in event time introduction` or `[hotfix][javadocs] Expand JavaDoc for PuncuatedWatermarkGenerator`.

  - Fill out the template below to describe the changes contributed by the pull request. That will give reviewers the context they need to do the review.
  
  - Make sure that the change passes the automated tests, i.e., `mvn clean verify` passes. You can read more on how we use GitHub Actions for CI [here](https://nightlies.apache.org/flink/flink-kubernetes-operator-docs-main/docs/development/guide/#cicd).

  - Each pull request should address only one issue, not mix up code from multiple issues.
  
  - Each commit in the pull request has a meaningful commit message (including the JIRA id)

  - Once all items of the checklist are addressed, remove the above text and this checklist, leaving only the filled out template below.


**(The sections below can be removed for hotfixes of typos)**
-->

## What is the purpose of the change

*(For example: This pull request adds a new feature to periodically create and maintain savepoints through the `FlinkDeployment` custom resource.)*


## Brief change log

*(for example:)*
  - *Periodic savepoint trigger is introduced to the custom resource*
  - *The operator checks on reconciliation whether the required time has passed*
  - *The JobManager's dispose savepoint API is used to clean up obsolete savepoints*

## Verifying this change
<!--
Please make sure both new and modified tests in this PR follows the conventions defined in our code quality guide: https://flink.apache.org/contributing/code-style-and-quality-common.html#testing
-->
*(Please pick either of the following options)*

This change is a trivial rework / code cleanup without any test coverage.

*(or)*

This change is already covered by existing tests, such as *(please describe tests)*.

*(or)*

This change added tests and can be verified as follows:

*(example:)*
  - *Added integration tests for end-to-end deployment with large payloads (100MB)*
  - *Extended integration test for recovery after master (JobManager) failure*
  - *Manually verified the change by running a 4 node cluster with 2 JobManagers and 4 TaskManagers, a stateful streaming program, and killing one JobManager and two TaskManagers during the execution, verifying that recovery happens correctly.*

## Does this pull request potentially affect one of the following parts:

  - Dependencies (does it add or upgrade a dependency): (yes / no)
  - The public API, i.e., is any changes to the `CustomResourceDescriptors`: (yes / no)
  - Core observer or reconciler logic that is regularly executed: (yes / no)

## Documentation

  - Does this pull request introduce a new feature? (yes / no)
  - If yes, how is the feature documented? (not applicable / docs / JavaDocs / not documented)
