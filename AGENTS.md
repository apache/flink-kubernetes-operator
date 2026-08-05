<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
-->

# Flink Kubernetes Operator AI Agent Instructions

This file provides guidance for AI coding agents working with the Apache Flink
Kubernetes Operator codebase.

## Prerequisites

- Java 17 (the build compiles and targets Java 17; see `maven.compiler.source`/`target` in the root `pom.xml`)
- Maven 3.8.6 or later. There is **no** Maven wrapper in this repository — use the system `mvn`.
- Git
- Docker (to build operator images and run Testcontainers-based integration tests)
- A local Kubernetes cluster (`minikube` or `kind`), `kubectl`, and `helm` to deploy the operator and run end-to-end tests
- Unix-like environment (Linux, macOS, WSL)
- This operator builds against Apache Flink `1.20.x` (`flink.version` in the root `pom.xml`)

## Commands

### Build

- Fast dev build: `mvn clean install -DskipTests -T1C`
- Full build with tests: `mvn clean verify`
- Single module: `mvn clean install -DskipTests -pl flink-kubernetes-operator -am`
- Single module with tests: `mvn clean verify -pl flink-kubernetes-operator -am`
- Build the operator Docker image: `docker build -t flink-kubernetes-operator .`

### Testing

- Single test class: `mvn -pl flink-kubernetes-operator -Dtest=FlinkConfigManagerTest test`
- Single test method: `mvn -pl flink-kubernetes-operator -Dtest=FlinkConfigManagerTest#testConfigUpdate test`
- End-to-end tests are shell scripts under `e2e-tests/` (e.g. `test_application_operations.sh`); they require a running Kubernetes cluster (minikube) with the operator installed via the Helm chart in `helm/`.

### Code Quality

- Format code: `mvn spotless:apply` (google-java-format, **AOSP** style; version pinned in the root `pom.xml`)
- Check formatting: `mvn spotless:check`
- Checkstyle: `mvn checkstyle:check` (config: `tools/maven/checkstyle.xml`, suppressions: `tools/maven/suppressions.xml`)
- Apache license header check (Rat) runs as part of the build.
- Spotless-check and checkstyle are bound to the `validate` phase, so any build (even `-DskipTests`) fails on unformatted or non-compliant code — run `mvn spotless:apply` before building.

### Documentation

- Regenerate the configuration reference docs: `mvn clean package -pl flink-kubernetes-docs -am -Pgenerate-docs`
  (runs `ConfigOptionsDocGenerator`; output lands under `docs/layouts/shortcodes/generated`). Do not hand-edit generated reference files.

## Repository Structure

Every module from the root `pom.xml`, plus the supporting directories.

### Modules

- `flink-kubernetes-operator-api` — Custom Resource (CRD) types: `FlinkDeployment`, `FlinkSessionJob`, `FlinkStateSnapshot`, `FlinkBlueGreenDeployment` and their specs/status. **This is the user-facing public API surface** (`CustomResourceDescriptors`).
- `flink-kubernetes-operator` — The operator itself: JOSDK controllers, observers, reconcilers, the Flink service abstraction, validation, mutation, metrics, and config. The primary module.
- `flink-kubernetes-standalone` — Standalone-mode (non-native) Kubernetes deployment support used by the operator.
- `flink-kubernetes-webhook` — Admission webhook for validating and mutating custom resources.
- `flink-autoscaler` — Generic Flink job autoscaler, decoupled from Kubernetes so it can be reused outside the operator.
- `flink-autoscaler-standalone` — Runs the autoscaler as a standalone process against existing Flink clusters.
- `flink-autoscaler-plugin-jdbc` — JDBC-backed state store / event handler plugin for the autoscaler.
- `flink-kubernetes-docs` — Documentation build module; auto-generates the config and CRD reference.
- `examples/` — Example projects (`flink-sql-runner-example`, `flink-beam-example`, `kubernetes-client-examples`, `autoscaling`).

### Supporting directories

- `helm/` — Helm chart used to install the operator.
- `e2e-tests/` — End-to-end test scripts run against a real cluster.
- `docs/` — User-facing documentation (Hugo site) and generated shortcodes.
- `tools/` — Maven config (`checkstyle.xml`, `suppressions.xml`), license tooling, OLM, and release scripts.
- `.github/` — CI workflows and the PR template.

### Key packages in `flink-kubernetes-operator`

- `controller/` — JOSDK `Reconciler` entry points, one per CRD (`FlinkDeploymentController`, `FlinkSessionJobController`, `FlinkStateSnapshotController`, `FlinkBlueGreenDeploymentController`).
- `observer/` — Read the actual state of Flink jobs and Kubernetes resources (`deployment/`, `sessionjob/`, `snapshot/`).
- `reconciler/` — Drive resources toward their desired spec: deploy, upgrade, suspend, savepoint, rollback (`deployment/`, `sessionjob/`, `snapshot/`, `diff/`).
- `service/` — Abstraction over the Flink REST/native client used by observers and reconcilers.
- `config/` — `KubernetesOperatorConfigOptions` and configuration management.
- `validation/`, `mutator/` — Resource validation and mutation logic.
- `autoscaler/` — Operator-side glue around the `flink-autoscaler` module.
- `metrics/`, `health/`, `listener/`, `artifact/`, `resources/`, `ssl/`, `utils/` — Supporting subsystems.

## Architecture Boundaries

The operator follows the standard Kubernetes operator (control loop) pattern, built on
the [Java Operator SDK](https://javaoperatorsdk.io/) (JOSDK) and the fabric8 Kubernetes client.

1. **Custom Resources** (`flink-kubernetes-operator-api`) define the user-facing contract:
   `FlinkDeployment` (Application/Session clusters), `FlinkSessionJob` (jobs on a session
   cluster), `FlinkStateSnapshot` (savepoints/checkpoints), and `FlinkBlueGreenDeployment`.
2. **Controller** — the JOSDK reconciliation entry point for each CRD. It delegates to an
   observer and a reconciler; it does not contain business logic itself.
3. **Observer** — reads the *actual* state (Flink job status via REST, Kubernetes resource
   status) and records it on the resource `status`.
4. **Reconciler** — compares desired `spec` against observed `status` and takes action
   (deploy, upgrade with the configured upgrade mode, trigger savepoint, suspend, roll back).
5. **FlinkService** — the boundary to a running Flink cluster (REST client / native or
   standalone deployment). Observers and reconcilers go through this abstraction.
6. **Autoscaler** — `flink-autoscaler` is deliberately independent of Kubernetes so it can be
   embedded in the operator *and* run standalone. Operator-specific wiring lives in the
   operator's `autoscaler/` package.
7. **Webhook** (`flink-kubernetes-webhook`) validates and mutates resources at admission time,
   mirroring the in-process validation/mutation logic.

Key separations:

- **API vs implementation:** CRD types live in `flink-kubernetes-operator-api` and are the
  public contract; implementation lives in `flink-kubernetes-operator`. Changing the API
  affects every user's resources.
- **Observe vs reconcile:** Observers only read and record state; reconcilers only act on it.
  Keep these responsibilities separate.
- **Operator vs autoscaler:** Do not introduce Kubernetes dependencies into `flink-autoscaler`;
  keep it reusable for the standalone autoscaler.

## Common Change Patterns

### Adding a new operator configuration option

1. Define a `ConfigOption<T>` in `KubernetesOperatorConfigOptions` (or the relevant config class)
   using the `ConfigOptions.key(...)` builder with type, default, and description.
2. Add the appropriate `@Documentation` annotation so it appears in the generated reference.
3. Regenerate docs (`mvn ... -Pgenerate-docs`) and document user-facing behavior under `docs/`.
4. Verify: unit test for the default and for the behavior the option controls.

### Adding or changing a CRD field

1. Edit the spec/status type in `flink-kubernetes-operator-api`.
2. **Maintain backward compatibility** — existing resources must continue to deserialize and
   reconcile. New fields should be optional with sensible defaults.
3. Add/extend validation in `validation/` (and the webhook) and mutation in `mutator/` as needed.
4. Regenerate the CRD YAML and reference docs; do not hand-edit generated output.
5. Verify: API/serialization tests plus reconciler coverage for the new field.

### Changing observer or reconciler logic

1. Locate the relevant `observer/` or `reconciler/` subpackage (`deployment`, `sessionjob`, `snapshot`).
2. This is "core reconciler logic that is regularly executed" — flag it as such in the PR template
   and be conservative about behavior and performance.
3. Verify: unit tests plus, where behavior is end-to-end, an `e2e-tests/` script.

### Autoscaler changes

1. Generic logic goes in `flink-autoscaler`; keep it Kubernetes-free.
2. Operator wiring goes in the operator's `autoscaler/` package; standalone wiring in
   `flink-autoscaler-standalone`.
3. Verify: unit tests in `flink-autoscaler`; the `test_autoscaler.sh` e2e script for behavior.

### Adding a metric

1. Register through the operator's metrics subsystem (`metrics/`).
2. Document user-facing metrics under `docs/`.

## Coding Standards

- **Format Java files with Spotless immediately after editing:** `mvn spotless:apply`
  (google-java-format, AOSP style). Run `mvn spotless:check` before committing.
- **Checkstyle:** `tools/maven/checkstyle.xml`. Do not suppress rules; fix the code instead.
- **No Scala.** This repository is Java-only.
- **Apache License 2.0 header** required on all new files (enforced by Apache Rat). Use an
  HTML comment for Markdown files.
- **Logging:** Use parameterized SLF4J log statements (`{}` placeholders), never string
  concatenation. Include caught exceptions in warn/error logs.
- **Use `final`** for variables and fields where applicable.
- **Comments:** Do not restate what the code does; explain "the why" where relevant.
- **Reuse existing code.** Search for existing utilities/abstractions before adding new ones.
- Full code style guide: https://flink.apache.org/how-to-contribute/code-style-and-quality-preamble/

## Testing Standards

- Add tests for new behavior, covering success, failure, and edge cases.
- Use **JUnit 5** + **AssertJ** assertions in new test code. Do not use JUnit 4 or Hamcrest.
- Prefer real or fake test implementations over Mockito mocks where practical.
- **Integration tests:** name classes with an `ITCase` suffix.
- **End-to-end tests:** add or extend a script under `e2e-tests/` for cluster-level behavior.
- **Red-green verification:** for bug fixes, confirm the new test fails without the fix and
  passes with it.
- **Test location** mirrors the source structure within each module.
- Testing conventions: https://flink.apache.org/how-to-contribute/code-style-and-quality-common/#7-testing

## Commits and PRs

### Commit message format

- `[FLINK-XXXX][component] Description` where FLINK-XXXX is the JIRA issue number.
- `[hotfix][component] Description` for typo/doc fixes without a JIRA issue.
- Each commit must have a meaningful message including the JIRA ID. If you don't know the
  ticket number, ask.
- Separate cleanup/refactoring from functional changes into distinct commits.
- When AI tools were used: add a `Generated-by: <Tool Name and Version>` trailer per the
  [ASF generative tooling guidance](https://www.apache.org/legal/generative-tooling.html).

### Pull request conventions

- Title format: `[FLINK-XXXX][component] Title of the pull request`.
- A corresponding JIRA issue is required (except hotfixes for typos).
- Fill out the PR template (`.github/PULL_REQUEST_TEMPLATE.md`) completely but concisely:
  purpose, change log, verification, and the impact checkboxes (dependencies, public
  API/`CustomResourceDescriptors`, core observer/reconciler logic).
- Each PR should address exactly one issue.
- Ensure `mvn clean verify` passes before opening a PR.
- Always push to your fork, not directly to `apache/flink-kubernetes-operator`.
- Rebase onto the latest target branch before submitting.

### AI-assisted contributions

- Disclose AI usage in the PR template: check the disclosure box and uncomment the
  `Generated-by` line with the tool name and version.
- Add `Generated-by: <Tool Name and Version>` to commit messages.
- Never add `Co-Authored-By` with an AI agent as co-author; agents are assistants, not authors.
- You must be able to explain the design, code, and tests, debug them, and respond to review
  feedback substantively.
- Reviewer-ready quality bar: the author owns PR quality. Low-effort AI-generated PRs
  (unreviewed prose, scaffolding without behavior, tests that do not exercise the change,
  padded commit messages) will be closed without review.

## Boundaries

### Ask first

- Adding or changing fields on the CRDs (`flink-kubernetes-operator-api`) — these are
  user-facing API commitments with backward-compatibility implications.
- Adding new dependencies.
- Changes to savepoint/checkpoint or state-upgrade behavior (affects job recovery).
- Changes to core observer/reconciler logic on the hot reconciliation path.
- Large cross-module refactors.

### Never

- Commit secrets, credentials, or tokens.
- Push directly to `apache/flink-kubernetes-operator`; always work from your fork.
- Mix unrelated changes into one PR.
- Hand-edit generated CRD YAML or reference docs when a generation workflow exists.
- Suppress or bypass checkstyle (no `CHECKSTYLE:OFF`/`CHECKSTYLE:ON` comments, no
  `tools/maven/suppressions.xml` additions, no `@SuppressWarnings`); fix the code instead.
- Add `Co-Authored-By` with an AI agent as co-author; use `Generated-by:` instead.
- Use destructive git operations unless explicitly requested.

## References

- [README.md](README.md) — Project overview
- [Documentation](https://nightlies.apache.org/flink/flink-kubernetes-operator-docs-main/) — User guides and the development guide (build, CI/CD)
- [.github/PULL_REQUEST_TEMPLATE.md](.github/PULL_REQUEST_TEMPLATE.md) — PR checklist
- [Code Style Guide](https://flink.apache.org/how-to-contribute/code-style-and-quality-preamble/) — Detailed coding guidelines
- [ASF Generative Tooling Guidance](https://www.apache.org/legal/generative-tooling.html) — AI tooling policy
