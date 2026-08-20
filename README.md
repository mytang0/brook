# Brook

Brook is a Java workflow orchestration engine for microservice calls and
in-process application logic. It can run embedded with `brook-engine`, or as a
Spring Boot integration through `brook-spring-boot-starter`. This manual covers
both approaches and the SPI-based extensions used in production deployments.

## Contents

- [Project layout](#project-layout)
- [Requirements and installation](#requirements-and-installation)
- [Run the Spring demo](#run-the-spring-demo)
- [Define flows](#define-flows)
- [Start and manage flows](#start-and-manage-flows)
- [Configuration and extensions](#configuration-and-extensions)
- [Customize Brook](#customize-brook)
- [Production guidance](#production-guidance)
- [Contributing](#contributing)

## Project layout

| Module | Purpose |
| --- | --- |
| `brook-common` | Shared models, configuration, utilities, and SPI loading infrastructure. |
| `brook-spi` | Public contracts for tasks, metadata, queues, execution storage, locks, callbacks, listeners, and computing engines. |
| `brook-core` | Flow execution, built-in control-flow tasks, local implementations, and flow-definition loading. |
| `brook-engine` | Convenience artifact for embedded applications. |
| `brook-spring-boot` | Spring Boot starter, auto-configuration, Spring task adapters, and optional Spring integrations. |
| `brook-spi-extensions` | Optional JavaScript computing, HTTP task, HTTP metadata, and Redis lock extensions. |
| `brook-demo` | A runnable Spring Boot application and flow-definition examples. |

Brook's built-in task types are `COMPUTING`, `IF`, `SWITCH`, `LOOP`, `WAIT`,
and `SUB_FLOW`. Additional task types are supplied by dependencies or your
application, such as the optional `HTTP` task and Spring task adapters.

## Requirements and installation

Build the repository with JDK 8 or later and Maven:

```bash
mvn clean verify
```

Use the version published to
[Maven Central](https://central.sonatype.com/search?q=g:xyz.mytang0.brook).

```xml
<properties>
    <brook.version>...</brook.version>
</properties>
```

### Embedded application

Use the engine artifact when the application creates and wires Brook without
Spring:

```xml
<dependency>
    <groupId>xyz.mytang0.brook</groupId>
    <artifactId>brook-engine</artifactId>
    <version>${brook.version}</version>
</dependency>
```

Instantiate a `FlowTaskRegistry` and `FlowExecutor`, register the task
implementations needed by the application, then start a `StartFlowReq`. The
engine resolves its configuration and SPI implementations through Brook's
configuration and extension facilities.

### Spring Boot application

For Spring Boot, add the starter. It auto-configures `FlowExecutor`,
`FlowTaskRegistry`, `FlowLogService`, Spring configuration lookup, and the
Spring task adapters.

```xml
<dependency>
    <groupId>xyz.mytang0.brook</groupId>
    <artifactId>brook-spring-boot-starter</artifactId>
    <version>${brook.version}</version>
</dependency>
```

Brook is enabled by default. Set `brook.enabled=false` to disable its Spring
auto-configuration.

## Run the Spring demo

The demo is in `brook-demo/brook-demo-spring`. It packages JSON definitions
from `src/main/resources/META-INF/flows` and exposes management endpoints under
`/flow`.

```bash
mvn -pl brook-demo/brook-demo-spring -am spring-boot:run
```

Start a background flow named `test-simple`:

```bash
curl -X POST http://localhost:8080/flow/instance/start \
  -H 'Content-Type: application/json' \
  -d '{"name":"test-simple","input":{"value":"example"}}'
```

The response is the flow ID. Fetch its latest state:

```bash
curl 'http://localhost:8080/flow/instance/get?flowId=<flow-id>'
```

The demo contains definitions for simple computation, lists and maps,
conditionals, switching, loops, HTTP calls, Spring bean methods, delays,
retries, timeouts, skips, and hanging asynchronous work. HTTP-related examples
target demo endpoints on `localhost:8080`; run the demo before invoking them.

## Define flows

### Discovery and structure

The default file metadata service scans every classpath
`META-INF/flows/**/*.json` resource. The filename, without `.json`, is the
default lookup name, and each definition also declares its own `name`. File
definitions can be saved or updated only in the in-memory cache for the running
process; use a metadata extension when definitions must be durable or mutable.

A flow requires `name` and at least one entry in `taskDefs`. A task requires
`type` and `name`.

```json
{
  "name": "hello-flow",
  "description": "A minimal flow",
  "input": {
    "name": "${input.name}"
  },
  "taskDefs": [
    {
      "type": "COMPUTING",
      "name": "message",
      "input": {
        "source": "Hello, ${flow.input.name}!"
      }
    }
  ],
  "output": "${message.output}"
}
```

`version` identifies a version when the selected metadata service supports
versions. `failureFlowName` identifies a compensating flow. A flow
`controlDef` supports `timeoutMs`, `timeoutPolicy` (`TIME_OUT` or
`ALERT_ONLY`), `enableLog`, `concurrencyLimit`, `executionProtocol`, and
`queueProtocol`.

### Inputs, outputs, and interpolation

Flow and task `input` and `output` values may be an object, array, or string.
Brook recursively replaces `${...}` expressions using JSONPath against the
execution context. Common paths are:

| Expression | Meaning |
| --- | --- |
| `${flow.input}` | The mapped flow input. |
| `${flow.input.customerId}` | A field in the flow input. |
| `${taskName.output}` | Output from a completed task. |
| `${flow.extension}` | The extension data supplied when the flow was started. |

When an entire string is one expression, its underlying value retains its
type. When text surrounds one or more expressions, Brook produces a string.
Use `$${` to render a literal `${`. Missing JSONPath values resolve to `null`
when used alone and to an empty string when combined with other text.

### Task controls

Each task can contain a `controlDef`:

```json
{
  "startDelayMs": 3000,
  "timeoutMs": 10000,
  "timeoutPolicy": "RETRY",
  "retryCount": 3,
  "retryDelayMs": 1000,
  "retryLogic": "EXPONENTIAL_BACKOFF",
  "enableCache": true,
  "concurrencyLimit": 10,
  "skipDef": {
    "engineType": "javascript",
    "skipCondition": "$.flow.input.dryRun == true"
  }
}
```

`startDelayMs`, `timeoutMs`, and `retryDelayMs` are milliseconds and must be
non-negative. `timeoutPolicy` can be `TIME_OUT`, `ALERT_ONLY`, or `RETRY`;
retry logic is `FIXED` or `EXPONENTIAL_BACKOFF`. Use `checkDef.successDef` to
evaluate business success and `checkDef.retryDef` to decide whether a
successful invocation should instead retry. Both conditions use the configured
computing engine.

`hangDef` models an asynchronous operation. The main task starts the work,
then `determineTaskDef` is executed repeatedly until it completes. For example,
the demo's HTTP poller retries while its response body is `"fail"`:

```json
{
  "hangDef": {
    "determineTaskDef": {
      "type": "HTTP",
      "name": "poll-status",
      "input": {
        "uri": "http://localhost:8080/hang/test/success",
        "method": "GET",
        "params": { "id": "${output.body}" }
      },
      "checkDef": {
        "retryDef": { "retryCondition": "$.body == 'fail'" }
      },
      "controlDef": { "retryCount": 100, "retryDelayMs": 5000 }
    },
    "feedbackOutput": true
  }
}
```

Task definitions also support `logDef` (`startFormat`, `terminalFormat`),
`progressDef` (`progress`, `interval`), `linkDef` (`title`, `url`), and
`callback` (`async`, `protocol`, `input`). Use `extension` for task-specific
string options.

### Built-in control-flow examples

`COMPUTING` evaluates `input.source` with the selected `engineType`. The
JavaScript engine accepts expressions such as `$.result`; `test-simple.json`
shows both literal and JavaScript computation.

`IF` selects the first matching branch. Its input contains an `engineType` and
`branches`, each with `condition` and `cases`:

```json
{
  "type": "IF",
  "name": "environment",
  "input": {
    "engineType": "javascript",
    "branches": [{
      "condition": "$.flow.input.envCode == 'dev'",
      "cases": [{
        "type": "COMPUTING",
        "name": "devLogic",
        "input": { "source": "dev" }
      }]
    }]
  }
}
```

`SWITCH` uses `input.case` to select an array from
`input.decisionCases`; `input.defaultCaseKey` is selected when no key matches.
`test-switch.json` includes nested `IF` tasks.

`LOOP` accepts `input.loopOver` and an array in `input.loopBody`. Each loop
body task receives a unique `__LOOP_<index>` suffix. Within the loop body,
references to the original task name resolve to the latest loop iteration, as
shown in `test-loop.json`.

`WAIT` and `SUB_FLOW` are built-in task types for waiting and starting a nested
flow. Inspect their `FlowTask` option catalogs when defining their inputs, and
add a focused test definition alongside the application flow that uses them.

### HTTP and Spring task examples

Add the HTTP extension for `HTTP` tasks:

```xml
<dependency>
    <groupId>xyz.mytang0.brook</groupId>
    <artifactId>brook-task-http</artifactId>
    <version>${brook.version}</version>
</dependency>
```

An HTTP task requires `uri` and `method`; it also accepts `body`, `headers`,
`params`, `variables`, and `charset`. `test-http.json` calls the demo metadata
endpoint and maps its flow name into the query parameters.

Spring applications can call arbitrary beans with the `springBeanMethod` task:

```json
{
  "type": "springBeanMethod",
  "name": "firstCall",
  "input": {
    "beanName": "demoService",
    "methodName": "hello",
    "args": ["${flow.input}"]
  }
}
```

Provide `types` when overload resolution requires explicit Java class names.
The `test-spring.json` example demonstrates both forms.

## Start and manage flows

`FlowExecutor` exposes two start modes:

- `startFlow(StartFlowReq)`: persists the instance and schedules it
  asynchronously; returns its flow ID.
- `requestFlow(StartFlowReq)`: waits for completion and returns the resulting
  `FlowInstance`. The flow-level timeout bounds the wait when configured.

Supply a named definition with `{"name":"test-simple","input":{...}}`, or put
an inline `flowDef` in `StartFlowReq`. `extension` may carry application
metadata that mapping expressions can read.

The demo maps the following executor methods to HTTP endpoints:

| Operation | Endpoint |
| --- | --- |
| Asynchronous start | `POST /flow/instance/start` |
| Synchronous request | `POST /flow/instance/request` |
| Submit an external task result | `POST /flow/instance/execute` |
| Skip a task | `POST /flow/instance/skip` |
| Trigger scheduling/decision | `PUT /flow/instance/decide?flowId=...` |
| Pause / resume | `PUT /flow/instance/pause?flowId=...`, `PUT /flow/instance/resume?flowId=...` |
| Terminate | `PUT /flow/instance/terminate?flowId=...&reason=...` |
| Read an instance | `GET /flow/instance/get?flowId=...` |

The demo also provides metadata management at `/flow/metadata`: `POST` saves,
`PUT` updates, `GET` reads by `flowName` and optional `flowVersion`, and
`DELETE` removes a definition. The file metadata service does not support
deletion.

## Configuration and extensions

Spring Boot reads Brook settings from the application environment. The core
protocol selectors are:

```properties
brook.enabled=true
brook.metadata.protocol=
brook.execution-dao.protocol=
brook.queue.protocol=local
brook.lock.protocol=
```

An empty metadata protocol uses classpath file definitions. The local queue and
local execution DAO are process-local defaults; select compatible SPI
implementations for shared infrastructure.

### Optional integrations

| Capability | Artifact | Relevant settings |
| --- | --- | --- |
| JavaScript computing | `brook-computing-javascript` | Select `engineType: "javascript"` in flow tasks. |
| HTTP task | `brook-task-http` | `brook.task.http.client-config.connection-request-timeout`, `socket-timeout`, `connect-timeout`, `max-conn-total`, and `max-conn-per-route`. |
| HTTP metadata | `brook-metadata-http` or Spring `brook-spring-boot-http-metadata` | Set `brook.metadata.protocol=http` and `brook.metadata.http.server-uri`; optional keys include `name-key`, `version-key`, `wrapped`, cache settings. |
| MySQL metadata, queue, and execution DAO | `brook-spring-boot-mybatis` | Enable each adapter with `brook.metadata.mysql.enabled=true`, `brook.queue.mysql.enabled=true`, and/or `brook.execution-dao.mysql.enabled=true`; choose the corresponding protocol. |
| Redis lock | `brook-lock-redis` or Spring `brook-spring-boot-lock-redis` | Set `brook.lock.protocol=redis` and configure `brook.lock.redis.config`. |

The HTTP client defaults to 6-second connection-request, 10-second connect,
and 30-second socket timeouts. Tune connection limits and timeouts for the
service being called. Do not place credentials in flow JSON; source them from
the deployment environment and map them only where needed.

## Customize Brook

### Spring task methods

Annotate Spring component methods with `@Taskable` to register application
task types:

```java
@Component
public class InvoiceTasks {
    @Taskable(type = "issue-invoice", description = "Create an invoice")
    public String issue(String customerId) {
        return customerId;
    }
}
```

`@Taskable` also declares whether the method is asynchronous or retryable.
Use a flow task with `type: "issue-invoice"` and provide an input compatible
with the method parameters. For fully dynamic bean calls, use
`springBeanMethod` as described above.

### SPI implementations

Implement `FlowTask` for a new task type, including its required and optional
configuration options, and register it through Brook's extension mechanism.
The same SPI approach applies to `MetadataService`, `ExecutionDAO`,
`QueueService`, `LockService`, `TaskCallback`, `FlowListener`, `TaskListener`,
`Engine`, and `EngineActuator`. Select named implementations with their
protocol settings or task/engine type.

Use task listeners for task lifecycle events, flow listeners for flow lifecycle
events, and callbacks when an external system must be notified of task
completion. Implement a computing `Engine` when a flow's conditions or
expressions need a language other than the available engines.

## Production guidance

- **Persist state:** replace local execution and queue implementations with
  durable, shared adapters before running more than one application instance.
- **Coordinate instances:** use a distributed `LockService` such as Redis
  locking so competing workers do not execute the same flow concurrently.
- **Make side effects safe:** task retries, delayed execution, and hang polling
  can repeat work. Use idempotency keys and make downstream operations safe to
  replay.
- **Set bounds:** configure flow/task timeouts, retries, delay, and concurrency
  limits. Validate that retry delay and retry count will not overload a failing
  downstream service.
- **Protect HTTP calls:** constrain target URLs, use TLS and application
  authentication, set connection/time limits, and avoid logging confidential
  headers or bodies.
- **Observe lifecycle:** configure logging and install listeners/callbacks to
  record flow IDs, task status, terminal reasons, and latency in the
  application's telemetry system.
- **Manage definitions:** file-backed definitions are read from the classpath
  and immutable on disk at runtime. Use a versioned metadata service and
  validate definitions before publishing changes.

## Contributing

Contributions, bug reports, documentation improvements, and feature requests
are welcome in [GitHub Issues](https://github.com/mytang0/brook/issues).

1. Fork the repository and create a topic branch.
2. Make focused changes and add or update tests when behavior changes.
3. Validate the complete Maven build:

   ```bash
   mvn clean verify
   ```

4. Open a pull request at
   <https://github.com/mytang0/brook/pulls>.
