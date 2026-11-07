# wfx

`github.com/hobbyGG/workflow/sdk/wfx` is the Go HTTP client for the workflow
service. Its API uses ordinary Go types and wraps generated protobuf requests,
response envelopes, and HTTP bindings.

`NewClient` accepts a service endpoint. `WithHTTPClient` supplies a transport and
timeout; `WithTimeout` overrides the request timeout. The default HTTP client
uses the existing OpenTelemetry transport wrapper and a 30-second timeout.
Close the client when it is no longer needed.

Definitions use `Create`, `Get`, and `List`. Workflow executions use
`CreateWorkflowExecution`, `GetWorkflowExecution`, and `ListWorkflowExecutions`.
`NodeTypes` retrieves schema metadata. Standalone nodes use `ListTools`,
`ExecuteTool`, and `CheckToolExecution`, with a caller-supplied stable
idempotency key.

The server currently stores workflow-execution input without applying it to the
DAG runtime. Use node presets and graph edges for graph data. Standalone input
is passed directly to the selected activity. The SDK does not add login,
authorization, or provider-specific execution behaviour.
